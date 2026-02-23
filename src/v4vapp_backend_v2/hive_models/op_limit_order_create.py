import json
from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, List

from pydantic import ConfigDict, Field
from redis import Redis

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.process.lock_str_class import CustIDType

from .amount_pyd import AmountPyd
from .op_base import OpBase, OpRealm


class LimitOrderCreate(OpBase):
    """Model for Hive limit_order_create operations.

    Historically instances kept a class-level dictionary of open orders
    (`open_order_ids`).  That approach didn't survive across process restarts
    and made running monitors in multiple workers problematic.  We now persist
    the same information in Redis (hash key ``limit_order_create:open_orders``)
    and provide helpers that automatically fall back to an in-memory dict when
    Redis is unavailable (e.g. in lightweight unit tests).

    The instance property ``open_order_ids`` and a few class methods are kept
    for compatibility with existing code/tests, but callers are encouraged to
    use the explicit helpers such as :meth:`get_open_order`.
    """

    amount_to_sell: AmountPyd
    expiration: datetime
    fill_or_kill: bool = Field(
        default=False,
        description="True if the order is fill or kill, False if it is good till canceled",
    )
    min_to_receive: AmountPyd
    orderid: int
    owner: str
    cust_id: CustIDType = Field("", description="Customer ID determined from to/from fields")

    # Used to store the amount remaining to be filled when doing math
    amount_remaining: AmountPyd | None = Field(None, alias="amount_remaining")

    # Redis key used to store open orders as a hash
    REDIS_KEY: ClassVar[str] = "limit_order_create:open_orders"

    # note: we no longer keep a meaningful in‑memory class variable for orders;
    # the following property is provided for backwards compatibility with tests
    # and code that accessed ``instance.open_order_ids``.

    @classmethod
    def _redis(cls) -> Redis:
        # use decoded client so we can store JSON strings directly
        return InternalConfig.redis_decoded

    # fall back to in-memory store when Redis is unreachable (e.g., during
    # unit tests or startup before Redis container is running)
    _in_memory: ClassVar[Dict[int, "LimitOrderCreate"]] = {}

    @classmethod
    def _redis_available(cls) -> bool:
        try:
            cls._redis().ping()
            return True
        except Exception:
            return False

    # flag used internally to suppress redis writes when we're simply
    # re-constructing objects from storage.  This avoids recursion in
    # ``get_all_open_orders`` and ``get_open_order``.
    _suppress_redis: ClassVar[bool] = False

    @classmethod
    def add_open_order(cls, order: "LimitOrderCreate") -> None:
        """Store an order in Redis (or memory) and enforce size limit."""
        if cls._suppress_redis:
            return
        if cls._redis_available():
            # use pydantic json output so datetime/etc are serializable
            cls._redis().hset(cls.REDIS_KEY, str(order.orderid), order.model_dump_json())
        else:
            cls._in_memory[order.orderid] = order
        cls._maintain_order_limit()

    @classmethod
    def _model_from_data(cls, data: dict) -> "LimitOrderCreate":
        """Helper that builds an instance without triggering storage."""
        cls._suppress_redis = True
        try:
            return cls.model_validate(data)
        finally:
            cls._suppress_redis = False

    @classmethod
    def get_open_order(cls, orderid: int) -> "LimitOrderCreate | None":
        if cls._redis_available():
            raw = cls._redis().hget(cls.REDIS_KEY, str(orderid))
            if not raw:
                return None
            return cls._model_from_data(json.loads(raw))
        else:
            return cls._in_memory.get(orderid)

    @classmethod
    def get_all_open_orders(cls) -> Dict[int, "LimitOrderCreate"]:
        if cls._redis_available():
            raw = cls._redis().hgetall(cls.REDIS_KEY)
            return {int(k): cls._model_from_data(json.loads(v)) for k, v in raw.items()}
        else:
            return dict(cls._in_memory)

    @classmethod
    def remove_open_order(cls, orderid: int) -> None:
        if cls._redis_available():
            cls._redis().hdel(cls.REDIS_KEY, str(orderid))
        else:
            cls._in_memory.pop(orderid, None)

    @classmethod
    def clear_open_orders(cls) -> None:
        if cls._redis_available():
            cls._redis().delete(cls.REDIS_KEY)
        cls._in_memory.clear()

    @property
    def open_order_ids(self) -> Dict[int, "LimitOrderCreate"]:
        """Instance-level accessor retaining old semantics."""
        return self.__class__.get_all_open_orders()

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, **hive_event: Any):
        super().__init__(**hive_event)
        self.realm = OpRealm.REAL
        self.amount_remaining = self.min_to_receive
        if self.expiration.tzinfo is None:
            self.expiration = self.expiration.replace(tzinfo=timezone.utc)
        # store the order in Redis if it's a watched owner
        # (skip entirely if redis writes are suppressed; this happens when
        # we're reconstructing objects from storage)
        if not self.__class__._suppress_redis:
            if self.watch_users and self.owner in self.watch_users:
                if not self.__class__.get_open_order(self.orderid):
                    self.__class__.add_open_order(self.model_copy())
                    icon = "📈"
                    orders = self.__class__.get_all_open_orders()
                    logger.info(
                        f"{icon} Open orders: {len(orders)}",
                        extra={"open_order_ids": orders},
                    )
        self.cust_id = self.owner

    @property
    def is_watched(self) -> bool:
        """
        Check if the order is watched.

        Returns:
            bool: True if the order is watched, False otherwise.
        """
        if LimitOrderCreate.watch_users and (self.owner in LimitOrderCreate.watch_users):
            return True
        return False

    @classmethod
    def expire_orders(cls) -> None:
        """
        Expires orders that have passed their expiration date.

        This method iterates through the open orders and checks if the expiration
        date has passed. If the expiration date has passed, it removes the order
        from the open orders dictionary.

        Returns:
            None
        """
        expired_orders: List[int] = []
        for orderid, order in cls.get_all_open_orders().items():
            if cls.watch_users and order.owner not in cls.watch_users:
                expired_orders.append(orderid)
                continue
            if order.expiration < datetime.now(tz=timezone.utc):
                expired_orders.append(orderid)
        for orderid in expired_orders:
            cls.remove_open_order(orderid)
        cls._maintain_order_limit()

    @classmethod
    def _maintain_order_limit(cls) -> None:
        """
        Ensures the open order store only holds the latest 50 items.
        Removes the oldest entries stored in Redis if the limit is exceeded.
        """
        orders = cls.get_all_open_orders()
        if len(orders) > 50:
            # Sort by timestamp and remove the oldest entries
            sorted_orders = sorted(orders.items(), key=lambda item: item[1].timestamp)
            for orderid, _ in sorted_orders[: len(orders) - 50]:
                cls.remove_open_order(orderid)

    # TODO: #40 Add logic for checking off filled orders

    @property
    def rate(self) -> float:
        # return a float to satisfy callers/type-checker; amounts are Decimal
        if self.amount_to_sell.symbol == "HIVE":
            return float(self.min_to_receive.amount_decimal / self.amount_to_sell.amount_decimal)
        return float(self.amount_to_sell.amount_decimal / self.min_to_receive.amount_decimal)

    def _log_internal(self) -> str:
        sell = self.amount_to_sell.fixed_width_str(15)
        receive = self.min_to_receive.fixed_width_str(15)
        rate_str = f"{self.rate:.3f}"  # HIVE/HBD
        icon = "📈"
        return (
            f"{icon} {sell} for {receive} {self.owner} created order {self.orderid} {rate_str:>8}"
        )

    @property
    def log_str(self) -> str:
        ans = self._log_internal()
        return f"{ans} {self.link} {self.short_id}"

    @property
    def notification_str(self) -> str:
        ans = self._log_internal()
        return f"{ans} {self.markdown_link}"

    @property
    def ledger_str(self) -> str:
        """
        Returns a string representation of the ledger entry for the transaction.
        This string is formatted to include the transaction details, including
        the current and open pays amounts, the order IDs, and the rate.
        """
        # return _log_internal but strip the icon from the start
        return_str = self._log_internal()
        return_str = return_str.replace("📈", "")
        return return_str.strip()

    async def update_conv(self, quote: QuoteResponse | None = None) -> None:
        """
        Updates the conversion for the transaction.

        If the subclass has a `conv` object, update it with the latest quote.
        If a quote is provided, it sets the conversion to the provided quote.
        If no quote is provided, it uses the last quote to set the conversion.

        Args:
            quote (QuoteResponse | None): The quote to update.
                If None, uses the last quote.
        """
        if not quote:
            quote = await TrackedBaseModel.nearest_quote(self.timestamp)
        conv = CryptoConversion(amount=self.min_to_receive, quote=quote).conversion
        self.conv = conv
