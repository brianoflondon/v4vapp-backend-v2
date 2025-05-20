from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, List

from pydantic import ConfigDict, Field

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes

from .amount_pyd import AmountPyd
from .op_base import OpBase, OpRealm


class LimitOrderCreate(OpBase):
    amount_to_sell: AmountPyd
    expiration: datetime
    fill_or_kill: bool = Field(
        default=False,
        description="True if the order is fill or kill, False if it is good till canceled",
    )
    min_to_receive: AmountPyd
    orderid: int
    owner: str

    conv: CryptoConv = CryptoConv()

    # Used to store the amount remaining to be filled when doing math
    amount_remaining: AmountPyd | None = Field(None, alias="amount_remaining")

    # Class variable shared by all instances
    open_order_ids: ClassVar[Dict[int, "LimitOrderCreate"]] = {}

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, **hive_event: Any):
        super().__init__(**hive_event)
        self.realm = OpRealm.REAL
        self.amount_remaining = self.min_to_receive
        if self.expiration.tzinfo is None:
            self.expiration = self.expiration.replace(tzinfo=timezone.utc)
        # Add the instance to the class variable
        if self.watch_users and self.owner in self.watch_users:
            if self.orderid not in LimitOrderCreate.open_order_ids:
                LimitOrderCreate.open_order_ids[self.orderid] = self.model_copy()
                icon = "📈"
                logger.info(
                    f"{icon} Open orders: {len(LimitOrderCreate.open_order_ids)}",
                    extra={"open_order_ids": LimitOrderCreate.open_order_ids},
                )
        if hive_event.get("update_conv", True):
            if self.last_quote.get_age() > 600.0:
                self.update_quote_sync(AllQuotes().get_binance_quote())
            self.update_conv()

    @property
    def log_extra(self) -> Dict[str, Any]:
        return {self.name(): self.model_dump()}

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
    def expire_orders(self) -> None:
        """
        Expires orders that have passed their expiration date.

        This method iterates through the open orders and checks if the expiration
        date has passed. If the expiration date has passed, it removes the order
        from the open orders dictionary.

        Returns:
            None
        """
        expired_orders: List[int] = []
        for orderid, order in LimitOrderCreate.open_order_ids.items():
            if self.watch_users and order.owner not in self.watch_users:
                expired_orders.append(orderid)
                continue
            if order.expiration < datetime.now(tz=timezone.utc):
                expired_orders.append(orderid)
        for orderid in expired_orders:
            LimitOrderCreate.open_order_ids.pop(orderid)
        self._maintain_order_limit()

    @classmethod
    def _maintain_order_limit(cls) -> None:
        """
        Ensures the open_order_ids dictionary only holds the latest 50 items.
        Removes the oldest items if the limit is exceeded.
        """
        if len(cls.open_order_ids) > 50:
            # Sort by timestamp and remove the oldest entries
            sorted_orders = sorted(cls.open_order_ids.items(), key=lambda item: item[1].timestamp)
            for orderid, _ in sorted_orders[: len(cls.open_order_ids) - 50]:
                cls.open_order_ids.pop(orderid)

    # TODO: #40 Add logic for checking off filled orders

    @property
    def rate(self) -> float:
        if self.amount_to_sell.symbol == "HIVE":
            return self.min_to_receive.amount_decimal / self.amount_to_sell.amount_decimal
        return self.amount_to_sell.amount_decimal / self.min_to_receive.amount_decimal

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
        return f"{ans} {self.link}"

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
