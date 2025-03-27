from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, List

from beem.amount import Amount  # type: ignore
from pydantic import BaseModel, ConfigDict, Field

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.general_purpose_funcs import snake_case
from v4vapp_backend_v2.hive.hive_extras import get_hive_block_explorer_link

from .amount_pyd import AmountPyd
from .op_base import OpBase


class LimitOrderCreate(OpBase):
    amount_to_sell: AmountPyd
    block_num: int
    expiration: datetime
    fill_or_kill: bool
    min_to_receive: AmountPyd
    orderid: int
    owner: str
    timestamp: datetime
    trx_num: int

    # Used to store the amount remaining to be filled when doing math
    amount_remaining: Amount = Field(0.0, alias="amount_remaining")

    # Class variable shared by all instances
    open_order_ids: ClassVar[Dict[int, "LimitOrderCreate"]] = {}

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.amount_remaining = self.min_to_receive.amount_decimal
        if self.expiration.tzinfo is None:
            self.expiration = self.expiration.replace(tzinfo=timezone.utc)
        # Add the instance to the class variable
        LimitOrderCreate.open_order_ids[self.orderid] = self.model_copy()
        icon = "📈"
        logger.info(f"{icon} Open orders: {len(LimitOrderCreate.open_order_ids)}")

    @classmethod
    def op_name(cls) -> str:
        return snake_case(cls.__name__)

    @property
    def log_extra(self) -> Dict[str, Any]:
        return {self.op_name(): self.model_dump()}

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
        for orderid, order in self.open_order_ids.items():
            if order.expiration < datetime.now(tz=timezone.utc):
                expired_orders.append(orderid)
        for orderid in expired_orders:
            self.open_order_ids.pop(orderid)

    # TODO: #40 Add logic for checking off filled orders

    @property
    def rate(self) -> float:
        if self.amount_to_sell.symbol == "HIVE":
            return (
                self.min_to_receive.amount_decimal / self.amount_to_sell.amount_decimal
            )
        return self.amount_to_sell.amount_decimal / self.min_to_receive.amount_decimal

    def _log_internal(self) -> str:
        sell = self.amount_to_sell.fixed_width_str(15)
        receive = self.min_to_receive.fixed_width_str(15)
        rate_str = f"{self.rate:.3f}"  # HIVE/HBD
        icon = "📈"
        return (
            f"{icon}{rate_str:>8} - "
            f"{sell} for {receive} "
            f"{self.owner} created order "
            f"{self.orderid}"
        )

    @property
    def log_str(self) -> str:
        ans = self._log_internal()
        link = get_hive_block_explorer_link(self.trx_id, markdown=False)
        return f"{ans} {link}"

    @property
    def notification_str(self) -> str:
        ans = self._log_internal()
        link = get_hive_block_explorer_link(self.trx_id, markdown=True)
        return f"{ans} {link}"
