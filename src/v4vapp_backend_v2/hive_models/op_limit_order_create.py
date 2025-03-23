from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, List

from beem.amount import Amount  # type: ignore
from pydantic import BaseModel, ConfigDict, Field

from v4vapp_backend_v2.helpers.general_purpose_funcs import snake_case

from .amount_pyd import AmountPyd
from .op_base import OpBase


class LimitOrderCreate(OpBase):
    _id: str
    amount_to_sell: AmountPyd
    block_num: int
    expiration: datetime
    fill_or_kill: bool
    min_to_receive: AmountPyd
    op_in_trx: int
    orderid: int
    owner: str
    timestamp: datetime
    trx_id: str
    trx_num: int
    type: str

    # Used to store the amount remaining to be filled when doing math
    amount_remaining: Amount | None = Field(None, alias="amount_remaining")

    # Class variable shared by all instances
    open_orderids: ClassVar[Dict[int, "LimitOrderCreate"]] = {}

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, **data: Any):
        super().__init__(**data)
        if self.expiration.tzinfo is None:
            self.expiration = self.expiration.replace(tzinfo=timezone.utc)
        # Add the instance to the class variable
        LimitOrderCreate.open_orderids[self.orderid] = self.model_copy()

    @classmethod
    def name(cls) -> str:
        return snake_case(cls.__name__)

    @property
    def log_extra(self) -> Dict[str, Any]:
        return {self.name(): self.model_dump()}

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
        for orderid, order in self.open_orderids.items():
            if order.expiration < datetime.now(tz=timezone.utc):
                expired_orders.append(orderid)
        for orderid in expired_orders:
            self.open_orderids.pop(orderid)

    @property
    def rate(self) -> float:
        if self.amount_to_sell.symbol == "HIVE":
            return (
                self.min_to_receive.amount_decimal / self.amount_to_sell.amount_decimal
            )
        return self.amount_to_sell.amount_decimal / self.min_to_receive.amount_decimal

    @property
    def log_str(self) -> str:
        sell = self.amount_to_sell.fixed_width_str(15)
        receive = self.min_to_receive.fixed_width_str(15)
        rate_str = f"{self.rate:.3f}"  # HIVE/HBD
        icon = "📈"
        return (
            f"{icon}{rate_str:>8}  - "
            f"{sell} for {receive} "
            f"{self.owner} created order "
            f"{self.orderid}"
        )
