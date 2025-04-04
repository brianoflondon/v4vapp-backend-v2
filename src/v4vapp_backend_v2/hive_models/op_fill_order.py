from datetime import datetime

from beem.amount import Amount  # type: ignore
from pydantic import Field

from v4vapp_backend_v2.hive.hive_extras import get_hive_block_explorer_link
from v4vapp_backend_v2.hive_models.op_base import OpBase
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate

from .amount_pyd import AmountPyd


class FillOrder(OpBase):
    current_orderid: int
    current_owner: str
    current_pays: AmountPyd
    open_orderid: int
    open_owner: str
    open_pays: AmountPyd
    timestamp: datetime
    completed_order: bool = Field(
        default=False,
        description="True if the order was completed, False if it was partially filled",
    )

    log_internal: str = Field(
        default="",
        exclude=True,
        description="Holds the internal log string for the log and notification log operations",
    )

    def _log_internal(self) -> str:
        if self.log_internal:
            return self.log_internal
        check_str = self.check_open_orders()  # forces self.completed_orders to be set
        current_pays_str = self.current_pays.fixed_width_str(15)
        open_pays_str = self.open_pays.fixed_width_str(15)
        if self.current_pays.symbol == "HIVE":
            rate = self.open_pays.amount_decimal / self.current_pays.amount_decimal
        else:
            rate = self.current_pays.amount_decimal / self.open_pays.amount_decimal
        rate_str = f"{rate:.3f}"  # HIVE/HBD
        icon = "📈"
        self.log_internal = (
            f"{icon}{rate_str:>8} - "
            f"{current_pays_str} --> {open_pays_str} "
            f"{self.open_owner} filled order for "
            f"{self.current_owner} "
            f"{check_str}"
        )
        return self.log_internal

    @property
    def log_str(self) -> str:
        ans = self._log_internal()
        # link = get_hive_block_explorer_link(
        #     trx_id=self.trx_id, markdown=False, any_op=self
        # )
        link = get_hive_block_explorer_link(
            self.trx_id,
            markdown=False,
            block_num=self.block_num,
            op_in_trx=self.op_in_trx,
        )
        return f"{ans} {link}"

    @property
    def notification_str(self) -> str:
        ans = self._log_internal()
        link = get_hive_block_explorer_link(
            self.trx_id,
            markdown=True,
            block_num=self.block_num,
            op_in_trx=self.op_in_trx,
        )
        return f"{ans} {link}"

    def check_open_orders(self) -> str:
        open_order = LimitOrderCreate.open_order_ids.get(self.current_orderid, None)
        if not open_order:
            open_order = LimitOrderCreate.open_order_ids.get(self.open_orderid, None)
        if open_order is not None:
            print(f"amount_remaining: {open_order.amount_remaining}")
            if (
                self.open_owner == open_order.owner
            ):  # This is when we fill someone else's order
                amount_remaining = Amount(str(open_order.amount_remaining))
                amount_remaining -= Amount(self.current_pays.model_dump())
            else:
                amount_remaining = Amount(str(open_order.amount_remaining))
                amount_remaining -= Amount(self.open_pays.model_dump())
            open_order.amount_remaining = amount_remaining
            if amount_remaining > 0:
                self.completed_order = False
                return f"Remaining {open_order.amount_remaining} {open_order.orderid}"
            else:
                LimitOrderCreate.open_order_ids.pop(open_order.orderid)
                self.completed_order = True
                return (
                    f"✅ Order {open_order.orderid} has been filled "
                    f"(xs {amount_remaining})"
                )
        return f"id {self.open_orderid}"
