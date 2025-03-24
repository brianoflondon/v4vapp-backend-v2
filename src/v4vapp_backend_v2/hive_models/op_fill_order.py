from datetime import datetime

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
    _id: str
    timestamp: datetime
    block_num: int
    trx_num: int

    @property
    def log_str(self) -> str:
        completed_order = self.check_open_orders()
        current_pays_str = self.current_pays.fixed_width_str(15)
        open_pays_str = self.open_pays.fixed_width_str(15)
        if self.current_pays.symbol == "HIVE":
            rate = self.open_pays.amount_decimal / self.current_pays.amount_decimal
        else:
            rate = self.current_pays.amount_decimal / self.open_pays.amount_decimal
        rate_str = f"{rate:.3f} "  # HIVE/HBD
        icon = "📈"
        link = get_hive_block_explorer_link(self.trx_id, markdown=True)
        return (
            f"{icon}{rate_str:>8} - "
            f"{current_pays_str} --> {open_pays_str} "
            f"{self.open_owner} filled order "
            f"for {self.current_owner} "
            f"{completed_order} {link}"
        )

    def check_open_orders(self) -> str:
        open_order = LimitOrderCreate.open_order_ids.get(self.open_orderid, None)
        if open_order is not None:
            outstanding_amount = (
                open_order.amount_to_sell.amount_decimal - self.open_pays.amount_decimal
            )
            if outstanding_amount > 0:
                open_order.amount_remaining = (
                    open_order.amount_to_sell.beam - self.open_pays.beam
                )
                return f"Remaining {str(open_order.amount_remaining):>9} {open_order.orderid}"
            else:
                LimitOrderCreate.open_order_ids.pop(self.open_orderid)
                return f"Order {open_order.orderid} has been filled."
        return f"id {self.open_orderid}"
