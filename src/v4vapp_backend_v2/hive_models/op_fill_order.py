from datetime import datetime

from pydantic import BaseModel

from v4vapp_backend_v2.hive_models.op_base import OpBase

from .amount_pyd import AmountPyd


class FillOrder(OpBase):
    type: str
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
    trx_id: str
    op_in_trx: int

    @property
    def log_str(self) -> str:
        current_pays_str = self.current_pays.fixed_width_str(15)
        open_pays_str = self.open_pays.fixed_width_str(15)
        if self.current_pays.symbol == "HIVE":
            rate = self.open_pays.amount_decimal / self.current_pays.amount_decimal
        else:
            rate = self.current_pays.amount_decimal / self.open_pays.amount_decimal
        rate_str = f"{rate:.3f} "  # HIVE/HBD
        return (
            f"💵{rate_str:>8}  - "
            f"{current_pays_str:>18} -> {open_pays_str:>18} "
            f"{self.open_owner} filled order "
            f"for {self.current_owner} with "
            f"{self.open_orderid}"
        )


# Example usage:
# fill_order = FillOrder(**your_data)
