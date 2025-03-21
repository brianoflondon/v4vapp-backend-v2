from datetime import datetime
from typing import Any, Dict, Optional

from beem import Hive  # type: ignore
from pydantic import BaseModel, ConfigDict, Field

from v4vapp_backend_v2.hive.hive_extras import decode_memo, get_event_id


class Amount(BaseModel):
    amount: str
    nai: str
    precision: int

    @property
    def decimal_amount(self) -> float:
        """Convert string amount to decimal with proper precision"""
        return float(self.amount) / (10**self.precision)


class Transfer(BaseModel):
    id: str = Field(alias="_id")
    amount: Amount
    block_num: int
    from_account: str = Field(alias="from")
    memo: str
    op_in_trx: int = 0
    timestamp: datetime
    to_account: str = Field(alias="to")
    trx_id: str
    trx_num: int
    type: str

    model_config = ConfigDict(
        populate_by_name=True, json_encoders={datetime: lambda v: v.isoformat()}
    )

    def __init__(self, **hive_event: Any) -> None:
        if "id" not in hive_event and "_id" in hive_event:
            trx_id = hive_event.get("trx_id", "")
            op_in_trx = hive_event.get("op_in_trx", 0)
            if op_in_trx == 0:
                hive_event["id"] = str(trx_id)
            else:
                hive_event["id"] = str(f"{trx_id}_{op_in_trx}")

        super().__init__(**hive_event)


class TransferEnhanced(Transfer):
    d_memo: str = ""

    def __init__(self, **hive_event: Any) -> None:
        super().__init__(**hive_event)
        hive_inst: Hive | None = hive_event.get("hive_inst", None)
        self.post_process(hive_inst=hive_inst)


    def post_process(self, hive_inst: Hive | None = None) -> None:
        if self.memo.startswith("#") and hive_inst:
            self.d_memo = decode_memo(memo=self.memo, hive_inst=hive_inst)
