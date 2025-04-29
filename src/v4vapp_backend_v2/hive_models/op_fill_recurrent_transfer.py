from typing import Any

from pydantic import ConfigDict, Field

from v4vapp_backend_v2.hive_models.account_name_type import AccNameType
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase

from .amount_pyd import AmountPyd


class FillRecurrentTransfer(TransferBase):
    from_account: AccNameType = Field(alias="from")
    to_account: AccNameType = Field(alias="to")
    remaining_executions: int = Field(0, description="Number of remaining executions")
    amount: AmountPyd = Field(description="Amount being transferred")
    memo: str = Field("", description="Memo associated with the transfer")

    model_config = ConfigDict(populate_by_name=True)

    def __init__(self, **hive_event: Any) -> None:
        super().__init__(**hive_event)
