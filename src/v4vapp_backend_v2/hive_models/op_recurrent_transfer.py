from typing import Any

from pydantic import ConfigDict, Field

from v4vapp_backend_v2.hive_models.op_base import TransferBase


class RecurrentTransfer(TransferBase):
    executions: int = Field(0, description="Number of executions")
    recurrence: int = Field(0, description="Hours between executions")
    extensions: list[Any] = Field(
        [], description="List of extensions associated with the recurrent transfer"
    )

    model_config = ConfigDict(populate_by_name=True)

    def __init__(self, **hive_event: Any) -> None:
        super().__init__(**hive_event)
