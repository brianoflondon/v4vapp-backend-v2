from typing import Any, override

from pydantic import ConfigDict, Field

from v4vapp_backend_v2.hive_models.op_transfer import TransferBase


class RecurrentTransfer(TransferBase):
    executions: int = Field(0, description="Number of executions")
    recurrence: int = Field(0, description="Hours between executions")
    extensions: list[Any] = Field(
        default=[], description="List of extensions associated with the recurrent transfer"
    )

    model_config = ConfigDict(populate_by_name=True)

    def __init__(self, **hive_event: Any) -> None:
        super().__init__(**hive_event)


    @property
    def recurrence_str(self) -> str:
        """
        Returns a string representation of the recurrence details.
        """
        return f" Execution: {self.executions} every {self.recurrence} hours"

"""
This change introduces a new element, `pair_id`, which allows distinguishing between
different transfers between the same pair of users. It also extends the
`recurrent_transfer_operation` with `recurrent_transfer_pair_id`.

- When `pair_id` is absent, its value defaults to `0`.
- When `pair_id` is present, it can be set to a different value, enabling multiple transfers
    between the same pair of users at the same time.

For example:
- Previously (<HF28), only one transfer was possible between Alice and Bob: `alice to bob`.
- Now (>=HF28), transfers like `alice to bob, 0`, `alice to bob, 1`, `alice to bob, 1234`, etc.,
    are possible.
"""
