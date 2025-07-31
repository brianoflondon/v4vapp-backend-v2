from pydantic import BaseModel

from v4vapp_backend_v2.actions.tracked_any import TrackedAny
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd


class HiveReturnMessage(BaseModel):
    """
    Represents a message to be returned to the Hive blockchain.
    This model is used to structure the response message for Hive transactions.
    """

    tracked_op: TrackedAny
    original_memo: str
    reason: str
    amount: AmountPyd
    nobroadcast: bool = False

    """
    The message to be returned to the Hive blockchain.
    This should be a clean and formatted string suitable for Hive transactions.
    """
