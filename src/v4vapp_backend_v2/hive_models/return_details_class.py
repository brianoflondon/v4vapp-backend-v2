from enum import StrEnum

from pydantic import BaseModel

from v4vapp_backend_v2.process.lock_str_class import CustIDType
from v4vapp_backend_v2.actions.tracked_any import TrackedAny
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd


class ReturnAction(StrEnum):
    IN_PROGRESS = "in_progress"
    REFUND = "refund"
    CHANGE = "change"
    CONVERSION = "conversion"
    CUSTOM_JSON = "custom_json"
    ERROR = "error"
    HOLD = "hold"


class HiveReturnDetails(BaseModel):
    """
    HiveReturnDetails

    A Pydantic model representing the details of a message to be returned to the Hive blockchain.
    This class structures the response message for Hive transactions, including information about
    the tracked operation, original memo, reason for return, amount, recipient customer ID, and
    whether the transaction should be broadcasted.

    Attributes:
        tracked_op (TrackedAny): The operation being tracked for return.
        original_memo (str): The original memo associated with the transaction.
        reason_str (str): A string describing the reason for the return.
        return_reason (ReturnReason): An enumerated reason for the return.
        amount (AmountPyd): The amount involved in the transaction.
        pay_to_cust_id (CustIDType): The customer ID to whom the payment should be made.
        nobroadcast (bool, optional): If True, the transaction will not be broadcasted. Defaults to False.

    Properties:
        log_extra (dict): Returns a dictionary of extra log information, excluding unset and None values,
                          for additional logging context.

    """

    tracked_op: TrackedAny
    original_memo: str
    action: ReturnAction
    reason_str: str = ""
    amount: AmountPyd | None = None
    pay_to_cust_id: CustIDType
    nobroadcast: bool = False

    @property
    def log_extra(self) -> dict:
        """
        Returns a dictionary of extra log information.
        This is used for logging purposes to provide additional context.
        """
        return self.model_dump(
            exclude_none=True,
            exclude_unset=True,
        )
