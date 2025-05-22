from typing import Annotated, Any

from pydantic import BaseModel, Discriminator, Tag

from v4vapp_backend_v2.hive_models.op_all import OpAllTransfers
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_fill_recurrent_transfer import FillRecurrentTransfer
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_recurrent_transfer import RecurrentTransfer
from v4vapp_backend_v2.hive_models.op_transfer import Transfer
from v4vapp_backend_v2.models.invoice_models import Invoice
from v4vapp_backend_v2.models.payment_models import Payment


def get_tracked_any_type(value: Any) -> str:
    """
    Extract the operation type from the input dictionary for discriminated union.

    Args:
        value: The input dictionary (hive_event)

    Returns:
        str: The operation type or 'base' for unknown types
    """
    # Check for a Ledger Tracked Hive Event
    if isinstance(value, dict):
        op_type = value.get("type", None)
        if op_type and op_type in [
            "transfer",
            "fill_order",
            "limit_order_create",
            "recurrent_transfer",
            "fill_recurrent_transfer",
        ]:
            return op_type
        add_index = value.get("add_index")
        if add_index and add_index != 0:
            return "invoice"
        payment_index = value.get("payment_index")
        if payment_index and payment_index != 0:
            return "payment"

    # Check for a Lightning Tracked Hive Event
    if isinstance(value, Invoice):
        return "invoice"
    if isinstance(value, Payment):
        return "payment"
    if isinstance(value, (OpAllTransfers, FillOrder, LimitOrderCreate)):
        if type := getattr(value, "type", None):
            return type

    raise ValueError(f"Invalid operation type {value}")


TrackedAny = Annotated[
    Annotated[Transfer, Tag("transfer")]
    | Annotated[RecurrentTransfer, Tag("recurrent_transfer")]
    | Annotated[FillRecurrentTransfer, Tag("fill_recurrent_transfer")]
    | Annotated[FillOrder, Tag("fill_order")]
    | Annotated[LimitOrderCreate, Tag("limit_order_create")]
    | Annotated[Invoice, Tag("invoice")]
    | Annotated[Payment, Tag("payment")],
    Discriminator(get_tracked_any_type),
]


class DiscriminatedTracked(BaseModel):
    value: TrackedAny
