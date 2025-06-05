from typing import Annotated, Any

from pydantic import BaseModel, Discriminator, Tag

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.hive_models.op_all import OpAllTransfers
from v4vapp_backend_v2.hive_models.op_base import OpBase
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
        if value.get("type", value.get("op_type", None)) == "block_marker":
            raise ValueError(f"Invalid operation type {value}")
        op_type = value.get("type", None) or value.get("op_type", None)
        if op_type and op_type in [
            "transfer",
            "fill_order",
            "limit_order_create",
            "recurrent_transfer",
            "fill_recurrent_transfer",
        ]:
            return op_type
        add_index = value.get("add_index", None)
        if add_index and add_index != 0:
            return "invoice"
        r_hash = value.get("r_hash")
        if r_hash and not value.get("status") == "settled":
            # This is an unpaid Lightning invoice
            raise ValueError(f"Unpaid Lightning invoice detected: {r_hash}")

        payment_index = value.get("payment_index")
        if payment_index and payment_index != 0:
            return "payment"

    # Check for a Lightning Tracked Hive Event
    if isinstance(value, Invoice):
        return value.op_type or "invoice"
    if isinstance(value, Payment):
        return value.op_type or "payment"
    if not isinstance(value, dict) and isinstance(
        value, (OpAllTransfers, FillOrder, LimitOrderCreate)
    ):
        if hasattr(value, "op_type"):
            return value.op_type

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

TrackedTransfer = Annotated[
    Annotated[Transfer, Tag("transfer")]
    | Annotated[RecurrentTransfer, Tag("recurrent_transfer")]
    | Annotated[FillRecurrentTransfer, Tag("fill_recurrent_transfer")],
    Discriminator(get_tracked_any_type),
]


class DiscriminatedTracked(BaseModel):
    value: TrackedAny


async def load_tracked_object(tracked_obj: TrackedAny | str) -> TrackedAny | None:
    """
    Asynchronously loads a tracked object from the database using either a TrackedAny instance or a short ID string.

    If a string is provided, the function determines the appropriate collection to query based on the format of the string.
    If a TrackedAny instance is provided, it uses its collection and group_id_query attributes to perform the lookup.

        tracked_obj (TrackedAny | str): The tracked object instance or its short ID.

        TrackedAny | None: The loaded tracked object if found, otherwise None.

    """
    if not TrackedBaseModel.db_client:
        return None

    if isinstance(tracked_obj, str):
        short_id = tracked_obj
        if "_" in short_id:
            # This is a for a hive_ops object
            async with TrackedBaseModel.db_client as client:
                collection_name = OpBase.collection
                query = TrackedBaseModel.short_id_query(short_id=short_id)
                result = await client.find_one(collection_name=collection_name, query=query)
                if result:
                    value = {"value": result}
                    answer = DiscriminatedTracked.model_validate(value)
                    return answer.value
        else:
            collections = [Invoice.collection, Payment.collection]
            async with TrackedBaseModel.db_client as client:
                for collection_name in collections:
                    query = TrackedBaseModel.short_id_query(short_id=short_id)
                    result = await client.find_one(collection_name=collection_name, query=query)
                    if result:
                        value = {"value": result}
                        answer = DiscriminatedTracked.model_validate(value)
                        return answer.value

    async with TrackedBaseModel.db_client as client:
        collection_name = tracked_obj.collection
        result = await client.find_one(
            collection_name=collection_name,
            query=tracked_obj.group_id_query,
        )
        if result:
            value = {"value": result}
            answer = DiscriminatedTracked.model_validate(value)
            return answer.value
    return None
