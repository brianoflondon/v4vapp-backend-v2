from typing import Annotated, Any

from pydantic import BaseModel, Discriminator, Tag, ValidationError

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive_models.op_all import OpAllTransfers
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_fill_recurrent_transfer import FillRecurrentTransfer
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_recurrent_transfer import RecurrentTransfer
from v4vapp_backend_v2.hive_models.op_transfer import Transfer, TransferBase
from v4vapp_backend_v2.models.invoice_models import Invoice
from v4vapp_backend_v2.models.payment_models import Payment

"""
This set of functions discriminates between the various types of tracked object which are
either Hive operations or LND Invoices or Payments.

Tracked operations for Hive need to be changed

"""


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
            "custom_json",
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
        value, (OpAllTransfers, FillOrder, LimitOrderCreate, CustomJson)
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
    | Annotated[Payment, Tag("payment")]
    | Annotated[CustomJson, Tag("custom_json")],
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
    db = InternalConfig.db

    if isinstance(tracked_obj, str):
        short_id = tracked_obj
        if "_" in short_id:
            # This is a for a hive_ops object
            collection_name = "hive_ops"
            query = TrackedBaseModel.short_id_query(short_id=short_id)
            result = await db[collection_name].find_one(filter=query)
            if result:
                value = {"value": result}
                answer = DiscriminatedTracked.model_validate(value)
                return answer.value
        else:
            collections = [Invoice.collection_name, Payment.collection_name]
            for collection_name in collections:
                query = TrackedBaseModel.short_id_query(short_id=short_id)
                result = await db[collection_name].find_one(filter=query)
                if result:
                    value = {"value": result}
                    answer = DiscriminatedTracked.model_validate(value)
                    return answer.value

    elif collection_name := getattr(tracked_obj, "collection_name", None):
        result = await db[collection_name].find_one(
            filter=tracked_obj.group_id_query,
        )
        if result:
            value = {"value": result}
            answer = DiscriminatedTracked.model_validate(value)
            return answer.value
    return None


def tracked_any_filter(tracked: dict[str, Any]) -> TrackedAny:
    """
    Validates and filters a tracked object, ensuring it is of type OpAny, Invoice, or Payment.

    Removes the '_id' field from the input dictionary if present, then attempts to validate
    the object using the DiscriminatedTracked model. If validation is successful, returns
    the validated object as a TrackedAny type. Raises a ValueError if validation fails.

    Args:
        tracked (dict[str, Any]): The tracked object to validate and filter.

    Returns:
        TrackedAny: The validated tracked object of type OpAny, Invoice, or Payment.

    Raises:
        ValueError: If the object cannot be validated as one of the expected types.

    """
    if "_id" in tracked:
        del tracked["_id"]  # Remove _id field if present

    try:
        value = {"value": tracked}
        answer = DiscriminatedTracked.model_validate(value)
        return answer.value
    except ValidationError as e:
        raise ValueError(f"Failed to validate tracked object: {e}") from e
    except ValueError as e:
        logger.warning(
            f"Parsing as OpAny, Invoice, or Payment. {e}",
            extra={"notification": False, "tracked": tracked},
        )
        raise ValueError(
            f"Invalid tracked object type: Expected OpAny, Invoice, or Payment. {e}"
        ) from e


def tracked_transfer_filter(tracked: dict[str, Any]) -> TrackedTransfer:
    """
    Validates and filters a tracked object, ensuring it is of type TrackedTransfer.

    Removes the '_id' field from the input dictionary if present, then attempts to validate
    the object using the TrackedTransfer model. If validation is successful, returns
    the validated object as a TrackedTransfer type. Raises a ValueError if validation fails.

    Args:
        tracked (dict[str, Any]): The tracked object to validate and filter.

    Returns:
        TrackedTransfer: The validated tracked object of type TrackedTransfer.

    Raises:
        ValueError: If the object cannot be validated as a TrackedTransfer.
    """
    tracked_any = tracked_any_filter(tracked)
    if isinstance(tracked_any, (TransferBase, Transfer, RecurrentTransfer, FillRecurrentTransfer)):
        return tracked_any
    raise ValueError(
        f"Invalid tracked object type: Expected TrackedTransfer, got {type(tracked_any)}"
    )
