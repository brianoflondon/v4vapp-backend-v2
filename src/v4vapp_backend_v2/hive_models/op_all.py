from typing import Annotated, Any

from pydantic import BaseModel, Discriminator, Tag, ValidationError

from v4vapp_backend_v2.hive_models.op_account_update2 import AccountUpdate2
from v4vapp_backend_v2.hive_models.op_account_witness_vote import AccountWitnessVote
from v4vapp_backend_v2.hive_models.op_base import OP_TRACKED, OpBase
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_fill_recurrent_transfer import FillRecurrentTransfer
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_producer_reward import ProducerReward
from v4vapp_backend_v2.hive_models.op_recurrent_transfer import RecurrentTransfer
from v4vapp_backend_v2.hive_models.op_transfer import Transfer
from v4vapp_backend_v2.hive_models.op_update_proposal_votes import UpdateProposalVotes

# Mapping of operation types to their corresponding models
OP_MAP: dict[str, Any] = {
    "custom_json": CustomJson,
    "transfer": Transfer,
    "account_witness_vote": AccountWitnessVote,
    "producer_reward": ProducerReward,
    "fill_order": FillOrder,
    "limit_order_create": LimitOrderCreate,
    "update_proposal_votes": UpdateProposalVotes,
    "account_update2": AccountUpdate2,
    "fill_recurrent_transfer": FillRecurrentTransfer,
    "recurrent_transfer": RecurrentTransfer,
}


# Define the discriminator function
def get_op_type(value: Any) -> str:
    """
    Extract the operation type from the input dictionary for discriminated union.

    Args:
        value: The input dictionary (hive_event)

    Returns:
        str: The operation type or 'base' for unknown types
    """
    if isinstance(value, dict):
        op_type = value.get("type")
        if op_type in OP_MAP:
            return op_type
        else:
            return "op_base"
    if isinstance(value, OpBase):
        if hasattr(value, "type"):
            if value.op_type in OP_MAP:
                return value.op_type
            else:
                return "op_base"
            return value.type
    raise ValueError("Invalid operation type")
    return "op_base"  # Fallback to OpBase for unknown types


# Define the discriminated union type using Annotated and Tag for each class
# Define the discriminated union type using Annotated and Tag for each class
OpAny = Annotated[
    Annotated[CustomJson, Tag("custom_json")]
    | Annotated[Transfer, Tag("transfer")]
    | Annotated[AccountWitnessVote, Tag("account_witness_vote")]
    | Annotated[ProducerReward, Tag("producer_reward")]
    | Annotated[FillOrder, Tag("fill_order")]
    | Annotated[LimitOrderCreate, Tag("limit_order_create")]
    | Annotated[UpdateProposalVotes, Tag("update_proposal_votes")]
    | Annotated[AccountUpdate2, Tag("account_update2")]
    | Annotated[FillRecurrentTransfer, Tag("fill_recurrent_transfer")]
    | Annotated[RecurrentTransfer, Tag("recurrent_transfer")]
    | Annotated[OpBase, Tag("op_base")],  # Default case for any other type
    Discriminator(get_op_type),
]


class DiscriminatedOp(BaseModel):
    value: OpAny


# # Define other type aliases using the discriminated union
# OpMarket = Annotated[FillOrder | LimitOrderCreate, Discriminator(get_op_type)]
# OpVirtual = Annotated[ProducerReward | FillOrder, Discriminator(get_op_type)]
# OpReal = Annotated[
#     Transfer | AccountWitnessVote | CustomJson | AccountUpdate2, Discriminator(get_op_type)
# ]
OpAllTransfers = Transfer | RecurrentTransfer | FillRecurrentTransfer

OpAllRecurrent = RecurrentTransfer | FillRecurrentTransfer
# OpRealOpsLoop = OpAny  # Since OpRealOpsLoop was a union of OpAny and OpMarket, it can reuse OpAny


# Check lists at startup
def op_tracked(op_type: str) -> bool:
    return op_type in OP_MAP


def check_op_tracked():
    op_map_keys = set(OP_MAP.keys())
    op_tracked_set = set(OP_TRACKED)
    missing_in_op_tracked = op_map_keys - op_tracked_set
    missing_in_op_map = op_tracked_set - op_map_keys
    if missing_in_op_tracked:
        raise ValueError(f"********* COMPILE ERROR Missing in OP_TRACKED: {missing_in_op_tracked}")
    if missing_in_op_map:
        raise ValueError(f"********* COMPILE ERROR Missing in OP_MAP: {missing_in_op_map}")


check_op_tracked()


def op_any(hive_event: dict[str, Any]) -> OpAny:
    """
    Processes a hive event dictionary and returns an OpAny object after validation.
    Args:
        hive_event (dict[str, Any]): The event data to process.
    Returns:
        OpAny: The processed operation object.
    Raises:
        ValueError: If the operation type is unknown or validation fails.
    """

    try:
        type = get_op_type(hive_event)
        if type == "op_base":
            raise ValueError("Unknown operation type")
        return op_any_or_base(hive_event)
    except ValidationError as e:
        raise ValueError(f"Failed to validate operation: {e}") from e


def op_any_or_base(hive_event: dict[str, Any]) -> OpAny:
    """
    Factory function to create the appropriate OpBase subclass instance based on the
    provided Hive event data using discriminated union.

    Args:
        hive_event: A dictionary containing Hive event data

    Returns:
        An instance of the appropriate OpBase subclass or OpBase for unknown types.

    Raises:
        ValueError: If the operation type is invalid or cannot be validated.
    """
    if "_id" in hive_event:
        del hive_event["_id"]  # Remove _id field if present
    try:
        value = {"value": hive_event}
        answer = DiscriminatedOp.model_validate(value)
        return answer.value
    except ValidationError as e:
        raise ValueError(f"Failed to validate operation: {e}") from e


def op_query(types: list[str]) -> dict[str, Any]:
    if not types:
        raise ValueError("types list cannot be empty")
    in_list = [op_type.lower() for op_type in types]
    return {"type": {"$in": in_list}}


def is_op_all_transfer(op: OpAny) -> bool:
    """
    Check if the operation type is a transfer operation.

    Args:
        op: The operation to check.

    Returns:
        bool: True if the operation type is a transfer operation, False otherwise.
    """
    return op.op_type in ["transfer", "recurrent_transfer", "fill_recurrent_transfer"]
