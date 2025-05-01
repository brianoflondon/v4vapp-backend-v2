from typing import Any, Union

from v4vapp_backend_v2.hive_models.op_update_proposal_votes import UpdateProposalVotes

from .op_account_update2 import AccountUpdate2
from .op_account_witness_vote import AccountWitnessVote
from .op_base import OP_TRACKED, OpBase
from .op_custom_json import CustomJson
from .op_fill_order import FillOrder
from .op_fill_recurrent_transfer import FillRecurrentTransfer
from .op_limit_order_create import LimitOrderCreate
from .op_producer_reward import ProducerReward
from .op_recurrent_transfer import RecurrentTransfer
from .op_transfer import Transfer

OpMarket = Union[FillOrder, LimitOrderCreate]

OpAny = Union[Transfer, ProducerReward, AccountWitnessVote, CustomJson, AccountUpdate2, OpBase]
OpVirtual = Union[ProducerReward, FillOrder]
OpReal = Union[Transfer, AccountWitnessVote, CustomJson, AccountUpdate2]
OpAllTransfers = Union[Transfer, RecurrentTransfer, FillRecurrentTransfer]
OpAllRecurrent = Union[RecurrentTransfer, FillRecurrentTransfer]
OpRealOpsLoop = Union[OpAny, OpMarket]


# Important: This list must be kept in sync with the OP_TRACKED list in the
# op_base.py file.
OP_MAP: dict[str, OpAny] = {
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

# Check lists at startup


def op_tracked(op_type: str) -> bool:
    if op_type in OP_MAP:
        return True
    return False


def check_op_tracked() -> bool:
    # Convert OP_MAP keys and OP_TRACKED to sets
    op_map_keys = set(OP_MAP.keys())
    op_tracked_set = set(OP_TRACKED)

    # Find differences
    missing_in_op_tracked = op_map_keys - op_tracked_set
    missing_in_op_map = op_tracked_set - op_map_keys

    # Output results
    if missing_in_op_tracked:
        raise ValueError(f"Missing in OP_TRACKED: {missing_in_op_tracked}")

    if missing_in_op_map:
        raise ValueError(f"Missing in OP_MAP: {missing_in_op_map}")


# This runs before the app starts and will also prevent tests from running.
check_op_tracked()


def op_any(hive_event: dict[str, Any]) -> OpAny:
    """
    Factory function to create the appropriate OpBase subclass instance based on the
    provided Hive event data.

    Args:
        hive_event: A dictionary containing Hive event data

    Returns:
        An instance of the appropriate OpBase subclass or raises a ValueError if the operation
        type is unknown.
    """
    op_type_value = hive_event.get("type", None)
    if op_type_value is None:
        raise ValueError("Operation type not found in the event data")

    op_type = OP_MAP.get(op_type_value, None)
    if op_type is None:
        raise ValueError(
            f"Unknown operation type: {op_type_value} did you mean to use op_any_or_base?"
        )
    if "_id" in hive_event:
        del hive_event["_id"]
    return op_type.model_validate(hive_event)


def op_any_or_base(hive_event: dict) -> OpAny:
    """
    Factory function to create the appropriate OpBase subclass instance based on the
    provided Hive event data.

    Args:
        hive_event: A dictionary containing Hive event data

    Returns:
        An instance of the appropriate OpBase subclass or raises a ValueError if the operation
        type is unknown.
    """
    try:
        op_answer = op_any(hive_event)
        return op_answer
    except ValueError:
        try:
            op_answer = OpBase.model_validate(hive_event)
            return op_answer
        except ValueError as e:
            raise ValueError(f"Unknown operation type: {e}") from e
