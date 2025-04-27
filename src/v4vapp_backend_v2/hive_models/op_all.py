from typing import Any, Union

from v4vapp_backend_v2.hive_models.op_update_proposal_votes import UpdateProposalVotes

from .op_account_update2 import AccountUpdate2
from .op_account_witness_vote import AccountWitnessVote
from .op_base import OpBase
from .op_custom_json import CustomJson
from .op_fill_order import FillOrder
from .op_limit_order_create import LimitOrderCreate
from .op_producer_reward import ProducerReward
from .op_transfer import Transfer

OpMarket = Union[FillOrder, LimitOrderCreate]

OpAny = Union[Transfer, ProducerReward, AccountWitnessVote, CustomJson, AccountUpdate2, OpBase]
OpVirtual = Union[ProducerReward, FillOrder]
OpReal = Union[Transfer, AccountWitnessVote, CustomJson, AccountUpdate2]
OpRealOpsLoop = Union[OpAny, OpMarket]

OP_MAP: dict[str, OpAny] = {
    "custom_json": CustomJson,
    "transfer": Transfer,
    "account_witness_vote": AccountWitnessVote,
    "producer_reward": ProducerReward,
    "fill_order": FillOrder,
    "limit_order_create": LimitOrderCreate,
    "update_proposal_votes": UpdateProposalVotes,
    "account_update2": AccountUpdate2,
}


def op_tracked(op_type: str) -> bool:
    if op_type in OP_MAP:
        return True
    return False


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
        raise ValueError(f"Unknown operation type: {op_type_value}")

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
