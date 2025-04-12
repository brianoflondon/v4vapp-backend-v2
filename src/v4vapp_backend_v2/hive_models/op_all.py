from typing import Any, Union

from .op_account_witness_vote import AccountWitnessVote
from .op_base import OpBase
from .op_custom_json import CustomJson
from .op_fill_order import FillOrder
from .op_limit_order_create import LimitOrderCreate
from .op_producer_reward import ProducerReward
from .op_transfer import Transfer

OpMarket = Union[FillOrder, LimitOrderCreate]

OpAny = Union[Transfer, ProducerReward, AccountWitnessVote, CustomJson, OpBase]
OpVirtual = Union[ProducerReward, FillOrder]
OpReal = Union[Transfer, AccountWitnessVote, CustomJson]
OpRealOpsLoop = Union[OpAny, OpMarket]


def op_any(hive_event: dict[str, Any]) -> OpAny:
    """
    Factory function to create the appropriate OpBase subclass instance based on the
    provided Hive event data.

    Args:
        hive_event: A dictionary containing Hive event data

    Returns:
        An instance of the appropriate OpBase subclass
    """
    op_type = hive_event.get("type", None)
    if op_type is None:
        raise ValueError("Operation type not found in the event data")
    if op_type not in [
        "custom_json",
        "transfer",
        "account_witness_vote",
        "producer_reward",
        "fill_order",
        "limit_order_create",
    ]:
        raise ValueError(f"Unknown operation type: {op_type}")

    if op_type == "custom_json":
        return CustomJson(**hive_event)
    elif op_type == "transfer":
        return Transfer(**hive_event)
    elif op_type == "account_witness_vote":
        return AccountWitnessVote(**hive_event)
    elif op_type == "producer_reward":
        return ProducerReward(**hive_event)
    elif op_type == "fill_order":
        return FillOrder(**hive_event)
    elif op_type == "limit_order_create":
        return LimitOrderCreate(**hive_event)

    else:
        raise ValueError(f"Unknown operation type: {op_type}")
