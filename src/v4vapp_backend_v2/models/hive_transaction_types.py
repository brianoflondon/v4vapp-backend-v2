from enum import StrEnum, auto

"""
This module defines various operation types for Hive transactions using enumerations.

Classes:
    TransferOpTypes (StrEnum): Enumeration for transfer operation types.
        - TRANSFER: Represents a standard transfer operation.
        - RECURRENT_TRANSFER: Represents a recurrent transfer operation.

    MarketOpTypes (StrEnum): Enumeration for market operation types.
        - FILL_ORDER: Represents a fill order operation.
        - LIMIT_ORDER_CREATE: Represents a limit order creation operation.
        - LIMIT_ORDER_CANCEL: Represents a limit order cancellation operation.

    WitnessOpTypes (StrEnum): Enumeration for witness operation types.
        - ACCOUNT_WITNESS_VOTE: Represents an account witness vote operation.
        - PRODUCER_REWARD: Represents a producer reward operation.

    VirtualOpTypes (StrEnum): Enumeration for virtual operation types.
        - PRODUCER_REWARD: Represents a producer reward operation.
        - FILL_ORDER: Represents a fill order operation.
        - FILL_RECURRENT_TRANSFER: Represents a fill recurrent transfer operation.
        - all_values: Class method that returns a list of all member values.

Functions:
    create_master_enum(*enums): Creates a master enumeration by combining multiple enumerations.
        Args:
            *enums: Variable length argument list of enumerations to combine.
        Returns:
            StrEnum: A new enumeration combining all members of the provided enumerations.

Variables:
    HiveOpTypes (StrEnum): Combined enumeration of TransferOpTypes and MarketOpTypes.
    RealOpsLoopTypes (StrEnum): Combined enumeration of TransferOpTypes, MarketOpTypes,
    and WitnessOpTypes.

Usage:
    This module can be used to categorize and handle different types of Hive operations
    in a structured manner.
"""


class TransferOpTypes(StrEnum):
    TRANSFER = auto()
    RECURRENT_TRANSFER = auto()


class MarketOpTypes(StrEnum):
    FILL_ORDER = auto()
    LIMIT_ORDER_CREATE = auto()
    LIMIT_ORDER_CANCEL = auto()


class WitnessOpTypes(StrEnum):
    ACCOUNT_WITNESS_VOTE = auto()
    PRODUCER_REWARD = auto()


class VirtualOpTypes(StrEnum):
    PRODUCER_REWARD = auto()
    FILL_ORDER = auto()
    FILL_RECURRENT_TRANSFER = auto()

    @classmethod
    def all_values(cls):
        """Return a list of all member values."""
        return [member.value for member in cls]


def create_master_enum(*enums):
    members = {}
    for enum in enums:
        members.update(enum.__members__)
    return StrEnum("HiveTypes", members)


HiveOpTypes = create_master_enum(TransferOpTypes, MarketOpTypes)

# Used in real_ops_loop in hive_monitor_v2.py
RealOpsLoopTypes = create_master_enum(TransferOpTypes, MarketOpTypes, WitnessOpTypes)


# HIVE_TRANSFER_OP_TYPES = [
#     TransferOpTypes.TRANSFER,
#     TransferOpTypes.RECURRENT_TRANSFER,
# ]

# HIVE_MARKET_OP_TYPES = [
#     MarketOpTypes.FILL_ORDER,
#     MarketOpTypes.LIMIT_ORDER_CREATE,
#     MarketOpTypes.LIMIT_ORDER_CANCEL,
# ]


if __name__ == "__main__":
    print("fill_order" in HiveOpTypes)
    print("fill_order" in MarketOpTypes)
    print("fill_order" in TransferOpTypes)
    print(HiveOpTypes)
    print(list(HiveOpTypes))

# TRANSFER_OP_TYPES = ["transfer", "recurrent_transfer"]
# WITNESS_OP_TYPES = ["account_witness_vote"]

# MARKET_OP_TYPES = [
#     "fill_order",
#     "limit_order_create",
#     "limit_order_cancel",
# ]

# TRANSACTIONS_LOOP_OP_TYPES = TRANSFER_OP_TYPES + WITNESS_OP_TYPES + MARKET_OP_TYPES
# VIRTUAL_OP_TYPES = ["producer_reward", "fill_order"]


# OP_NAMES = TRANSFER_OP_TYPES + ["update_proposal_votes", "account_witness_vote"]
