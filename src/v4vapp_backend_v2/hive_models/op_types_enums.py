from enum import StrEnum, auto
from typing import Protocol

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
    """
    All OpTypes that are virtual operations.
    Virtual operations are ones which the Hive Blockchain core software carries out on its own
    during block processing. They are not initiated directly by users. These operations may appear
    on other lists.
    """

    PRODUCER_REWARD = auto()
    FILL_ORDER = auto()
    FILL_RECURRENT_TRANSFER = auto()

    # @classmethod
    # def all_values(cls):
    #     """Return a list of all member values."""
    #     return [member.value for member in cls]


def create_master_enum(*enums):
    members = {}
    for enum in enums:
        members.update(enum.__members__)
    return StrEnum("HiveTypes", members)


HiveOpTypes = create_master_enum(TransferOpTypes, MarketOpTypes)
# Used in real_ops_loop in hive_monitor_v2.py
RealOpsLoopTypes = create_master_enum(TransferOpTypes, MarketOpTypes, WitnessOpTypes)

OpTypes = create_master_enum(
    TransferOpTypes, MarketOpTypes, WitnessOpTypes, VirtualOpTypes
)


class OpTypeMixin:
    @property
    def log_str(self) -> str:
        raise NotImplementedError("Subclasses must implement this method")

    @property
    def notification_str(self) -> str:
        raise NotImplementedError("Subclasses must implement this method")


if __name__ == "__main__":
    print("fill_order" in HiveOpTypes)
    print("fill_order" in MarketOpTypes)
    print("fill_order" in TransferOpTypes)
    print(HiveOpTypes)
    print(list(HiveOpTypes))
