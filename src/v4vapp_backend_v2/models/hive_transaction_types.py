from enum import StrEnum, auto


class TransferOpTypes(StrEnum):
    TRANSFER = auto()
    RECURRENT_TRANSFER = auto()


class MarketOpTypes(StrEnum):
    FILL_ORDER = auto()
    LIMIT_ORDER_CREATE = auto()
    LIMIT_ORDER_CANCEL = auto()


class WitnessOpTypes(StrEnum):
    ACCOUNT_WITNESS_VOTE = auto()


class VirtualOpTypes(StrEnum):
    PRODUCER_REWARD = auto()
    FILL_ORDER = auto()

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
TransactionLoopOpTypes = create_master_enum(
    TransferOpTypes, MarketOpTypes, WitnessOpTypes
)


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
    print(HiveOpTypes.values())


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
