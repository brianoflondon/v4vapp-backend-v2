from collections import deque
from enum import Enum, auto
from typing import Any, ClassVar, Deque, Dict

from pydantic import BaseModel, Field

from v4vapp_backend_v2.helpers.general_purpose_funcs import snake_case
from v4vapp_backend_v2.hive_models.real_virtual_ops import (
    HIVE_REAL_OPS,
    HIVE_VIRTUAL_OPS,
)


class OpRealm(Enum):
    REAL = auto()
    VIRTUAL = auto()


class OpBase(BaseModel):
    realm: OpRealm = Field(
        default=OpRealm.REAL,
        description="Hive transactions are either REAL: user-generated or VIRTUAL: blockchain-generated",
    )
    trx_id: str = Field(description="Transaction ID")
    op_in_trx: int = Field(default=0, description="Operation index in the block")
    type: str = Field(description="Type of the event")

    def __init__(self, **data):
        super().__init__(**data)
        if data.get("type", None) is not None:
            if data["type"] in HIVE_VIRTUAL_OPS:
                self.realm = OpRealm.VIRTUAL
            elif data["type"] in HIVE_REAL_OPS:
                self.realm = OpRealm.REAL
            else:
                raise ValueError(f"Unknown operation type: {data['type']}")

    @classmethod
    def name(cls) -> str:
        return snake_case(cls.__name__)

    @property
    def log_extra(self) -> Dict[str, Any]:
        return {self.name(): self.model_dump()}


class OpInTrxCounter:
    """
    A class to track operation counts within transactions, with a shared history
    of the last 100 transaction IDs stored in a class-level deque.
    """

    # Class variables: Two separate deques for REAL and VIRTUAL transactions, limited to 100 IDs each
    real_trx_id_stack: ClassVar[Deque[str]] = deque(maxlen=100)
    virtual_trx_id_stack: ClassVar[Deque[str]] = deque(maxlen=100)

    def __init__(self, op_real_virtual: OpRealm) -> None:
        """
        Initialize an instance with its own operation counter and last transaction ID.

        Attributes:
            op_in_trx (int): Number of operations in the current transaction for this instance.
            last_trx_id (str): The ID of the last seen transaction for this instance.
        """
        self.op_in_trx: int = 0
        self.last_trx_id: str = ""
        self.op_real_virtual: OpRealm = op_real_virtual

    def inc(self, trx_id: str) -> int:
        """
        Increment the operation count for a given transaction ID and return the count.
        If the transaction ID is new, reset the instance's count and add it to the shared stack.
        If it matches the instance's last transaction ID or is in the stack, increment the count.

        Args:
            trx_id (str): The transaction ID to process.

        Returns:
            int: The current operation count for the transaction in this instance.
        """
        # Case 1: Same transaction as last time for this instance, just increment
        if trx_id == "0000000000000000000000000000000000000000":
            return 0

        if self.last_trx_id == trx_id:
            self.op_in_trx += 1
            return self.op_in_trx

        # Case 2: Transaction exists in the shared stack, update instance's last_trx_id and increment
        if self.op_real_virtual == OpRealm.REAL:
            use_stack = OpInTrxCounter.real_trx_id_stack
        else:  # OpRealVirtual.VIRTUAL
            use_stack = OpInTrxCounter.virtual_trx_id_stack

        if trx_id in use_stack:
            self.last_trx_id = trx_id
            self.op_in_trx += 1
            return self.op_in_trx

        # Case 3: New transaction, reset instance count and add to shared stack
        use_stack.append(trx_id)  # Access class variable
        self.last_trx_id = trx_id
        self.op_in_trx = 0  # Reset count for new transaction in this instance
        return 0


def op_in_trx_counter(
    op_in_trx: int, last_trx_id: str, post: Dict[str, Any]
) -> tuple[int, str]:
    if last_trx_id == post["trx_id"]:
        op_in_trx += 1
    else:
        op_in_trx = 0
        last_trx_id = post["trx_id"]
    post["op_in_trx"] = op_in_trx
    return op_in_trx, last_trx_id
