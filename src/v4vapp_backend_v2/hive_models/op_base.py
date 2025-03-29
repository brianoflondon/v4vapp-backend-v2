from collections import deque
from enum import StrEnum, auto
from typing import Any, ClassVar, Deque, Dict

from pydantic import BaseModel, Field, computed_field

from v4vapp_backend_v2.helpers.general_purpose_funcs import snake_case
from v4vapp_backend_v2.hive.hive_extras import get_hive_block_explorer_link
from v4vapp_backend_v2.hive_models.real_virtual_ops import (
    HIVE_REAL_OPS,
    HIVE_VIRTUAL_OPS,
)


class OpRealm(StrEnum):
    REAL = auto()
    VIRTUAL = auto()
    MARKER = auto()


class OpLogData(BaseModel):
    """
    OpLogData is a Pydantic model that represents the structure of log data.

    Attributes:
        log (str): The main log message.
        notification (str): A notification message associated with the log.
        log_extra (Dict[str, Any]): Additional data or metadata related to the log.
    """

    log: str
    notification: str
    log_extra: Dict[str, Any]


class OpBase(BaseModel):
    """
    OpBase is a base model representing a Hive blockchain operation. It provides attributes
    and methods to handle both real and virtual operations, along with logging and notification
    functionalities.

    Attributes:
        realm (OpRealm): Specifies whether the operation is REAL (user-generated) or VIRTUAL
            (blockchain-generated). Defaults to OpRealm.REAL.
        trx_id (str): The transaction ID associated with the operation.
        op_in_trx (int): The index of the operation within the block. Defaults to 0.
        type (str): The type of the event or operation.
        block_num (int): The block number containing the transaction.
        trx_num (int): The transaction number within the block.

    Methods:
        __init__(**data): Initializes the OpBase instance. Automatically sets the `realm`
            attribute based on the `type` of the operation. Raises a ValueError if the
            operation type is unknown.
        name() -> str: Returns the snake_case representation of the class name.
        log_extra() -> Dict[str, Any]: A property that returns a dictionary containing the
            serialized model data, keyed by the snake_case class name.
        log_str() -> str: A property that returns a formatted string for logging purposes,
            including the operation type, index, and a link to the Hive block explorer.
        notification_str() -> str: A property that returns a formatted string for notification
            purposes, including the operation type, index, and a markdown link to the Hive
            block explorer.
        logs() -> OpLogData: A property that returns an OpLogData object containing the log
            string, notification string, and additional log data.
    """

    realm: OpRealm = Field(
        default=OpRealm.REAL,
        description=(
            "Hive transactions are either REAL: user-generated or VIRTUAL:"
            "blockchain-generated"
        ),
    )
    trx_id: str = Field(description="Transaction ID")
    op_in_trx: int = Field(default=0, description="Operation index in the block")
    type: str = Field(description="Type of the event")
    block_num: int = Field(description="Block number containing this transaction")
    trx_num: int = Field(default=0, description="Transaction number within the block")

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
    def op_name(cls) -> str:
        return snake_case(cls.__name__)

    @property
    def log_extra(self) -> Dict[str, Any]:
        return {self.op_name(): self.model_dump()}

    @property
    def log_str(self) -> str:
        link = get_hive_block_explorer_link(self.trx_id)
        return f"{self.type} | {self.op_in_trx} | {link}"

    @property
    def notification_str(self) -> str:
        link = get_hive_block_explorer_link(self.trx_id, markdown=True)
        return f"{self.type} | {self.op_in_trx} | {link}"

    @property
    def logs(self) -> OpLogData:
        return OpLogData(
            log=self.log_str,
            notification=self.notification_str,
            log_extra=self.log_extra,
        )

    @computed_field
    def link(self) -> str:
        """
        Generates a link to the Hive block explorer for the transaction ID.

        Returns:
            str: A formatted string containing the link to the Hive block explorer.
        """
        if self.realm == OpRealm.MARKER:
            return f"MARKER: {self.trx_id}"
        return get_hive_block_explorer_link(
            trx_id=self.trx_id,
            block_num=self.block_num,
            op_in_trx=self.op_in_trx,
            markdown=False,
        )

    @property
    def markdown_link(self) -> str:
        """
        Generates a markdown link to the Hive block explorer for the transaction ID.

        Returns:
            str: A formatted markdown string containing the link to the Hive block explorer.
        """
        return get_hive_block_explorer_link(
            trx_id=self.trx_id,
            block_num=self.block_num,
            op_in_trx=self.op_in_trx,
            markdown=True,
        )


class OpInTrxCounter:
    """
    A class to track operation counts within transactions, with a shared history
    of the last 100 transaction IDs stored in a class-level deque.
    """

    # Class variables: Two separate deques for REAL and VIRTUAL transactions,
    # limited to 50 IDs each
    real_trx_id_stack: ClassVar[Deque[str]] = deque(maxlen=50)
    virtual_trx_id_stack: ClassVar[Deque[str]] = deque(maxlen=50)

    def __init__(self, realm: OpRealm) -> None:
        """
        Initialize an instance with its own operation counter and last transaction ID.

        Attributes:
            op_in_trx (int): Number of operations in the current transaction for this instance.
            last_trx_id (str): The ID of the last seen transaction for this instance.
        """
        self.op_in_trx: int = 1
        self.last_trx_id: str = ""
        self.op_real_virtual: OpRealm = realm

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
            return 1

        if self.last_trx_id == trx_id:
            self.op_in_trx += 1
            return self.op_in_trx

        # Case 2: Transaction exists in the shared stack,
        # update instance's last_trx_id and increment
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
        self.op_in_trx = 1  # Reset count for new transaction in this instance
        return 1


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
