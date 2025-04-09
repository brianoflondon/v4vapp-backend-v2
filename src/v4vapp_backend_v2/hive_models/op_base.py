from collections import deque
from dataclasses import dataclass
from enum import StrEnum, auto
from typing import Any, ClassVar, Deque, Dict

from pydantic import BaseModel, Field, computed_field

from v4vapp_backend_v2.helpers.general_purpose_funcs import snake_case
from v4vapp_backend_v2.hive.hive_extras import HiveExp, get_hive_block_explorer_link
from v4vapp_backend_v2.hive_models.real_virtual_ops import HIVE_REAL_OPS, HIVE_VIRTUAL_OPS


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
            "Hive transactions are either REAL: user-generated or VIRTUAL:blockchain-generated"
        ),
    )
    trx_id: str = Field(description="Transaction ID")
    op_in_trx: int = Field(default=0, description="Operation index in the block")
    type: str = Field(description="Type of the event")
    block_num: int = Field(description="Block number containing this transaction")
    trx_num: int = Field(default=0, description="Transaction number within the block")

    block_explorer: HiveExp = Field(
        default=HiveExp.HiveHub,
        exclude=True,
        description="Hive Block explorer to use for links",
    )

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
            block_explorer=self.block_explorer,
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
            block_explorer=self.block_explorer,
            markdown=True,
        )

