from enum import StrEnum, auto
from typing import Any, Dict

from pydantic import BaseModel, Field, computed_field

from v4vapp_backend_v2.helpers.general_purpose_funcs import snake_case
from v4vapp_backend_v2.hive_models.real_virtual_ops import HIVE_REAL_OPS, HIVE_VIRTUAL_OPS


class OpRealm(StrEnum):
    REAL = auto()
    VIRTUAL = auto()
    MARKER = auto()


class HiveExp(StrEnum):
    HiveHub = "https://hivehub.dev/{prefix_path}"
    HiveScanInfo = "https://hivescan.info/{prefix_path}"
    # HiveBlockExplorer = "https://hiveblockexplorer.com/{prefix_path}"
    # HiveExplorer = "https://hivexplorer.com/{prefix_path}"


def get_hive_block_explorer_link(
    trx_id: str,
    block_explorer: HiveExp = HiveExp.HiveHub,
    markdown: bool = False,
    block_num: int = 0,
    op_in_trx: int = 0,
    realm: OpRealm = OpRealm.REAL,
    # any_op: OpBase | None = None,
) -> str:
    """
    Generate a Hive blockchain explorer URL for a given transaction ID.

    Args:
        trx_id (str): The transaction ID to include in the URL
        block_explorer (HiveExp): The blockchain explorer to use (defaults to HiveHub)

    Returns:
        str: The complete URL with the transaction ID inserted
    """

    if trx_id and not (block_num and op_in_trx):
        path = f"{trx_id}"
        prefix = "tx/"
    elif trx_id == "0000000000000000000000000000000000000000" and block_num:
        op_in_trx = op_in_trx if op_in_trx else 1
        prefix = f"{block_num}/"
        path = f"{trx_id}/{op_in_trx}"
    elif trx_id and block_num and op_in_trx and realm == OpRealm.VIRTUAL:
        path = f"{block_num}/{trx_id}/{op_in_trx}"
        prefix = "tx/"
    elif trx_id and op_in_trx and realm == OpRealm.REAL:
        if op_in_trx > 1:
            path = f"{trx_id}/{op_in_trx}"
        else:
            path = f"{trx_id}"
        prefix = "tx/"
    elif not trx_id and block_num:
        path = f"{block_num}"
        prefix = "b/"

    if block_explorer == HiveExp.HiveScanInfo or block_explorer == HiveExp.HiveExplorer:
        if prefix == "tx/":
            prefix = "transaction/"
        elif prefix == "b/":
            prefix = "block/"

    prefix_path = f"{prefix}{path}"

    link_html = block_explorer.value.format(prefix_path=prefix_path)
    if not markdown:
        return link_html
    markdown_link = f"[{block_explorer.name}]({link_html})"
    return markdown_link


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
        return f"{self.block_num:,} | {self.realm:<8} | {self.type:<35} | {self.op_in_trx:<3} | {self.link}"

    @property
    def notification_str(self) -> str:
        return f"{self.type} | {self.op_in_trx} | {self.markdown_link}"

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
        return self._get_hive_block_explorer_link(markdown=False)

    @property
    def markdown_link(self) -> str:
        """
        Generates a markdown link to the Hive block explorer for the transaction ID.

        Returns:
            str: A formatted markdown string containing the link to the Hive block explorer.
        """
        if self.realm == OpRealm.MARKER:
            return f"MARKER: {self.trx_id}"
        return self._get_hive_block_explorer_link(markdown=True)

    def _get_hive_block_explorer_link(self, markdown: bool = False) -> str:
        """
        Generate a Hive blockchain explorer URL for a given transaction ID.

        Args:
            trx_id (str): The transaction ID to include in the URL
            block_explorer (HiveExp): The blockchain explorer to use (defaults to HiveHub)

        Returns:
            str: The complete URL with the transaction ID inserted
        """

        if self.realm == OpRealm.REAL:
            prefix = "tx/"
            path = f"{self.trx_id}"
            # if self.op_in_trx > 1:
            #     path = f"{self.trx_id}/{self.op_in_trx}"
            # else:

        elif self.realm == OpRealm.VIRTUAL:
            prefix = "tx/"
            path = f"{self.block_num}/{self.trx_id}/{self.op_in_trx}"

        if self.block_explorer == HiveExp.HiveScanInfo:
            if prefix == "tx/":
                prefix = "transaction/"
            elif prefix == "b/":
                prefix = "block/"

        prefix_path = f"{prefix}{path}"

        link_html = self.block_explorer.value.format(prefix_path=prefix_path)
        if not markdown:
            return link_html
        markdown_link = f"[{self.block_explorer.name}]({link_html})"
        return markdown_link
