from datetime import datetime, timezone
from enum import StrEnum, auto
from typing import Any, ClassVar, Dict

from pydantic import BaseModel, Field, computed_field

from v4vapp_backend_v2.helpers.general_purpose_funcs import format_time_delta, snake_case
from v4vapp_backend_v2.hive_models.custom_json_data import custom_json_test_id
from v4vapp_backend_v2.hive_models.real_virtual_ops import HIVE_REAL_OPS, HIVE_VIRTUAL_OPS

OP_TRACKED = [
    "custom_json",
    "transfer",
    "account_witness_vote",
    "producer_reward",
    "fill_order",
    "limit_order_create",
]


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

    if block_explorer == HiveExp.HiveScanInfo:
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
    timestamp: datetime = Field(description="Timestamp of the transaction in UTC format")

    raw_op: dict[str, Any] = Field(
        default={}, description="Raw operation data from the blockchain", exclude=True
    )
    block_explorer: ClassVar[HiveExp] = HiveExp.HiveHub

    def __init__(self, **data):
        super().__init__(**data)
        self.raw_op = data.copy()
        if (
            self.timestamp.tzinfo is None
            or self.timestamp.tzinfo.utcoffset(self.timestamp) is None
        ):
            self.timestamp = self.timestamp.replace(tzinfo=timezone.utc)
        if data.get("type", None) is not None:
            if data["type"] in HIVE_VIRTUAL_OPS:
                self.realm = OpRealm.VIRTUAL
            elif data["type"] in HIVE_REAL_OPS:
                self.realm = OpRealm.REAL
            elif data["type"] == "block_marker":
                self.realm = OpRealm.MARKER
            else:
                raise ValueError(f"Unknown operation type: {data['type']}")

    @classmethod
    def name(cls) -> str:
        """
        Returns the name of the class in snake_case format.

        This method converts the class name to a snake_case string
        representation, which is typically used for naming operations
        or identifiers in a consistent and readable format.

        Returns:
            str: The snake_case representation of the class name.
        """
        return snake_case(cls.__name__)

    @property
    def known_custom_json(self) -> bool:
        """
        Determines if the operation is a special custom JSON operation.

        Returns:
            bool: True if the operation is a special custom JSON operation, False otherwise.
        """
        if hasattr(self, "cj_id"):
            if custom_json_test_id(self.cj_id):
                return True
        return False

    @property
    def tracked(self) -> bool:
        if self.type == "custom_json":
            return self.known_custom_json
        else:
            return self.type in OP_TRACKED

    @property
    def log_extra(self) -> Dict[str, Any]:
        """
        Generates a dictionary containing additional logging information.
        Usage: in a log entry use as an unpacked dictionary like this:
        `logger.info(f"{op.block_num} | {op.log_str}", extra={**op.log_extra})`

        Returns:
            Dict[str, Any]: A dictionary where the key is the name of the current instance
            and the value is the serialized representation of the instance, excluding the
            "raw_op" field.
        """
        return {self.name(): self.model_dump(exclude={"raw_op"})}

    @property
    def log_str(self) -> str:
        return f"{self.block_num:,} | {self.age:.2f} | {self.timestamp:%Y-%m-%d %H:%M:%S} {self.realm:<8} | {self.type:<35} | {self.op_in_trx:<3} | {self.link}"

    @property
    def notification_str(self) -> str:
        return f"{self.type} | {self.op_in_trx} | {self.markdown_link}"

    @property
    def age(self) -> float:
        """
        Calculates the age of the transaction based on the current time and the transaction timestamp.
        in seconds

        Returns:
            age: The time difference between the current time and the transaction timestamp in seconds.
        """
        return (datetime.now(tz=timezone.utc) - self.timestamp).total_seconds()

    @property
    def age_str(self) -> str:
        age_text = f" {format_time_delta(self.age)}" if self.age > 120 else ""
        return age_text

    @property
    def logs(self) -> OpLogData:
        """
        Retrieves the operation log data.

        Returns:
            OpLogData: An object containing the log string, notification string,
            and additional log information.
        """
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

        elif self.realm == OpRealm.VIRTUAL:
            prefix = "tx/"
            path = f"{self.block_num}/{self.trx_id}/{self.op_in_trx}"

        if OpBase.block_explorer == HiveExp.HiveScanInfo:
            if prefix == "tx/":
                prefix = "transaction/"
            elif prefix == "b/":
                prefix = "block/"

        prefix_path = f"{prefix}{path}"

        link_html = OpBase.block_explorer.value.format(prefix_path=prefix_path)
        if not markdown:
            return link_html
        return f"[{OpBase.block_explorer.name}]({link_html})"
