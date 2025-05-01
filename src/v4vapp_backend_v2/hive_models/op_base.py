import re
from asyncio import get_event_loop
from datetime import datetime, timezone
from enum import StrEnum, auto
from typing import Any, ClassVar, Dict, List

from nectar import Hive
from pydantic import BaseModel, Field, computed_field

from v4vapp_backend_v2.accounting.account_type import AccountAny
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, QuoteResponse
from v4vapp_backend_v2.helpers.general_purpose_funcs import format_time_delta, snake_case
from v4vapp_backend_v2.hive_models.custom_json_data import all_custom_json_ids, custom_json_test_id
from v4vapp_backend_v2.hive_models.real_virtual_ops import HIVE_REAL_OPS, HIVE_VIRTUAL_OPS

# This list needs to be synced with op_all.py
OP_TRACKED = [
    "custom_json",
    "transfer",
    "account_witness_vote",
    "producer_reward",
    "fill_order",
    "limit_order_create",
    "update_proposal_votes",
    "account_update2",
    "fill_recurrent_transfer",
    "recurrent_transfer",
]

# I don't think I need the recurrent_transfer_cancel op because it doesn't affect me.


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


def op_realm(op_type: str):
    """
    Determines the operational realm based on the provided operation type.

    Args:
        op_type (str): The type of operation to evaluate. Can be one of the
                       predefined operation types or None.

    Returns:
        OpRealm: The corresponding operational realm, which can be one of the
                 following:
                 - OpRealm.VIRTUAL: If the operation type is in HIVE_VIRTUAL_OPS.
                 - OpRealm.REAL: If the operation type is in HIVE_REAL_OPS.
                 - OpRealm.MARKER: If the operation type is "block_marker".
                 - None: If the operation type is None or does not match any
                         predefined types.
    """
    if op_type is not None:
        if op_type in HIVE_VIRTUAL_OPS:
            return OpRealm.VIRTUAL
        elif op_type in HIVE_REAL_OPS:
            return OpRealm.REAL
        elif op_type == "block_marker":
            return OpRealm.MARKER
    return None


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


class WatchPair(BaseModel):
    """
    WatchPair is a Pydantic model that represents a pair of accounts to watch.

    Attributes:
        from_account (str): The account from which the operation originates.
        to_account (str): The account to which the operation is directed.
    """

    from_account: str | None = None
    to_account: str | None = None
    ledger_debit: AccountAny | None = None
    ledger_credit: AccountAny | None = None
    ledger_fee: AccountAny | None = None

    def __init__(self, **data):
        super().__init__(**data)


# These are not correct, they are just examples.
# watch_pairs: List[WatchPair] = [
#     WatchPair(
#         from_account="v4vapp.dhf",
#         to_account="privex",
#         ledger_debit=ExpenseAccount(name="Hosting Expenses Privex"),
#         ledger_credit=AssetAccount(name="V4VApp DHF"),
#         ledger_fee=None,
#     ),
#     WatchPair(
#         from_account="v4vapp.tre",
#         to_account="bdhivesteem",
#         ledger_debit=AssetAccount(name="V4VApp Treasury"),
#         ledger_credit=AssetAccount(name="Binance Hive Wallet"),
#         ledger_fee=None,
#     ),
# ]


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
        age() -> float: A property that calculates the age of the transaction in seconds
            based on the current time and the transaction timestamp.
        age_str() -> str: A property that returns a formatted string representing the age of

            the transaction, including a human-readable time delta if the age is greater than
            120 seconds.
        update_quote_sync(quote: QuoteResponse | None = None) -> None: A class method that
            synchronously updates the last quote for the class.
        update_quote(quote: QuoteResponse | None = None) -> None: A class method that
            asynchronously updates the last quote for the class. If no quote is provided,
            it fetches all quotes and sets the last quote to the fetched quote.
        update_conv(quote: QuoteResponse | None = None) -> None: Updates the conversion for
            the transaction. If the subclass has a `conv` object, it updates it with the
            latest quote. If a quote is provided, it sets the conversion to the provided
            quote. If no quote is provided, it uses the last quote to set the conversion.
        link() -> str: A computed property that generates a link to the Hive block explorer
            for the transaction ID. If the realm is OpRealm.MARKER, it returns a marker string.
        markdown_link() -> str: A property that generates a markdown link to the Hive block
            explorer for the transaction ID. If the realm is OpRealm.MARKER, it returns a
            marker string.
        _get_hive_block_explorer_link(markdown: bool = False) -> str: A private method that
            generates a Hive blockchain explorer URL for the transaction ID. It takes into
            account the realm and other parameters to construct the URL. It can return either
            an HTML link or a markdown link based on the `markdown` parameter.
    """

    realm: OpRealm = Field(
        default=OpRealm.REAL,
        description=(
            "Hive transactions are either REAL: user-generated or VIRTUAL:blockchain-generated"
        ),
    )
    trx_id: str = Field(description="Transaction ID")
    op_in_trx: int = Field(default=1, description="Operation index in the block")
    type: str = Field(description="Type of the event")
    block_num: int = Field(description="Block number containing this transaction")
    trx_num: int = Field(default=0, description="Transaction number within the block")
    timestamp: datetime = Field(description="Timestamp of the transaction in UTC format")
    extensions: List[Any] = Field(
        default=[], description="List of extensions associated with the operation"
    )

    raw_op: dict[str, Any] = Field(
        default={}, description="Raw operation data from the blockchain", exclude=True
    )

    # Class variables
    block_explorer: ClassVar[HiveExp] = HiveExp.HiveHub
    op_tracked: ClassVar[List[str]] = OP_TRACKED
    watch_users: ClassVar[List[str]] = []
    proposals_tracked: ClassVar[List[int]] = []
    custom_json_ids_tracked: ClassVar[List[str]] = []
    last_quote: ClassVar[QuoteResponse] = QuoteResponse()
    hive_inst: ClassVar[Hive | None] = None
    db_client: ClassVar[MongoDBClient | None] = None

    def __init__(self, **data):
        """
        Initializes an instance of the class with the provided data.

        Args:
            **data: Arbitrary keyword arguments containing the data to initialize the instance.

        Raises:
            ValueError: If the "type" field in the provided data is not recognized.

        Attributes:
            custom_json_ids_tracked (List[str]): Tracks custom JSON IDs, initialized if not already set.
            raw_op (dict): A copy of the input data.
            timestamp (datetime): Ensures the timestamp is timezone-aware, defaulting to UTC if not provided.
            realm (str): The realm determined by the operation type, based on the "type" field in the input data.
        """
        super().__init__(**data)
        if not hasattr(self, "custom_json_ids_tracked") or self.custom_json_ids_tracked is None:
            self.custom_json_ids_tracked = all_custom_json_ids()
        self.raw_op = data.copy()
        if (
            self.timestamp.tzinfo is None
            or self.timestamp.tzinfo.utcoffset(self.timestamp) is None
        ):
            self.timestamp = self.timestamp.replace(tzinfo=timezone.utc)
        if data.get("type", None) is not None:
            self.realm = op_realm(data["type"])
            if not self.realm:
                raise ValueError(f"Unknown operation type: {data['type']}")

    @property
    def group_id_query(self) -> dict[str, Any]:
        """
        Returns a Mongodb Query for this record.

        This method is used to determine the key in the database where
        the operation data will be stored. It is typically used for
        database operations and indexing.

        The mongodb is a compound of these three fields (and also the realm)

        Returns:
            dict: A dictionary containing the block number, transaction number,
            operation index in the transaction, and realm.
        """
        ans = {
            "block_num": self.block_num,
            "trx_num": self.trx_num,
            "op_in_trx": self.op_in_trx,
            "realm": self.realm,
        }
        # special case for OpRealm.MARKER (Overrides this default)
        return ans

    @property
    def group_id(self) -> str:
        """
        Returns a group ID for this record. This is a string used to uniquely identify
        the operation in the database.
        The group ID is a combination of the block number, transaction number,
        operation index in the transaction, and realm.
        This is used to determine the key in the database where the operation
        """
        group_id = f"{self.block_num}_{self.trx_num}_{self.op_in_trx}_{self.realm}"
        return group_id

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
            if self.cj_id in self.custom_json_ids_tracked:
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
    def is_watched(self) -> bool:
        """
        Check if the transfer is to a watched user.

        Returns:
            bool: True if the transfer is to a watched user, False otherwise.
        """
        if not OpBase.watch_users:
            return False
        if self.type == "custom_json" and hasattr(self, "cj_id"):
            if not custom_json_test_id(self.get("cj_id")):
                return False

        if OpBase.watch_users:
            if hasattr(self, "to_account") and self.to_account in OpBase.watch_users:
                return True
            # Check if the transfer is from a watched user
            if hasattr(self, "from_account") and self.from_account in OpBase.watch_users:
                return True
        return False

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
        return f"{self.age:.2f} | {self.timestamp:%Y-%m-%d %H:%M:%S} {self.realm:<8} | {self.type:<35} | {self.op_in_trx:<3} | {self.link}"

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

    @classmethod
    def update_quote_sync(cls, quote: QuoteResponse | None = None) -> None:
        """
        Synchronously updates the last quote for the class.

        Args:
            quote (QuoteResponse | None): The quote to update.

        Returns:
            None
        """
        if quote:
            cls.last_quote = quote
            return

        try:
            loop = get_event_loop()
            if loop.is_running():
                # If the event loop is already running, schedule the coroutine
                raise RuntimeError(
                    "update_quote_sync cannot be called in an async context. Use update_quote instead."
                )
            else:
                loop.run_until_complete(cls.update_quote())
        except RuntimeError as e:
            # Handle cases where the event loop is already running
            logger.error(f"Error in update_quote_sync: {e}")
            raise e

    @classmethod
    async def update_quote(cls, quote: QuoteResponse | None = None) -> None:
        """
        Asynchronously updates the last quote for the class.

        If a quote is provided, it sets the last quote to the provided quote.
        If no quote is provided, it fetches all quotes and sets the last quote
        to the fetched quote.

        Args:
            quote (QuoteResponse | None): The quote to update.
                If None, fetches all quotes.

        Returns:
            None
        """
        if quote:
            cls.last_quote = quote
        else:
            if cls.db_client:
                AllQuotes.db_client = cls.db_client
            all_quotes = AllQuotes()
            await all_quotes.get_all_quotes()
            cls.last_quote = all_quotes.quote

    async def update_quote_conv(self, quote: QuoteResponse | None = None) -> None:
        """
        Asynchronously updates the last quote for the class.

        If a quote is provided, it sets the last quote to the provided quote.
        If no quote is provided, it fetches all quotes and sets the last quote
        to the fetched quote.
        Uses the new quote to update a `conv` object.

        Args:
            quote (QuoteResponse | None): The quote to update.
                If None, fetches all quotes.

        Returns:
            None
        """
        await OpBase.update_quote(quote)
        self.update_conv()

    def update_conv(self, quote: QuoteResponse | None = None) -> None:
        """
        Updates the conversion for the transaction.

        If the subclass has a `conv` object, update it with the lastest quote.
        If a quote is provided, it sets the conversion to the provided quote.
        If no quote is provided, it uses the last quote to set the conversion.

        Args:
            quote (QuoteResponse | None): The quote to update.
                If None, uses the last quote.
        """
        if getattr(self, "conv", None) is not None:
            quote = quote or self.last_quote
            self.conv = CryptoConversion(amount=self.amount.beam, quote=quote).conversion
        else:
            return

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

    @property
    def lightning_memo(self) -> str:
        """
        Removes and shortens a lightning invoice from a memo for outpu.

        Returns:
            str: The shortened memo string.
        """
        # Regex pattern to capture 'lnbc' followed by numbers and one letter
        pattern = r"(lnbc\d+[a-zA-Z])"
        match = re.search(pattern, self.d_memo)
        if match:
            # Replace the entire memo with the matched lnbc pattern
            memo = f"⚡️{match.group(1)}...{self.d_memo[-5:]}"
        else:
            memo = f"💬{self.d_memo}"
        return memo
