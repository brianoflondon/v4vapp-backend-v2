import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, ClassVar, Dict, List

from pydantic import BaseModel, ConfigDict, Field, computed_field
from pymongo.asynchronous.collection import AsyncCollection

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import snake_case
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType
from v4vapp_backend_v2.hive_models.op_all import trx_unpack
from v4vapp_backend_v2.hive_models.op_base_extras import HiveExp
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson

ICON = "🧙‍♂️"
DB_MAGI_BTC_COLLECTION = "magi_btc"

# Hive transaction IDs are always exactly 40 lowercase hex characters.
_HIVE_TRX_ID_RE = re.compile(r"^[0-9a-f]{40}$")


class MagiBTCBalanceError(Exception):
    """Custom exception for errors related to fetching Magi BTC balance."""


class MagiBTCBalance(BaseModel):
    account: AccNameType
    balance_sats: Decimal
    error: str | None = None

    @property
    def balance_msats(self) -> Decimal:
        return self.balance_sats * Decimal(1000)


class MagiBTCTransferEvent(TrackedBaseModel):
    from_addr: AccNameType
    to_addr: AccNameType
    amount: Decimal
    indexer_block_height: int
    indexer_tx_hash: str
    indexer_ts: str
    indexer_id: int

    timestamp: datetime = Field(
        datetime(1970, 1, 1, tzinfo=timezone.utc),
        description="Timestamp for the event",
    )

    block_explorer: ClassVar[HiveExp] = HiveExp.HiveHub

    model_config = ConfigDict(populate_by_name=True)

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.timestamp = datetime.fromisoformat(self.indexer_ts)
        # Ensure amount is a Decimal for consistency
        if not isinstance(self.amount, Decimal):
            self.amount = Decimal(self.amount)

    @property
    def collection_name(self) -> str:
        return DB_MAGI_BTC_COLLECTION

    @classmethod
    def collection(cls) -> AsyncCollection:
        return InternalConfig.db[DB_MAGI_BTC_COLLECTION]

    @computed_field
    def op_in_trx(self) -> int:
        """
        Derives the operation index within the transaction from the indexer_tx_hash suffix.

        The indexer appends a 0-based counter to the tx hash when multiple operations share
        the same transaction (e.g. "abc123-0" is the first, "abc123-1" the second).
        No suffix means a single operation, which maps to op_in_trx = 1.

        Returns:
            int: 1-based operation index (no suffix → 1, suffix -0 → 1, -1 → 2, ...).
        """
        if "-" in self.indexer_tx_hash:
            suffix = self.indexer_tx_hash.rsplit("-", 1)[1]
            try:
                return int(suffix) + 1
            except ValueError:
                return 1
        return 1

    @computed_field
    def to_from_accounts(self) -> List[str]:
        """
        Returns a list with 'from' and 'to' accounts in their original network formats.

        This is a convenience property to easily access the relevant accounts for the transfer.

        Returns:
            List[str]: A list with the 'from' and 'to' accounts.

        """
        return [self.to_addr, self.from_addr]

    @property
    def trx_id(self) -> str:
        """
        Extracts the base transaction ID by removing any trailing operation index suffix.

        Returns:
            str: The base transaction ID (e.g. "abc123" from "abc123-0").
        """
        tx_hash = self.indexer_tx_hash
        if "-" in tx_hash:
            return tx_hash.rsplit("-", 1)[0]
        return tx_hash

    @computed_field
    def group_id(self) -> str:
        """
        Returns a group ID analogous to OpBase: block_height_txhash_op_in_trx_realm.
        The trailing -N suffix is stripped from the tx hash; the index is captured in op_in_trx.
        """
        return f"{self.indexer_block_height}_{self.trx_id}_{self.op_in_trx}_real"

    @computed_field
    def short_id(self) -> str:
        """
        Returns a short ID for this record. This is a string used to uniquely identify
        the operation in the database.
        The short ID is a combination of the block number, transaction number,
        operation index in the transaction, and realm.
        This is used to determine the key in the database where the operation
        """
        # Give the last 4 digits of the block number and first 5 chars of the trx_id
        block_num_str = str(self.indexer_block_height)
        short_block_num = f"{block_num_str[-4:]}"
        short_trx_id = self.trx_id[:6]
        short_op_in_trx = f"_{self.op_in_trx}"
        return f"{short_block_num}_{short_trx_id}{short_op_in_trx}"

    @property
    def short_id_p(self) -> str:
        return self.short_id  # Type: ignore

    @property
    def group_id_p(self) -> str:
        tx_hash = self.indexer_tx_hash
        if "-" in tx_hash:
            tx_hash = tx_hash.rsplit("-", 1)[0]
        return f"{self.indexer_block_height}_{tx_hash}_{self.op_in_trx}_real"

    @property
    def group_id_query(self) -> Dict[str, Any]:
        return {"indexer_id": self.indexer_id}

    @property
    def op_type(self) -> str:
        """
        Returns the operation type for the Magi BTC transfer event.

        Returns:
            str: The operation type for the Magi BTC transfer event, which is always "magi_btc_transfer_event".
        """
        return "magi_btc_transfer_event"

    async def update_conv(self, quote: QuoteResponse | None = None) -> None:
        if not quote:
            quote = await TrackedBaseModel.nearest_quote(self.timestamp)
        self.conv = CryptoConversion(
            conv_from=Currency.SATS,
            value=self.amount,
            quote=quote,
        ).conversion

    async def hive_custom_json(self) -> List[CustomJson] | None:
        """
        Fetch and return all CustomJson operations from the Hive transaction
        matching this transfer's trx_id.

        IPFS CID hashes (e.g. bafyrei...) are not valid Hive transaction IDs;
        for those events there is no on-chain custom_json to look up.

        Returns:
            List[CustomJson] | None: Matching CustomJson ops, or None if none found.
        """
        if not _HIVE_TRX_ID_RE.match(self.trx_id):
            logger.debug(
                f"{ICON} trx_id={self.trx_id!r} is not a Hive txid — skipping custom_json lookup",
                extra={"notification": False},
            )
            return None
        ops = trx_unpack(self.trx_id)
        matching = [op for op in ops if isinstance(op, CustomJson)]
        if not matching:
            logger.warning(
                f"{ICON} No custom_json found for indexer_tx_hash={self.indexer_tx_hash}",
                extra={"notification": False},
            )
            return None
        return matching

    @property
    def log_str(self) -> str:
        return (
            f"{ICON} Transfer {self.from_addr:>18} -> {self.to_addr:>18} "
            f"{self.amount:,.0f} sats (indexer_id={self.indexer_id}) {self.link or ''}"
        )

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
        return {self.name(): self.model_dump(by_alias=True)}

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

    def _get_btc_explorer_link(self, markdown: bool = False) -> str:
        """
        Generate a block explorer URL for this BTC-on-Hive transaction.

        Args:
            markdown (bool): If True, returns a markdown-formatted link.

        Returns:
            str: The complete URL (or markdown link) for the transaction.
        """
        tx_hash = self.indexer_tx_hash
        if "-" in tx_hash:
            tx_hash = tx_hash.rsplit("-", 1)[0]
        prefix_path = f"tx/{tx_hash}"
        link_html = MagiBTCTransferEvent.block_explorer.value.format(prefix_path=prefix_path)
        if not markdown:
            return link_html
        return f"[{MagiBTCTransferEvent.block_explorer.name}]({link_html})"

    @property
    def link(self) -> str:
        return self._get_btc_explorer_link(markdown=False)

    @property
    def markdown_link(self) -> str:
        return self._get_btc_explorer_link(markdown=True)
