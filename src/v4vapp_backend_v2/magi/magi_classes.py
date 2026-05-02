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
from v4vapp_backend_v2.helpers.general_purpose_funcs import (
    detect_paywithsats,
    paywithsats_amount,
    snake_case,
)
from v4vapp_backend_v2.hive_models.account_name_type import AccName
from v4vapp_backend_v2.hive_models.magi_json_data import VSCCall, VSCCallPayload
from v4vapp_backend_v2.hive_models.op_all import trx_unpack
from v4vapp_backend_v2.hive_models.op_base_extras import HiveExp
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson

ICON = "🧙‍♂️"
DB_MAGI_BTC_COLLECTION = "magi_btc"

# Hive transaction IDs are always exactly 40 lowercase hex characters.
_HIVE_TRX_ID_RE = re.compile(r"^[0-9a-f]{40}$")


class MagiBTCBalanceError(Exception):
    """Custom exception for errors related to fetching Magi BTC balance."""

    pass


class MagiSatsInboundFollowOnTransferError(Exception):
    """Custom exception for errors related to processing follow-on transfers for Magi sats inbound events."""

    pass


class MagiBTCBalance(BaseModel):
    account: str
    balance_sats: Decimal
    error: str | None = None

    @property
    def sats(self) -> Decimal:
        return self.balance_sats

    @property
    def msats(self) -> Decimal:
        return self.balance_sats * Decimal(1000)

    @property
    def balance_msats(self) -> Decimal:
        return self.balance_sats * Decimal(1000)


class MagiBTCTransferEvent(TrackedBaseModel):
    from_addr: str = Field(
        "", description="The sender's account name, including network prefix (e.g. 'hive:alice')"
    )
    to_addr: str = Field(
        "", description="The recipient's account name, including network prefix (e.g. 'hive:bob')"
    )
    amount: Decimal = Field(Decimal(0), description="The amount transferred, in sats")
    indexer_block_height: int = Field(
        0, description="The block height at which the transfer was indexed"
    )
    indexer_tx_hash: str = Field(
        "",
        description="The transaction hash from the indexer, may include -N suffix for multiple ops",
    )
    indexer_ts: str = Field(
        "", description="The timestamp from the indexer for when the transfer was indexed"
    )
    indexer_id: int = Field(
        0, description="The unique ID from the indexer for this transfer event"
    )

    cust_id: str = Field("", description="Customer ID determined from to/from fields")
    timestamp: datetime = Field(
        datetime(1970, 1, 1, tzinfo=timezone.utc),
        description="Timestamp for the event",
    )
    custom_jsons: List[CustomJson] | None = Field(
        None,
        description="The CustomJson operations associated with this transfer, if any",
    )
    memo: str = Field("", description="The memo associated with this transfer, if any")

    block_explorer: ClassVar[HiveExp] = HiveExp.HiveHub

    model_config = ConfigDict(populate_by_name=True)

    def __init__(self, **data: Any):
        super().__init__(**data)
        try:
            ts = datetime.fromisoformat(self.indexer_ts)
            # fromisoformat returns a naive datetime when no timezone is present;
            # treat it as UTC so arithmetic with offset-aware datetimes works.
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            self.timestamp = ts
        except ValueError:
            self.timestamp = datetime.now(tz=timezone.utc)
        self.cust_id = self.get_cust_id()
        # Ensure amount is a Decimal for consistency
        if not isinstance(self.amount, Decimal):
            self.amount = Decimal(self.amount)

    @property
    def do_not_pay(self) -> bool:
        """
        Determines if this transfer should be marked as "do not pay" based on the presence of a specific flag in the memo.
        We only pay onward Magi transactions if they have a #magioutbound flag in the memo.

        This is a placeholder implementation. The actual logic for determining "do not pay" status may involve
        checking for specific keywords or flags in the memo or other fields.

        Returns:
            bool: True if the transfer should be marked as "do not pay", False otherwise.
        """
        if self.amount <= 0:
            return True
        if self.d_memo and "#magioutbound" in self.memo.lower():
            return False
        return True

    @property
    def pay_with_sats(self) -> bool:
        """
        Determines if this transfer should be paid with sats based on the amount and "do not pay" status.

        Returns:
            bool: True if the transfer should be paid with sats, False otherwise.
        """
        return not self.do_not_pay and self.amount > 0

    @property
    def paywithsats(self) -> bool:
        """
        This mirrors the flag in the CustomJson for the follow-on transfer,
        but is derived here for easier access when processing the initial transfer event.

        This is used by mark: We have a pay_req, we will pay it in `process_transfer.py`

        Returns:
            bool: True if the transfer should be marked as "pay with sats", False otherwise.
        """
        if not self.do_not_pay:
            return True
        if detect_paywithsats(self.memo):
            return True
        return False

    @property
    def paywithsats_amount(self) -> Decimal:
        """
        Extracts and returns the 'paywithsats' amount from the memo if present.
        This is in sats, not msats.

        Returns:
            Decimal: The amount specified in the memo after 'paywithsats:', or 0 if not present or not applicable.

        Notes:
            - The memo is expected to be in the format "paywithsats:amount".
            - If 'paywithsats' is not enabled or the memo does not match the expected format, returns 0.
        """
        return paywithsats_amount(self.memo)

    @property
    def d_memo(self) -> str:
        """
        This is a placeholder for the memo field, which may be derived from associated CustomJson operations or other sources.

        Returns:
            str: The memo associated with this transfer event.
        """
        if self.memo:
            return self.memo

        if self.custom_jsons:
            for cj in self.custom_jsons:
                if isinstance(cj.json_data, VSCCall):
                    payload = cj.json_data.payload
                    if isinstance(payload, VSCCallPayload):
                        self.memo = payload.memo
                        return payload.memo
        return ""

    def max_send_amount_msats(self) -> Decimal:
        """
        Calculates the maximum amount in millisatoshis that can be sent based on the transfer amount in sats.
        Needs to include a fee estimate for the follow-on payment if paywithsats is enabled.

        Returns:
            Decimal: The maximum send amount in millisatoshis.
        """
        if not self.paywithsats:
            return Decimal(0)
        if not self.conv:
            return Decimal(0)
        msats_fee = self.conv.msats_fee
        send_sats = self.amount - (msats_fee / Decimal(1000)).quantize(
            Decimal("1."), rounding="ROUND_UP"
        )
        lnd_config = InternalConfig().config.lnd_config
        fee_estimate_msats = Decimal(
            Decimal(lnd_config.lightning_fee_base_msats)
            + (
                (send_sats * Decimal(2000))
                * Decimal(lnd_config.lightning_fee_estimate_ppm)
                / 1_000_000
            )
        ).quantize(Decimal("1."), rounding="ROUND_UP")
        return send_sats * Decimal(1000) - fee_estimate_msats

    @property
    def collection_name(self) -> str:
        return DB_MAGI_BTC_COLLECTION

    @classmethod
    def collection(cls) -> AsyncCollection:
        return InternalConfig.db[DB_MAGI_BTC_COLLECTION]

    @computed_field
    def from_account(self) -> str:
        """
        Returns the sender account stripped of the network prefix.
        For example, "hive:alice" becomes "alice". If the sender does not have a known prefix, it is returned unchanged.
        """
        acc_name = AccName(self.from_addr)
        return acc_name.no_prefix

    @computed_field
    def to_account(self) -> str:
        """
        Returns the recipient account stripped of the network prefix.
        For example, "hive:alice" becomes "alice". If the recipient does not have a known prefix, it is returned unchanged.
        """
        acc_name = AccName(self.to_addr)
        return acc_name.no_prefix

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
    def all_accounts(self) -> List[str]:
        """
        Returns a list with 'from' and 'to' accounts in their original network formats.

        This is a convenience property to easily access the relevant accounts for the transfer.

        Returns:
            List[str]: A list with the 'from' and 'to' accounts.

        """
        # problem with computed fields.
        return [self.from_account, self.to_account]  # type: ignore

    def get_cust_id(self) -> str:
        server_id = InternalConfig().server_id
        if self.from_account == server_id:
            return self.to_account  # type: ignore
        if self.to_account == server_id:
            return self.from_account  # type: ignore
        return f"{self.from_account}:{self.to_account}"

    @property
    def is_watched(self) -> bool:
        """
        Determines if this transfer event involves any accounts that are being watched.

        Checks either the sender and recipient against the server's own ID and a list of
        watched users from the configuration.

        Returns:
            bool: True if any watched accounts are involved, False otherwise.
        """
        server_id = InternalConfig().server_id
        if self.from_account == server_id:
            return True
        if self.to_account == server_id:
            return True
        watch_users = InternalConfig().config.hive_config.watch_users
        if self.from_account in watch_users:
            return True
        if self.to_account in watch_users:
            return True
        return False

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

        Returns:
            str: The group ID for this transfer event, formatted as indexer_id_trx_id_magi.

        **Important:** This group ID is used by `load_tracked_object` in `tracked_any.py`
        to determine which operations belong together for processing and database storage.
        It is crucial that all operations derived from the same indexer record share the same group ID

        `_m` and `_magi` are used to identify Magi-related operations in the database, so they must be included in the group ID.

        """
        return f"{self.indexer_id}_{self.trx_id}_magi"

    @computed_field
    def short_id(self) -> str:
        """
        Returns a short ID for this record. This is a string used to uniquely identify
        the operation in the database.
        The short ID is a combination of the block number, transaction number,
        operation index in the transaction, and realm.
        This is used to determine the key in the database where the operation
        """
        return f"{self.trx_id[:8]}_m"

    @property
    def short_id_p(self) -> str:
        return self.short_id  # type: ignore

    @property
    def group_id_p(self) -> str:
        return self.group_id  # type: ignore

    @property
    def group_id_query(self) -> Dict[str, Any]:
        return {"group_id": self.group_id}

    @property
    def op_type(self) -> str:
        """
        Returns the operation type for the Magi BTC transfer event.

        Returns:
            str: The operation type for the Magi BTC transfer event, which is always "magi_btc_transfer_event".
        """
        return "magi_btc_transfer_event"

    async def update_conv(self, quote: QuoteResponse | None = None) -> None:
        if not self.memo:
            self.memo = self.d_memo
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
        self.memo = self.d_memo
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
