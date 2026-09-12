import asyncio
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, ClassVar, Dict, List

from pydantic import BaseModel, ConfigDict, Field, computed_field
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import DuplicateKeyError
from pymongo.results import UpdateResult

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, StartupFailure, logger
from v4vapp_backend_v2.database.db_retry import mongo_call
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import (
    convert_decimals_for_mongodb,
    paywithsats_amount,
    snake_case,
)
from v4vapp_backend_v2.helpers.service_fees import calculate_fee_msats
from v4vapp_backend_v2.hive_models.account_name_type import AccName
from v4vapp_backend_v2.hive_models.magi_json_data import VSCCall, VSCCallPayload
from v4vapp_backend_v2.hive_models.op_all import trx_hive_fetch_unpack
from v4vapp_backend_v2.hive_models.op_base_extras import HiveExp
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson

ICON = "🧙‍♂️"
DB_MAGI_BTC_COLLECTION = "magi_btc"

# Hive transaction IDs are always exactly 40 lowercase hex characters.
_HIVE_TRX_ID_RE = re.compile(r"^[0-9a-f]{40}$")
# Underscore (current) and hyphen (early Magi persist) indexer-local ids.
_OLD_GID = re.compile(r"^(?P<indexer_id>\d+)[_-](?P<trx_id>.+)[_-]magi$")
_NEW_GID = re.compile(r"^(?P<trx_id>.+)_(?P<op_in_trx>\d+)_magi$")
_OLD_LEDGER_GID = re.compile(
    r"^(?P<indexer_id>\d+)[_-](?P<trx_id>.+)[_-]magi_"
    r"(?P<lt>magi_out|magi_in|fee_inc|magi_chg|funding)$"
)
_MAGI_LEDGER_TYPES = ("magi_out", "magi_in", "fee_inc", "magi_chg", "funding")
_DUP_PREFIX = "__dup_"
MAGI_IDENTITY_ERROR_CODE = "magi_identity_db_mismatch"
# Mongo regex: indexer-local magi_btc ids (underscore or hyphen).
_OLD_MAGI_BTC_GID_MONGO = r"^[0-9]+[_-].+[_-]magi$"
_OLD_MAGI_LEDGER_GID_MONGO = (
    r"^[0-9]+[_-].+_magi_(magi_out|magi_in|fee_inc|magi_chg|funding)$"
)
# Never $set these on an existing magi_btc _id (resume cursor + PR1 identity).
_KEEPER_NEVER_OVERWRITE = frozenset({"indexer_id", "group_id", "_id"})


class MagiIdentityInconsistency(StartupFailure):
    """magi_btc / MAGI_* ledger identity does not match this binary.

    db_monitor must not start change streams. StartupFailure is handled with
    process exit 0 so Docker ``restart: on-failure`` does not bounce the container.
    """


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


def magi_hash_aliases(indexer_tx_hash: str) -> list[str]:
    """Per-op aliases only. Never include stripped trx for op != 1.

    op 1 (no suffix or -0) → {txid, txid-0}
    op ≥ 2 (txid-N)        → {txid-N} only
    """
    if "-" not in indexer_tx_hash:
        return [indexer_tx_hash, f"{indexer_tx_hash}-0"]
    trx, suffix = indexer_tx_hash.rsplit("-", 1)
    try:
        op = int(suffix) + 1
    except ValueError:
        return [indexer_tx_hash]
    if op == 1:
        return [trx, f"{trx}-0"]
    return [f"{trx}-{op - 1}"]


def identity_key_from_hash(indexer_tx_hash: str) -> str:
    """Same trx_id / op_in_trx mapping as MagiBTCTransferEvent."""
    if "-" not in indexer_tx_hash:
        return f"{indexer_tx_hash}_1"
    trx, suffix = indexer_tx_hash.rsplit("-", 1)
    try:
        op = int(suffix) + 1
    except ValueError:
        return f"{trx}_1"
    return f"{trx}_{op}"


def identity_key_for(trx_id: str, op_in_trx: int) -> str:
    return f"{trx_id}_{op_in_trx}"


def new_group_id_from_hash(indexer_tx_hash: str) -> str:
    return f"{identity_key_from_hash(indexer_tx_hash)}_magi"


def is_old_magi_group_id(group_id: str | None) -> bool:
    if not group_id:
        return False
    gid = str(group_id)
    if gid.startswith(_DUP_PREFIX):
        return False
    return bool(_OLD_GID.match(gid))


async def assert_magi_identity_ready(*, sample_limit: int = 5, notify_delay: float = 2.0) -> None:
    """Fail closed if Mongo Magi identity does not match this db_monitor binary.

    This image looks up Magi by ``identity_key`` / hash and computes
    ``{trx}_{op}_magi``. Old-format ``{indexer_id}_{trx}_magi`` keepers (or
    missing ``identity_key``) make the process skip miss and re-book MAGI_OUTBOUND.
    """
    from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry

    magi_coll = MagiBTCTransferEvent.collection()
    not_dup = {"identity_key": {"$not": {"$regex": f"^{_DUP_PREFIX}"}}}
    old_btc_q = {"group_id": {"$regex": _OLD_MAGI_BTC_GID_MONGO}, **not_dup}
    missing_key_q = {
        "indexer_tx_hash": {"$exists": True, "$nin": [None, ""]},
        "$or": [
            {"identity_key": {"$exists": False}},
            {"identity_key": None},
            {"identity_key": ""},
        ],
    }
    old_led_q = {"group_id": {"$regex": _OLD_MAGI_LEDGER_GID_MONGO}}

    n_old_btc = await magi_coll.count_documents(old_btc_q)
    n_missing = await magi_coll.count_documents(missing_key_q)
    n_old_led = await LedgerEntry.collection().count_documents(old_led_q)
    if n_old_btc == 0 and n_missing == 0 and n_old_led == 0:
        logger.info(
            f"{ICON} Magi identity gate passed (indexer-agnostic magi_btc + MAGI_* ids)",
            extra={"notification": False, "error_code_clear": MAGI_IDENTITY_ERROR_CODE},
        )
        return

    old_btc_sample = await magi_coll.find(old_btc_q, {"group_id": 1, "indexer_id": 1}).to_list(
        length=sample_limit
    )
    missing_sample = await magi_coll.find(
        missing_key_q, {"group_id": 1, "indexer_tx_hash": 1}
    ).to_list(length=sample_limit)
    old_led_sample = await LedgerEntry.collection().find(
        old_led_q, {"group_id": 1, "ledger_type": 1}
    ).to_list(length=sample_limit)

    msg = (
        f"{ICON} Magi identity mismatch: this db_monitor expects "
        f"{{trx}}_{{op}}_magi + identity_key. "
        f"old_format_magi_btc={n_old_btc} missing_identity_key={n_missing} "
        f"old_format_MAGI_ledger={n_old_led}. "
        "Refusing to start change streams (would re-book MAGI_OUTBOUND). "
        "Run scripts/rewrite_magi_group_id.py --mode apply --commit, "
        "then start only this image."
    )
    extra = {
        "notification": True,
        "error_code": MAGI_IDENTITY_ERROR_CODE,
        "old_format_magi_btc": n_old_btc,
        "missing_identity_key": n_missing,
        "old_format_magi_ledger": n_old_led,
        "old_format_magi_btc_sample": [d.get("group_id") for d in old_btc_sample],
        "missing_identity_key_sample": [d.get("group_id") for d in missing_sample],
        "old_format_magi_ledger_sample": [d.get("group_id") for d in old_led_sample],
    }
    logger.error(msg, extra=extra)
    if notify_delay:
        await asyncio.sleep(notify_delay)
    raise MagiIdentityInconsistency(msg)


def keeper_identity_sets(keeper: dict, *, migrate_group_id: bool = True) -> dict[str, Any]:
    """identity_key / legacy_group_id, and PR2 group_id migrate-on-write."""
    sets: dict[str, Any] = {}
    hash_val = keeper.get("indexer_tx_hash") or ""
    if not keeper.get("identity_key") and hash_val:
        sets["identity_key"] = identity_key_from_hash(hash_val)
    stored_gid = keeper.get("group_id")
    if is_old_magi_group_id(stored_gid):
        if not keeper.get("legacy_group_id"):
            sets["legacy_group_id"] = stored_gid
        if migrate_group_id and hash_val:
            new_gid = new_group_id_from_hash(hash_val)
            if stored_gid != new_gid:
                sets["group_id"] = new_gid
    return sets


def _not_dup(doc: dict) -> bool:
    return not str(doc.get("identity_key") or "").startswith(_DUP_PREFIX)


async def find_magi_docs(
    *,
    indexer_tx_hash: str | None = None,
    group_id: str | None = None,
    identity_key: str | None = None,
) -> list[dict]:
    """Candidates for ONE (trx_id, op_in_trx).

    Instance/save/process: pass indexer_tx_hash (and identity_key). Incoming
    group_id_p is ignored here — AND-ing it would miss stored 802_… docs.

    String parent_id load: pass group_id only. Exact {group_id|legacy_group_id}.
    Old-format strings are NOT expanded into hash aliases (no op_in_trx).
    New-format strings may also query identity_key = {trx}_{op}.
    """
    coll = MagiBTCTransferEvent.collection()

    if indexer_tx_hash:
        aliases = magi_hash_aliases(indexer_tx_hash)
        key = identity_key or identity_key_from_hash(indexer_tx_hash)
        query = {
            "$or": [
                {"identity_key": key},
                {
                    "identity_key": {"$exists": False},
                    "indexer_tx_hash": {"$in": aliases},
                },
            ]
        }
        docs = await coll.find(query).to_list(length=20)
        return [d for d in docs if _not_dup(d)]

    if group_id:
        or_terms: list[dict] = [{"group_id": group_id}, {"legacy_group_id": group_id}]
        m_new = _NEW_GID.match(group_id)
        if m_new:
            or_terms.append({
                "identity_key": identity_key_for(m_new["trx_id"], int(m_new["op_in_trx"]))
            })
        docs = await coll.find({"$or": or_terms}).to_list(length=20)
        return [d for d in docs if _not_dup(d)]

    if identity_key:
        docs = await coll.find({"identity_key": identity_key}).to_list(length=20)
        return [d for d in docs if _not_dup(d)]
    return []


def underscore_old_group_id(group_id: str | None) -> str | None:
    """Normalize hyphen old ids (242-trx-magi) to 242_trx_magi for ledger join."""
    if not group_id:
        return None
    m = _OLD_GID.match(str(group_id))
    if not m:
        return str(group_id)
    return f"{m['indexer_id']}_{m['trx_id']}_magi"


async def magi_ledgers_for(doc: dict) -> list[dict]:
    """MAGI_* rows for THIS keeper only. No trx-wide regex."""
    from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry

    bases = {b for b in (doc.get("group_id"), doc.get("legacy_group_id")) if b}
    bases |= {underscore_old_group_id(b) for b in list(bases) if b}
    bases.discard(None)
    if not bases:
        return []
    ids = [f"{b}_{lt}" for b in bases for lt in _MAGI_LEDGER_TYPES]
    return await LedgerEntry.collection().find({"group_id": {"$in": ids}}).to_list(length=20)


async def select_keeper(docs: list[dict]) -> dict | None:
    """Prefer process_time, then MAGI_* on stored ids, then higher stored indexer_id."""
    if not docs:
        return None
    if len(docs) == 1:
        return docs[0]
    ledgers_by_id = {str(d["_id"]): await magi_ledgers_for(d) for d in docs}

    def score(d: dict) -> tuple:
        has_pt = 1 if d.get("process_time") is not None else 0
        has_led = 1 if ledgers_by_id.get(str(d["_id"])) else 0
        return (has_pt, has_led, int(d.get("indexer_id") or 0))

    return max(docs, key=score)


async def persist_watched_magi_event(event: "MagiBTCTransferEvent") -> str:
    """Persist a watched stream event, or no-op if the keeper is already booked.

    Returns one of: keeper_hit_noop, keeper_hit_backfill, insert.
    """
    docs = await find_magi_docs(
        indexer_tx_hash=event.indexer_tx_hash,
        identity_key=event.identity_key_p,
    )
    keeper = await select_keeper(docs)
    if keeper and (
        keeper.get("process_time") is not None or await magi_ledgers_for(keeper)
    ):
        path = await MagiBTCTransferEvent.backfill_identity_fields(keeper)
        logger.debug(
            f"{ICON} magi save path={path} identity_key={event.identity_key_p}",
            extra={"notification": False},
        )
        return path
    await event.fill_custom_jsons()
    logger.debug(
        f"{ICON} magi save path=insert identity_key={event.identity_key_p}",
        extra={"notification": False},
    )
    return "insert"


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
    legacy_group_id: str | None = Field(
        None,
        description="First stored group_id, copied from DB, never from the current indexer_id",
    )
    identity_key: str | None = Field(
        None,
        description="Persisted {trx_id}_{op_in_trx} uniqueness key",
    )

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
    def paywithsats(self) -> bool:
        """
        This mirrors the flag in the CustomJson for the follow-on transfer,
        but is derived here for easier access when processing the initial transfer event.

        This is used by mark: We have a pay_req, we will pay it in `process_transfer.py`

        Returns:
            bool: True if the transfer should be marked as "pay with sats", False otherwise.
        """
        if self.amount > 0 and not self.do_not_pay:
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

        if self.paywithsats_amount:
            max_to_send_base_msats = self.paywithsats_amount * Decimal(1000)
            delta_msats = (self.amount * Decimal(1000)) - max_to_send_base_msats
        else:
            max_to_send_base_msats = self.amount * Decimal(1000)
            delta_msats = Decimal(0)

        if delta_msats < Decimal(0):
            raise ValueError("Max to send cannot exceed the total amount of the transfer")

        msats_fee = calculate_fee_msats(max_to_send_base_msats)
        max_to_send_msats = max_to_send_base_msats - msats_fee
        max_to_send_sats = max_to_send_msats / Decimal(1000)

        lnd_config = InternalConfig().config.lnd_config

        forwarding_fee_estimate_msats = Decimal(
            Decimal(lnd_config.lightning_fee_base_msats)
            + (
                (max_to_send_sats * Decimal(2000))
                * Decimal(lnd_config.lightning_fee_estimate_ppm)
                / 1_000_000
            )
        ).quantize(Decimal("1."), rounding="ROUND_UP")

        logger.info(
            f"{ICON} max_to_send_base_msats={max_to_send_base_msats:,.0f}, msats_fee={msats_fee:,.0f}, forwarding_fee_estimate_msats={forwarding_fee_estimate_msats:,.0f}, delta_msats={delta_msats:,.0f}"
        )

        if (forwarding_fee_estimate_msats + msats_fee) < delta_msats:
            return max_to_send_base_msats
        else:
            return max_to_send_msats - forwarding_fee_estimate_msats

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

    async def fill_custom_jsons(self) -> None:
        """
        Fetches and fills the custom_jsons field with associated CustomJson operations from the Hive transaction.
        Also updates the conversion and saves the record after populating the custom_jsons.

        This method should be called after initializing the MagiBTCTransferEvent to populate the custom_jsons field
        with the relevant CustomJson operations for further processing.

        Returns:
            None
        """
        custom_jsons = await self.hive_custom_json()
        for op in custom_jsons or []:
            if op.is_watched:
                await op.save()
        self.custom_jsons = custom_jsons
        await self.update_conv()
        await self.save()

    def _mongo_dump(
        self,
        exclude_unset: bool = False,
        exclude_none: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        dump = self.model_dump(
            exclude_unset=exclude_unset,
            exclude_none=exclude_none,
            by_alias=self.dump_by_alias,
            **kwargs,
        )
        if dump.get("replies") == []:
            dump.pop("replies", None)
        return convert_decimals_for_mongodb(dump)

    @classmethod
    async def backfill_identity_fields(cls, keeper: dict) -> str:
        """$set identity_key / legacy_group_id if missing; PR2 migrates old group_id.

        Never writes indexer_id. group_id migrate is on IGNORED_UPDATE_FIELDS.
        """
        sets = keeper_identity_sets(keeper, migrate_group_id=True)
        if not sets:
            logger.debug(
                f"{ICON} magi save path=keeper_hit_noop _id={keeper.get('_id')}",
                extra={"notification": False},
            )
            return "keeper_hit_noop"
        await mongo_call(
            lambda: cls.collection().update_one({"_id": keeper["_id"]}, {"$set": sets}),
            error_code="db_save_error_magi_btc",
            context=f"magi_btc:backfill:{keeper.get('_id')}",
        )
        logger.debug(
            f"{ICON} magi save path=keeper_hit_backfill _id={keeper.get('_id')}",
            extra={"notification": False},
        )
        return "keeper_hit_backfill"

    async def _save_keeper_update(
        self,
        keeper: dict,
        exclude_unset: bool,
        exclude_none: bool,
        mongo_kwargs: dict[str, Any],
        **kwargs: Any,
    ) -> UpdateResult:
        processed = keeper.get("process_time") is not None or await magi_ledgers_for(keeper)
        sets = keeper_identity_sets(keeper, migrate_group_id=True)
        if self.process_time is not None and keeper.get("process_time") is None:
            sets["process_time"] = self.process_time

        if not processed:
            dump = self._mongo_dump(exclude_unset, exclude_none, **kwargs)
            for protected in _KEEPER_NEVER_OVERWRITE:
                dump.pop(protected, None)
            dump.pop("legacy_group_id", None)
            dump.pop("identity_key", None)
            dump.pop("process_time", None)
            stored_hash = keeper.get("indexer_tx_hash")
            if stored_hash:
                dump.pop("indexer_tx_hash", None)
            sets.update(dump)

        if not sets:
            logger.debug(
                f"{ICON} magi save path=keeper_hit_noop _id={keeper.get('_id')}",
                extra={"notification": False},
            )
            return await mongo_call(
                lambda: InternalConfig.db[self.collection_name].update_one(
                    {"_id": keeper["_id"]},
                    {"$set": {"identity_key": keeper.get("identity_key") or self.identity_key_p}},
                    **{k: v for k, v in mongo_kwargs.items() if k != "upsert"},
                ),
                error_code=f"db_save_error_{self.collection_name}",
                context=f"{self.collection_name}:{self.identity_key_p}",
            )

        path = "keeper_hit_backfill" if processed else "keeper_update"
        logger.debug(
            f"{ICON} magi save path={path} _id={keeper.get('_id')}",
            extra={"notification": False},
        )
        return await mongo_call(
            lambda: InternalConfig.db[self.collection_name].update_one(
                {"_id": keeper["_id"]},
                {"$set": sets},
                **{k: v for k, v in mongo_kwargs.items() if k != "upsert"},
            ),
            error_code=f"db_save_error_{self.collection_name}",
            context=f"{self.collection_name}:{self.identity_key_p}",
        )

    async def _save_insert(
        self,
        exclude_unset: bool,
        exclude_none: bool,
        mongo_kwargs: dict[str, Any],
        **kwargs: Any,
    ) -> UpdateResult:
        dump = self._mongo_dump(exclude_unset, exclude_none, **kwargs)
        dump["identity_key"] = self.identity_key_p
        dump.pop("legacy_group_id", None)
        logger.debug(
            f"{ICON} magi save path=insert identity_key={self.identity_key_p}",
            extra={"notification": False},
        )
        kwargs_insert = dict(mongo_kwargs)
        kwargs_insert["upsert"] = True
        return await mongo_call(
            lambda: InternalConfig.db[self.collection_name].update_one(
                {"identity_key": self.identity_key_p},
                {"$setOnInsert": dump},
                **kwargs_insert,
            ),
            error_code=f"db_save_error_{self.collection_name}",
            context=f"{self.collection_name}:{self.identity_key_p}",
        )

    async def save(
        self,
        exclude_unset: bool = False,
        exclude_none: bool = True,
        mongo_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> UpdateResult:
        """Field-policy save: never dump indexer_id or group_id onto an existing keeper.

        mongomock will not prove TOCTOU double-insert; unique identity_key after
        identity-backfill plus DuplicateKeyError retry is the production mitigation.
        """
        if mongo_kwargs is None:
            mongo_kwargs = {"upsert": True}

        docs = await find_magi_docs(
            indexer_tx_hash=self.indexer_tx_hash,
            identity_key=self.identity_key_p,
        )
        keeper = await select_keeper(docs)
        if keeper:
            return await self._save_keeper_update(
                keeper, exclude_unset, exclude_none, mongo_kwargs, **kwargs
            )
        try:
            return await self._save_insert(exclude_unset, exclude_none, mongo_kwargs, **kwargs)
        except DuplicateKeyError:
            logger.debug(
                f"{ICON} magi save path=dup_key_retry identity_key={self.identity_key_p}",
                extra={"notification": False},
            )
            docs = await find_magi_docs(
                indexer_tx_hash=self.indexer_tx_hash,
                identity_key=self.identity_key_p,
            )
            if not docs:
                aliases = magi_hash_aliases(self.indexer_tx_hash)
                raw = await MagiBTCTransferEvent.collection().find(
                    {"indexer_tx_hash": {"$in": aliases}}
                ).to_list(length=20)
                docs = [d for d in raw if _not_dup(d)]
            keeper = await select_keeper(docs)
            if keeper:
                return await self._save_keeper_update(
                    keeper, exclude_unset, exclude_none, mongo_kwargs, **kwargs
                )
            raise

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
        ops = trx_hive_fetch_unpack(self.trx_id)
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
        """Indexer-agnostic Magi identity: {trx_id}_{op_in_trx}_magi.

        Not {indexer_id}_{trx_id}_magi — indexer_id is a local Hasura cursor, not identity.
        `_m` and `_magi` identify Magi-related operations in load_tracked_object.
        """
        return f"{self.trx_id}_{self.op_in_trx}_magi"

    @computed_field
    def short_id(self) -> str:
        """
        Returns a short ID for this record. This is a string used to uniquely identify
        the operation in the database.
        The short ID is a combination of the block number, transaction number,
        operation index in the transaction, and realm.
        This is used to determine the key in the database where the operation
        """
        # make this the last 8 chars of the trx_id plus _m for magi, to ensure it's unique but not too long to be unwieldy in logs and such.
        return f"{self.trx_id[-8:]}_m"

    @property
    def short_id_p(self) -> str:
        return self.short_id  # type: ignore

    @property
    def group_id_p(self) -> str:
        return self.group_id  # type: ignore

    @property
    def identity_key_p(self) -> str:
        return f"{self.trx_id}_{self.op_in_trx}"

    @property
    def group_id_query(self) -> Dict[str, Any]:
        # Not used for Magi load/save after PR1. Kept for TrackedBaseModel compatibility.
        return {"identity_key": self.identity_key_p}

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
