"""
Ledger balance checkpoints stored in MongoDB.

A checkpoint captures the net balance state for a specific account at the end of
a calendar period (daily, weekly, or monthly).  Subsequent balance queries can
start from a known state and only process the incremental transactions, reducing
aggregation pipeline costs for long-running accounts.

Collection: ``ledger_checkpoints``

Each document covers one account × one period:
    {
        "account_name": "VSC Liability",
        "account_sub": "alice",
        "account_type": "Liability",
        "contra": false,
        "period_type": "daily",
        "period_end": ISODate("2024-01-31T23:59:59.999999Z"),
        "balances_net": {"HIVE": "1.234", "MSATS": "100000"},
        "conv_totals": {
            "HIVE": {"hive": "1.234", "hbd": "0.5", "usd": "0.3", "sats": "100", "msats": "100000"},
            ...
        },
        "last_transaction_date": ISODate("..."),
        "created_at": ISODate("..."),
    }
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from timeit import default_timer as timer
from typing import Any, Dict, List, Optional, Tuple

from bson import Decimal128
from pydantic import BaseModel, Field
from pymongo import ASCENDING, DESCENDING, IndexModel
from pymongo.asynchronous.collection import AsyncCollection

from v4vapp_backend_v2.accounting.account_balances import (
    list_all_active_accounts,
    one_account_balance,
)
from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.period_end_type import (
    PeriodType,
    completed_period_ends_since,
    last_completed_period_end,
)

ICON = "📌"


class CheckpointConvSummary(BaseModel):
    """Pydantic-serialisable mirror of ``ConvertedSummary`` for MongoDB storage."""

    hive: Decimal = Decimal(0)
    hbd: Decimal = Decimal(0)
    usd: Decimal = Decimal(0)
    sats: Decimal = Decimal(0)
    msats: Decimal = Decimal(0)

    def to_converted_summary(self):
        """Return a ``ConvertedSummary`` dataclass instance."""
        from v4vapp_backend_v2.accounting.converted_summary_class import ConvertedSummary

        return ConvertedSummary(
            hive=self.hive,
            hbd=self.hbd,
            usd=self.usd,
            sats=self.sats,
            msats=self.msats,
        )

    @classmethod
    def from_converted_summary(cls, cs) -> "CheckpointConvSummary":
        """Build from a ``ConvertedSummary`` dataclass."""
        return cls(
            hive=cs.hive,
            hbd=cs.hbd,
            usd=cs.usd,
            sats=cs.sats,
            msats=cs.msats,
        )


class LedgerCheckpoint(BaseModel):
    """A pre-calculated balance snapshot for one account at the end of a period."""

    account_name: str = Field(..., description="Name of the ledger account")
    account_sub: str = Field("", description="Sub-account identifier")
    account_type: str = Field(..., description="AccountType enum value")
    contra: bool = Field(False, description="Whether this is a contra account")
    period_type: PeriodType = Field(..., description="Granularity of the period")
    period_end: datetime = Field(
        ...,
        description=(
            "Inclusive upper bound of the period: all transactions with "
            "timestamp ≤ period_end are captured in this checkpoint."
        ),
    )
    # Net running balance at period_end per currency (e.g. {"HIVE": Decimal("1.234")})
    balances_net: Dict[str, Decimal] = Field(
        default_factory=dict,
        description="Net amount per currency at period_end",
    )
    # Conversion running-total at period_end per currency
    conv_totals: Dict[str, CheckpointConvSummary] = Field(
        default_factory=dict,
        description="Conversion running-total per currency at period_end",
    )
    last_transaction_date: Optional[datetime] = Field(
        None, description="Timestamp of last included transaction"
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="When this checkpoint was written",
    )

    # ------------------------------------------------------------------
    # Database helpers
    # ------------------------------------------------------------------

    @classmethod
    def collection_name(cls) -> str:
        return "ledger_checkpoints"

    @classmethod
    def collection(cls) -> AsyncCollection:
        return InternalConfig.db[cls.collection_name()]

    @classmethod
    async def ensure_indexes(cls) -> None:
        """Create the compound unique index if it does not exist."""
        index = IndexModel(
            [
                ("account_name", ASCENDING),
                ("account_sub", ASCENDING),
                ("account_type", ASCENDING),
                ("period_type", ASCENDING),
                ("period_end", DESCENDING),
            ],
            unique=True,
            name="checkpoint_unique",
        )
        await cls.collection().create_indexes([index])

    def _to_mongo_doc(self) -> dict:
        """Serialise to a MongoDB-compatible dict (Decimal → Decimal128)."""
        raw: dict = {
            "account_name": self.account_name,
            "account_sub": self.account_sub,
            "account_type": self.account_type,
            "contra": self.contra,
            "period_type": str(self.period_type),
            "period_end": self.period_end,
            "last_transaction_date": self.last_transaction_date,
            "created_at": self.created_at,
            "balances_net": {k: Decimal128(str(v)) for k, v in self.balances_net.items()},
            "conv_totals": {
                currency: {
                    field: Decimal128(str(getattr(cs, field)))
                    for field in ("hive", "hbd", "usd", "sats", "msats")
                }
                for currency, cs in self.conv_totals.items()
            },
        }
        return raw

    @classmethod
    def _from_mongo_doc(cls, doc: dict) -> "LedgerCheckpoint":
        """Deserialise from a raw MongoDB document."""
        from bson import Decimal128 as _D128

        def _dec(v: Any) -> Decimal:
            if isinstance(v, _D128):
                return v.to_decimal()
            return Decimal(str(v))

        balances_net = {k: _dec(v) for k, v in doc.get("balances_net", {}).items()}
        conv_totals: Dict[str, CheckpointConvSummary] = {}
        for currency, cs_raw in doc.get("conv_totals", {}).items():
            conv_totals[currency] = CheckpointConvSummary(
                hive=_dec(cs_raw.get("hive", 0)),
                hbd=_dec(cs_raw.get("hbd", 0)),
                usd=_dec(cs_raw.get("usd", 0)),
                sats=_dec(cs_raw.get("sats", 0)),
                msats=_dec(cs_raw.get("msats", 0)),
            )
        return cls(
            account_name=doc["account_name"],
            account_sub=doc.get("account_sub", ""),
            account_type=doc["account_type"],
            contra=doc.get("contra", False),
            period_type=PeriodType(doc["period_type"]),
            period_end=doc["period_end"],
            balances_net=balances_net,
            conv_totals=conv_totals,
            last_transaction_date=doc.get("last_transaction_date"),
            created_at=doc.get("created_at", datetime.now(tz=timezone.utc)),
        )

    async def save(self) -> None:
        """Upsert this checkpoint document into MongoDB."""
        doc = self._to_mongo_doc()
        try:
            await self.collection().update_one(
                filter={
                    "account_name": self.account_name,
                    "account_sub": self.account_sub,
                    "account_type": self.account_type,
                    "period_type": str(self.period_type),
                    "period_end": self.period_end,
                },
                update={"$set": doc},
                upsert=True,
            )
            logger.debug(
                f"📌 Checkpoint saved: {self.account_name}:{self.account_sub} "
                f"{self.period_type} {self.period_end.date()}",
                extra={"notification": False},
            )
        except Exception as e:
            logger.error(
                f"Failed to save checkpoint: {e}",
                extra={"notification": False},
            )
            raise


async def delete_all_ledger_checkpoints(
    account_name: str, account_sub: str, account_type: str, period_type: PeriodType
) -> None:
    """
    Delete all the checkpoint documents for a given account and period type from MongoDB.
    This is necessary after reversing a transaction or making a backdated correction (such as for the
    Customer Deposits Hive (Asset) account for the server when reversing Limit Order Create transactions).

    Args:
        - *account_name*: Name of the ledger account (e.g. "VSC Liability")
        - *account_sub*: Sub-account identifier (e.g. "alice")
        - *account_type*: AccountType enum value as a string (e.g. "Liability")
        - *period_type*: Granularity of the checkpoints to delete (daily/weekly/monthly)


    """
    try:
        delete_result = await LedgerCheckpoint.collection().delete_many(
            filter={
                "account_name": account_name,
                "account_sub": account_sub,
                "account_type": account_type,
                "period_type": str(period_type),
            }
        )
        logger.debug(
            f"📌 Checkpoint deleted: {account_name}:{account_sub} {period_type}",
            extra={"notification": False, "delete_result": delete_result.raw_result},
        )
    except Exception as e:
        logger.error(
            f"Failed to delete checkpoint: {e}",
            extra={"notification": False},
        )
        raise


# ---------------------------------------------------------------------------
# CRUD helpers
# ---------------------------------------------------------------------------


async def invalidate_checkpoints_for_accounts_by_date(
    accounts: List[LedgerAccount], timestamp: datetime
) -> None:
    """
    Invalidate the cache for given accounts and timestamp.

    This is necessary after reversing a transaction or making a backdated correction (such as for the
    Customer Deposits Hive (Asset) account for the server when reversing Limit Order Create transactions).

    Args:
        - *accounts*: List of LedgerAccount instances identifying the accounts
        - *timestamp*: The timestamp to compare against the last completed period end
    """
    tasks = []
    # convert timestamp to UTC if it is naive
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    for period_type in PeriodType:
        if timestamp < last_completed_period_end(period_type):
            tasks.append(delete_checkpoints_for_accounts_and_period_type(accounts, period_type))
    await asyncio.gather(*tasks)


async def delete_checkpoints_for_accounts_and_period_type(
    accounts: List[LedgerAccount], period_type: PeriodType
) -> None:
    """
    Delete all checkpoints for given accounts and period type.

    This is necessary after reversing a transaction or making a backdated correction (such as for the
    Customer Deposits Hive (Asset) account for the server when reversing Limit Order Create transactions).

    Args:
        - *accounts*: List of LedgerAccount instances identifying the accounts
        - *period_type*: Granularity of the checkpoints to delete (daily/weekly/monthly)
    """
    tasks = []
    for account in accounts:
        tasks.append(
            delete_all_ledger_checkpoints(
                account_name=account.name,
                account_sub=account.sub,
                account_type=str(account.account_type),
                period_type=period_type,
            )
        )
    await asyncio.gather(*tasks)


async def get_latest_checkpoint_before(
    account: LedgerAccount,
    as_of_date: datetime,
    period_type: PeriodType | None = None,
) -> LedgerCheckpoint | None:
    """Return the most-recent checkpoint whose ``period_end ≤ as_of_date``.

    If *period_type* is supplied only checkpoints of that granularity are
    considered; otherwise all granularities are searched and the one with the
    latest ``period_end`` wins.

    Returns ``None`` when no matching checkpoint exists.
    """
    query: dict[str, Any] = {
        "account_name": account.name,
        "account_sub": account.sub,
        "account_type": str(account.account_type),
        "period_end": {"$lte": as_of_date},
    }
    if period_type is not None:
        query["period_type"] = str(period_type)

    doc = await LedgerCheckpoint.collection().find_one(
        filter=query,
        sort=[("period_end", DESCENDING)],
    )
    if doc is None:
        return None
    try:
        return LedgerCheckpoint._from_mongo_doc(doc)
    except Exception as e:
        logger.warning(
            f"⚠️  Failed to deserialise checkpoint document: {e}",
            extra={"notification": False},
        )
        return None


async def get_checkpoint_by_id(
    account: LedgerAccount,
    period_type: PeriodType,
    period_end: datetime,
) -> LedgerCheckpoint | None:
    """
    Return the checkpoint matching the unique combination of identifying fields.

    Arguments:
    - *account*: identifies the account by name, sub, and type
    - *period_type*: granularity of the checkpoint (daily/weekly/monthly)
    - *period_end*: inclusive end of the period covered by the checkpoint

    Returns ``None`` when no matching checkpoint exists.
    """
    query = {
        "account_name": account.name,
        "account_sub": account.sub,
        "account_type": str(account.account_type),
        "period_type": str(period_type),
        "period_end": period_end,
    }
    doc = await LedgerCheckpoint.collection().find_one(filter=query)
    if doc is None:
        return None
    try:
        return LedgerCheckpoint._from_mongo_doc(doc)
    except Exception as e:
        logger.warning(
            f"⚠️  Failed to deserialise checkpoint document: {e}",
            extra={"notification": False},
        )
        return None


async def latest_period_create_checkpoint(
    account: LedgerAccount, period_type: PeriodType = PeriodType.DAILY
) -> Tuple[LedgerCheckpoint, bool, timedelta, datetime]:
    """
    Create a checkpoint for the most recently completed period if it does not already exist.

    This is a convenience wrapper around :func:`create_checkpoint` that automatically determines the appropriate period_end for the latest completed period of the specified type, and creates a checkpoint for
    that period if one does not already exist.  If a checkpoint for that period already exists, it is returned without modification.

    Arguments:
    - *account*: identifies the account by name, sub, and type
    - *period_type*: granularity of the checkpoint (daily/weekly/monthly)

    Returns:
    - The checkpoint instance (newly created or existing)
    - A boolean flag indicating whether a new checkpoint was created (True) or an existing one was returned (False)

    """
    period_start: datetime | None = None
    now = datetime.now(tz=timezone.utc)
    period_start = last_completed_period_end(period_type, now)
    age = now - period_start
    checkpoint, created = await create_checkpoint(account, period_type, period_start)
    return checkpoint, created, age, period_start


async def create_checkpoint(
    account: LedgerAccount,
    period_type: PeriodType,
    period_end: datetime,
    use_cache: bool = True,
    force: bool = False,
) -> Tuple[LedgerCheckpoint, bool]:
    """
    Compute and persist a checkpoint for *account* at *period_end*.

    If *force* is False (default), the checkpoint is only created if one does
        not already exist for the same account/period.  When a checkpoint already
        exists, it is returned along with a boolean flag indicating that it was
        not newly created.

    if *use_cache* is True (default), the balance computation will consult the cache
        for the full balance, if False it will force a full aggregation from the ledger entries.
        This is the redis temp cache.

    The balance is computed by calling ``one_account_balance`` with
    ``as_of_date=period_end`` and cache/checkpoint lookup disabled so that
    the result is authoritative.

    Returns:
    - The checkpoint instance (newly created or existing)
    - A boolean flag indicating whether a new checkpoint was created (True) or an existing one was returned (False)

    """
    start = timer()
    if not force:
        existing_checkpoint = await get_checkpoint_by_id(account, period_type, period_end)
        if existing_checkpoint is not None:
            logger.info(
                f"{ICON} Checkpoint already exists for {account} at {period_end} ({period_type}); skipping. (took {timer() - start:.2f}s)",
                extra={"notification": False},
            )
            return existing_checkpoint, False

    ledger_details = await one_account_balance(
        account=account,
        as_of_date=period_end,
        use_cache=use_cache,
        use_checkpoints=False,
    )

    balances_net: Dict[str, Decimal] = {}
    conv_totals: Dict[str, CheckpointConvSummary] = {}

    for currency, net_value in ledger_details.balances_net.items():
        key = str(currency)
        balances_net[key] = net_value

    for currency, cs in ledger_details.balances_totals.items():
        key = str(currency)
        conv_totals[key] = CheckpointConvSummary.from_converted_summary(cs)

    checkpoint = LedgerCheckpoint(
        account_name=account.name,
        account_sub=account.sub,
        account_type=str(account.account_type),
        contra=account.contra,
        period_type=period_type,
        period_end=period_end,
        balances_net=balances_net,
        conv_totals=conv_totals,
        last_transaction_date=ledger_details.last_transaction_date,
    )
    await checkpoint.save()
    logger.info(
        f"{ICON} Created checkpoint for {account} at {period_end} ({period_type}) (took {timer() - start:.2f}s)"
    )
    return checkpoint, True


async def build_checkpoints_for_period(
    period_type: PeriodType,
    since: datetime | None = None,
    until: datetime | None = None,
) -> int:
    """Create checkpoints for all known accounts for every completed period.

    Scans the ledger collection to discover all distinct accounts, then
    creates checkpoints for each account for each completed period between
    *since* and *until*.

    Args:
        period_type: Granularity of the checkpoints to build.
        since: Lower bound for period discovery.  Defaults to the earliest
            ledger entry's timestamp.
        until: Upper bound (exclusive).  Defaults to *now*.

    Returns:
        The total number of checkpoints written.
    """
    start = timer()
    now = datetime.now(tz=timezone.utc)
    if until is None:
        until = now

    if since is None:
        # Find the earliest ledger entry timestamp
        first_doc = await LedgerEntry.collection().find_one(
            filter={}, sort=[("timestamp", ASCENDING)]
        )
        if first_doc is None:
            logger.info(
                f"{ICON} No ledger entries found; skipping checkpoint build. (took {timer() - start:.2f}s)"
            )
            return 0
        since = first_doc["timestamp"]
        if since and since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)

    period_ends = completed_period_ends_since(period_type, since, until)
    if not period_ends:
        logger.info(
            f"{ICON} No completed {period_type} periods to checkpoint. (took {timer() - start:.2f}s)"
        )
        return 0

    accounts = await list_all_active_accounts()
    total = 0

    async def _run_one(account: LedgerAccount, period_end: datetime, use_cache: bool) -> bool:
        try:
            _, new_checkpoint = await create_checkpoint(
                account, period_type, period_end, use_cache=use_cache, force=False
            )
            return bool(new_checkpoint)
        except Exception as e:
            logger.warning(
                f"⚠️  Could not create {period_type} checkpoint for "
                f"{account.name}:{account.sub} at {period_end}: {e}",
                extra={"notification": False},
            )
            return False

    for account in accounts:
        async with asyncio.TaskGroup() as tg:
            tasks = [tg.create_task(_run_one(account, pe, use_cache=True)) for pe in period_ends]

        total += sum(t.result() for t in tasks)

    logger.info(
        f"{ICON} Built {total} {period_type} checkpoints "
        f"({len(accounts)} accounts x {len(period_ends)} periods). (took {timer() - start:.2f}s)",
        extra={"notification": False},
    )
    return total
