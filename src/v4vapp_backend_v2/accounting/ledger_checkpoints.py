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

import calendar
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Any, Dict, List, Optional, Tuple

from bson import Decimal128
from pydantic import BaseModel, Field
from pymongo import ASCENDING, DESCENDING, IndexModel
from pymongo.asynchronous.collection import AsyncCollection

from v4vapp_backend_v2.accounting.account_balances import one_account_balance
from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount
from v4vapp_backend_v2.config.decorators import async_time_decorator
from v4vapp_backend_v2.config.setup import InternalConfig, logger

ICON = "📌"


class PeriodType(StrEnum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


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


# ---------------------------------------------------------------------------
# Period boundary helpers
# ---------------------------------------------------------------------------


def period_end_for_date(period_type: PeriodType, d: date) -> datetime:
    """Return the last microsecond of the calendar period that contains *d*.

    The returned datetime is timezone-aware (UTC) and represents the inclusive
    upper bound: all transactions with ``timestamp ≤ period_end`` belong to
    this period's checkpoint.
    """
    if period_type == PeriodType.DAILY:
        return datetime(d.year, d.month, d.day, 23, 59, 59, 999999, tzinfo=timezone.utc)

    if period_type == PeriodType.WEEKLY:
        # ISO week: Monday=0 … Sunday=6; advance to Sunday
        days_to_sunday = 6 - d.weekday()
        sunday = d + timedelta(days=days_to_sunday)
        return datetime(
            sunday.year, sunday.month, sunday.day, 23, 59, 59, 999999, tzinfo=timezone.utc
        )

    if period_type == PeriodType.MONTHLY:
        last_day = calendar.monthrange(d.year, d.month)[1]
        return datetime(d.year, d.month, last_day, 23, 59, 59, 999999, tzinfo=timezone.utc)

    raise ValueError(f"Unknown period type: {period_type}")


def last_completed_period_end(period_type: PeriodType, now: datetime | None = None) -> datetime:
    """Return the end datetime of the last fully completed period before *now*.

    Unlike :func:`period_end_for_date`, which returns the end of the period
    *containing* a given date (which may be in the future), this function
    always returns a period boundary that has already passed:

    - **daily**  → end of yesterday
    - **weekly** → end of the most-recently completed ISO week (last Sunday)
    - **monthly** → end of the previous calendar month
    """
    if now is None:
        now = datetime.now(tz=timezone.utc)
    today = now.date()

    if period_type == PeriodType.DAILY:
        d = today - timedelta(days=1)
    elif period_type == PeriodType.WEEKLY:
        # weekday(): Mon=0 … Sun=6; go back enough days to land on the last Sunday
        days_since_last_sunday = today.weekday() + 1  # Mon→1, Tue→2, …, Sun→7
        d = today - timedelta(days=days_since_last_sunday)
    elif period_type == PeriodType.MONTHLY:
        # First day of this month minus one day = last day of previous month
        d = date(today.year, today.month, 1) - timedelta(days=1)
    else:
        raise ValueError(f"Unknown period type: {period_type}")

    return period_end_for_date(period_type, d)


def completed_period_ends_since(
    period_type: PeriodType,
    since: datetime,
    until: datetime,
) -> List[datetime]:
    """Return a list of completed period-end timestamps in chronological order.

    Only periods whose ``period_end`` is strictly less than *until* are
    included, so the caller's "current" period is never returned as complete.

    Args:
        period_type: Granularity of the periods to enumerate.
        since: Start of the range (inclusive).
        until: End of the range (exclusive).  Typically *now*.
    """
    results: List[datetime] = []
    current = since.date()
    while True:
        end = period_end_for_date(period_type, current)
        if end >= until:
            break
        if end > since:
            results.append(end)

        # Advance to the next period start
        if period_type == PeriodType.DAILY:
            current = current + timedelta(days=1)
        elif period_type == PeriodType.WEEKLY:
            days_to_next_monday = 7 - current.weekday()
            current = current + timedelta(days=days_to_next_monday)
        elif period_type == PeriodType.MONTHLY:
            last_day = calendar.monthrange(current.year, current.month)[1]
            first_next = date(current.year, current.month, last_day) + timedelta(days=1)
            current = first_next

    return results


# ---------------------------------------------------------------------------
# CRUD helpers
# ---------------------------------------------------------------------------


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


@async_time_decorator
async def create_checkpoint(
    account: LedgerAccount,
    period_type: PeriodType,
    period_end: datetime,
    force: bool = False,
) -> Tuple[LedgerCheckpoint, bool]:
    """
    Compute and persist a checkpoint for *account* at *period_end*.

    If *force* is False (default), the checkpoint is only created if one does
        not already exist for the same account/period.  When a checkpoint already
        exists, it is returned along with a boolean flag indicating that it was
        not newly created.

    The balance is computed by calling ``one_account_balance`` with
    ``as_of_date=period_end`` and cache/checkpoint lookup disabled so that
    the result is authoritative.

    Returns:
    - The checkpoint instance (newly created or existing)
    - A boolean flag indicating whether a new checkpoint was created (True) or an existing one was returned (False)

    """
    if not force:
        existing_checkpoint = await get_checkpoint_by_id(account, period_type, period_end)
        if existing_checkpoint is not None:
            logger.info(
                f"{ICON} Checkpoint already exists for {account} at {period_end} ({period_type}); skipping.",
                extra={"notification": False},
            )
            return existing_checkpoint, False

    ledger_details = await one_account_balance(
        account=account,
        as_of_date=period_end,
        use_cache=False,
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
    logger.info(f"{ICON} Created checkpoint for {account} at {period_end} ({period_type})")
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
    from v4vapp_backend_v2.accounting.account_balances import list_all_accounts
    from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry

    now = datetime.now(tz=timezone.utc)
    if until is None:
        until = now

    if since is None:
        # Find the earliest ledger entry timestamp
        first_doc = await LedgerEntry.collection().find_one(
            filter={}, sort=[("timestamp", ASCENDING)]
        )
        if first_doc is None:
            logger.info(f"{ICON} No ledger entries found; skipping checkpoint build.")
            return 0
        since = first_doc["timestamp"]
        if since and since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)

    period_ends = completed_period_ends_since(period_type, since, until)
    if not period_ends:
        logger.info(f"{ICON} No completed {period_type} periods to checkpoint.")
        return 0

    accounts = await list_all_accounts()
    total = 0

    for account in accounts:
        for period_end in period_ends:
            try:
                _, new_checkpoint = await create_checkpoint(account, period_type, period_end)
                if new_checkpoint:
                    total += 1
            except Exception as e:
                logger.warning(
                    f"⚠️  Could not create {period_type} checkpoint for "
                    f"{account.name}:{account.sub} at {period_end}: {e}",
                    extra={"notification": False},
                )

    logger.info(
        f"{ICON} Built {total} {period_type} checkpoints "
        f"({len(accounts)} accounts × {len(period_ends)} periods).",
        extra={"notification": False},
    )
    return total
