"""
Tests for the ledger balance checkpoint system.

Covers:
- Period boundary calculations (daily, weekly, monthly)
- completed_period_ends_since enumeration
- LedgerCheckpoint save/load round-trip
- create_checkpoint stores correct net balances
- get_latest_checkpoint_before returns correct document
- one_account_balance uses checkpoint (delta path) and produces the
  same final totals as the full-history path
"""

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from bson import json_util

from v4vapp_backend_v2.accounting.account_balances import one_account_balance
from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_checkpoints import (
    CheckpointConvSummary,
    LedgerCheckpoint,
    PeriodType,
    build_checkpoints_for_period,
    completed_period_ends_since,
    create_checkpoint,
    get_latest_checkpoint_before,
    last_completed_period_end,
    period_end_for_date,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn

# ---------------------------------------------------------------------------
# Module-scoped fixtures (DB + test data)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def module_monkeypatch():
    from _pytest.monkeypatch import MonkeyPatch

    mp = MonkeyPatch()
    yield mp
    mp.undo()


@pytest.fixture(autouse=True, scope="module")
async def setup_test_db(module_monkeypatch):
    test_config_path = Path("tests/data/config")
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    module_monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    i_c = InternalConfig()
    db_conn = DBConn()
    await db_conn.setup_database()

    # Load test ledger data
    await InternalConfig.db["ledger"].drop()
    await InternalConfig.db["ledger_checkpoints"].drop()
    with open("tests/accounting/test_data/v4vapp-dev.ledger.json") as f:
        raw_data = f.read()
        json_data = json.loads(raw_data, object_hook=json_util.object_hook)
    for entry_raw in json_data:
        entry = LedgerEntry.model_validate(entry_raw)
        await entry.save()

    await LedgerCheckpoint.ensure_indexes()

    yield

    await InternalConfig.db["ledger"].drop()
    await InternalConfig.db["ledger_checkpoints"].drop()
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


# ---------------------------------------------------------------------------
# Pure unit tests – no DB needed
# ---------------------------------------------------------------------------


class TestPeriodEndForDate:
    def test_daily(self):
        d = date(2024, 3, 15)
        end = period_end_for_date(PeriodType.DAILY, d)
        assert end == datetime(2024, 3, 15, 23, 59, 59, 999999, tzinfo=timezone.utc)

    def test_weekly_wednesday(self):
        d = date(2024, 3, 13)  # Wednesday
        end = period_end_for_date(PeriodType.WEEKLY, d)
        # ISO week: Sunday = weekday 6, Wed offset = 6-2 = 4 days
        assert end.weekday() == 6  # Sunday
        assert end == datetime(2024, 3, 17, 23, 59, 59, 999999, tzinfo=timezone.utc)

    def test_weekly_sunday(self):
        d = date(2024, 3, 17)  # Already Sunday
        end = period_end_for_date(PeriodType.WEEKLY, d)
        assert end == datetime(2024, 3, 17, 23, 59, 59, 999999, tzinfo=timezone.utc)

    def test_monthly_march(self):
        d = date(2024, 3, 5)
        end = period_end_for_date(PeriodType.MONTHLY, d)
        assert end == datetime(2024, 3, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)

    def test_monthly_february_leap(self):
        d = date(2024, 2, 1)
        end = period_end_for_date(PeriodType.MONTHLY, d)
        assert end == datetime(2024, 2, 29, 23, 59, 59, 999999, tzinfo=timezone.utc)


class TestLastCompletedPeriodEnd:
    """last_completed_period_end always returns a boundary that has already passed."""

    def test_daily(self):
        # Wednesday March 26, 2026 at noon → yesterday = March 25
        now = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)
        end = last_completed_period_end(PeriodType.DAILY, now)
        assert end == datetime(2026, 3, 25, 23, 59, 59, 999999, tzinfo=timezone.utc)

    def test_weekly_wednesday(self):
        # Wednesday March 25, 2026 → last completed Sunday = March 22
        now = datetime(2026, 3, 25, 12, 0, 0, tzinfo=timezone.utc)
        end = last_completed_period_end(PeriodType.WEEKLY, now)
        assert end == datetime(2026, 3, 22, 23, 59, 59, 999999, tzinfo=timezone.utc)
        assert end.weekday() == 6  # Sunday

    def test_weekly_monday(self):
        # Monday March 23, 2026 → last completed Sunday = March 22
        now = datetime(2026, 3, 23, 6, 0, 0, tzinfo=timezone.utc)
        end = last_completed_period_end(PeriodType.WEEKLY, now)
        assert end == datetime(2026, 3, 22, 23, 59, 59, 999999, tzinfo=timezone.utc)

    def test_weekly_sunday(self):
        # Sunday March 29, 2026 → last completed Sunday = March 22 (not today)
        now = datetime(2026, 3, 29, 12, 0, 0, tzinfo=timezone.utc)
        end = last_completed_period_end(PeriodType.WEEKLY, now)
        assert end == datetime(2026, 3, 22, 23, 59, 59, 999999, tzinfo=timezone.utc)

    def test_monthly_march(self):
        # March 26, 2026 → previous month end = February 28, 2026
        now = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)
        end = last_completed_period_end(PeriodType.MONTHLY, now)
        assert end == datetime(2026, 2, 28, 23, 59, 59, 999999, tzinfo=timezone.utc)

    def test_monthly_first_of_month(self):
        # March 1, 2026 → previous month end = February 28, 2026
        now = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
        end = last_completed_period_end(PeriodType.MONTHLY, now)
        assert end == datetime(2026, 2, 28, 23, 59, 59, 999999, tzinfo=timezone.utc)

    def test_monthly_leap_year(self):
        # March 5, 2024 → previous month end = February 29, 2024 (leap year)
        now = datetime(2024, 3, 5, 12, 0, 0, tzinfo=timezone.utc)
        end = last_completed_period_end(PeriodType.MONTHLY, now)
        assert end == datetime(2024, 2, 29, 23, 59, 59, 999999, tzinfo=timezone.utc)


class TestCompletedPeriodEnds:
    def test_daily_three_days(self):
        since = datetime(2024, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
        until = datetime(2024, 3, 4, 12, 0, 0, tzinfo=timezone.utc)
        ends = completed_period_ends_since(PeriodType.DAILY, since, until)
        assert len(ends) == 3
        assert ends[0].day == 1
        assert ends[-1].day == 3

    def test_empty_when_same_day(self):
        since = datetime(2024, 3, 15, 10, 0, 0, tzinfo=timezone.utc)
        until = datetime(2024, 3, 15, 23, 0, 0, tzinfo=timezone.utc)
        ends = completed_period_ends_since(PeriodType.DAILY, since, until)
        assert ends == []

    def test_monthly_boundaries(self):
        since = datetime(2024, 1, 1, tzinfo=timezone.utc)
        until = datetime(2024, 4, 1, tzinfo=timezone.utc)
        ends = completed_period_ends_since(PeriodType.MONTHLY, since, until)
        assert len(ends) == 3
        assert ends[0].month == 1
        assert ends[1].month == 2
        assert ends[2].month == 3


# ---------------------------------------------------------------------------
# Integration tests – require DB
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_checkpoint_save_load():
    """A LedgerCheckpoint saved to MongoDB can be round-tripped via _from_mongo_doc."""
    period_end = datetime(2025, 1, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
    cp = LedgerCheckpoint(
        account_name="VSC Liability",
        account_sub="testuser",
        account_type="Liability",
        contra=False,
        period_type=PeriodType.MONTHLY,
        period_end=period_end,
        balances_net={"MSATS": Decimal("123456"), "HIVE": Decimal("1.234")},
        conv_totals={
            "MSATS": CheckpointConvSummary(
                hive=Decimal("1.0"),
                hbd=Decimal("0.5"),
                usd=Decimal("0.3"),
                sats=Decimal("123"),
                msats=Decimal("123456"),
            )
        },
    )
    await cp.save()

    account = LiabilityAccount(name="VSC Liability", sub="testuser")
    loaded = await get_latest_checkpoint_before(account, datetime(2025, 2, 1, tzinfo=timezone.utc))
    assert loaded is not None
    assert loaded.account_sub == "testuser"
    assert loaded.period_type == PeriodType.MONTHLY
    assert loaded.balances_net["MSATS"] == Decimal("123456")
    assert loaded.balances_net["HIVE"] == Decimal("1.234")
    cs = loaded.conv_totals["MSATS"].to_converted_summary()
    assert cs.hive == Decimal("1.0")
    assert cs.msats == Decimal("123456")


@pytest.mark.asyncio
async def test_get_latest_checkpoint_before_returns_none_for_future():
    """No checkpoint should be returned when as_of_date is before all period_ends."""
    account = LiabilityAccount(name="VSC Liability", sub="testuser")
    # Epoch = no checkpoints exist before this
    result = await get_latest_checkpoint_before(account, datetime(2000, 1, 1, tzinfo=timezone.utc))
    assert result is None


@pytest.mark.asyncio
async def test_create_checkpoint_matches_full_balance():
    """
    create_checkpoint should produce net balances identical to a full one_account_balance call.

    We pick an account that is known to have entries in the test dataset and
    verify that the checkpoint's MSATS net equals the balance computed directly.
    """
    # Discover an account sub that exists in the test data
    test_accounts = await LedgerEntry.collection().distinct("credit.sub")
    if not test_accounts:
        pytest.skip("No accounts found in test data")

    sub = test_accounts[0]
    account = LiabilityAccount(name="VSC Liability", sub=sub)

    # Use a period_end well beyond the test data range so everything is captured
    period_end = datetime(2030, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)

    checkpoint, new_checkpoint = await create_checkpoint(account, PeriodType.MONTHLY, period_end)
    assert new_checkpoint is True, "Expected to create a new checkpoint document"

    full_balance = await one_account_balance(
        account, as_of_date=period_end, use_cache=False, use_checkpoints=False
    )

    for unit_str, cp_net in checkpoint.balances_net.items():
        from v4vapp_backend_v2.helpers.currency_class import Currency

        try:
            currency = Currency(unit_str)
        except ValueError:
            continue
        expected = full_balance.balances_net.get(currency, Decimal(0))
        assert cp_net == expected, f"Mismatch for {unit_str}: checkpoint={cp_net} full={expected}"


@pytest.mark.asyncio
async def test_one_account_balance_checkpoint_matches_full_history():
    """
    one_account_balance with use_checkpoints=True must produce the same final
    balances as the full-history path.

    We first create a checkpoint at an intermediate date, then query a later date
    and compare the two code paths.
    """
    # Use the earliest timestamped entry to find an active sub
    first_doc = await LedgerEntry.collection().find_one(filter={}, sort=[("timestamp", 1)])
    if first_doc is None:
        pytest.skip("No ledger entries in test data")

    # Find a sub with entries
    all_subs = await LedgerEntry.collection().distinct("credit.sub")
    if not all_subs:
        pytest.skip("No credit subs found in test data")
    sub = all_subs[0]
    account = LiabilityAccount(name="VSC Liability", sub=sub)

    # Find entry timestamps to create a mid-point checkpoint
    docs = (
        await LedgerEntry.collection()
        .find(
            filter={"$or": [{"debit.sub": sub}, {"credit.sub": sub}]},
            sort=[("timestamp", 1)],
        )
        .to_list(length=None)
    )

    if len(docs) < 2:
        pytest.skip(f"Not enough entries for sub={sub}")

    mid_idx = len(docs) // 2
    mid_ts = docs[mid_idx]["timestamp"]
    if mid_ts.tzinfo is None:
        mid_ts = mid_ts.replace(tzinfo=timezone.utc)

    last_ts = docs[-1]["timestamp"]
    if last_ts.tzinfo is None:
        last_ts = last_ts.replace(tzinfo=timezone.utc)

    # Create checkpoint at the midpoint
    await LedgerCheckpoint.collection().delete_many(
        {"account_name": account.name, "account_sub": sub}
    )
    await create_checkpoint(account, PeriodType.DAILY, mid_ts)

    # Query with and without checkpoints at the last timestamp
    result_with_cp = await one_account_balance(
        account, as_of_date=last_ts, use_cache=False, use_checkpoints=True
    )
    result_without_cp = await one_account_balance(
        account, as_of_date=last_ts, use_cache=False, use_checkpoints=False
    )

    # Final net balances should agree within floating-point rounding
    assert abs(result_with_cp.msats - result_without_cp.msats) <= Decimal("10"), (
        f"MSATS mismatch: with_cp={result_with_cp.msats} without={result_without_cp.msats}"
    )
    assert abs(result_with_cp.hive - result_without_cp.hive) <= Decimal("0.001"), (
        f"HIVE mismatch: with_cp={result_with_cp.hive} without={result_without_cp.hive}"
    )


@pytest.mark.asyncio
async def test_build_checkpoints_for_period():
    """build_checkpoints_for_period creates documents for all accounts × periods."""
    await InternalConfig.db["ledger_checkpoints"].drop()
    await LedgerCheckpoint.ensure_indexes()

    # Narrow the range to keep the test fast
    since = datetime(2024, 1, 1, tzinfo=timezone.utc)
    until = datetime(2024, 2, 1, tzinfo=timezone.utc)

    count = await build_checkpoints_for_period(PeriodType.MONTHLY, since=since, until=until)
    # At least one checkpoint per account (there may be 0 if test data precedes this range)
    docs_count = await InternalConfig.db["ledger_checkpoints"].count_documents({})
    assert docs_count == count
