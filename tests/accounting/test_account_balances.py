import json
from pathlib import Path
from pprint import pprint

import pytest
from bson import json_util

from v4vapp_backend_v2.accounting.account_balance_pipelines import all_account_balances_pipeline
from v4vapp_backend_v2.accounting.account_balances import (
    account_balance_printout,
    account_balance_printout_grouped_by_customer,
    all_account_balances,
    keepsats_balance,
    list_all_accounts,
    one_account_balance,
)
from v4vapp_backend_v2.accounting.accounting_classes import AccountBalances, LedgerAccountDetails
from v4vapp_backend_v2.accounting.ledger_account_classes import AccountType, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn

"""
The test data for this module must be up to date with any changes in the accounting models.
"""


@pytest.fixture(scope="module")
def module_monkeypatch():
    """MonkeyPatch fixture with module scope."""
    from _pytest.monkeypatch import MonkeyPatch

    monkey_patch = MonkeyPatch()
    yield monkey_patch
    monkey_patch.undo()  # Restore original values after module tests


@pytest.fixture(autouse=True, scope="module")
async def set_base_config_path_combined(module_monkeypatch):
    test_config_path = Path("tests/data/config")
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    module_monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    i_c = InternalConfig()
    print("InternalConfig initialized:", i_c)
    db_conn = DBConn()
    await db_conn.setup_database()
    await load_ledger_events()
    yield
    await i_c.db["ledger"].drop()
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


async def load_ledger_events():
    # This function should load ledger events from a file or database.
    await InternalConfig.db["ledger"].drop()
    with open("tests/accounting/test_data/v4vapp-dev.ledger.json") as f:
        raw_data = f.read()
        json_data = json.loads(raw_data, object_hook=json_util.object_hook)

    for ledger_entry_raw in json_data:
        ledger_entry = LedgerEntry.model_validate(ledger_entry_raw)
        await ledger_entry.save()


async def test_list_all_accounts():
    """
    Test to list all accounts in the ledger.
    """
    accounts = await list_all_accounts()
    assert isinstance(accounts, list)
    assert len(accounts) > 0
    pprint(accounts)


async def test_account_details_pipeline():
    """
    Test the account details pipeline.
    """
    account = LiabilityAccount(name="VSC Liability", sub="v4vapp.dev")
    pipeline = all_account_balances_pipeline(account)
    cursor = await LedgerEntry.collection().aggregate(pipeline=pipeline)
    results = await cursor.to_list()
    for unit_result in results:
        for unit, lines in unit_result.items():
            print(f"Unit: {unit}")
            for line in lines:
                print(f"  {line['timestamp']} {line['amount_running_total']} {line['unit']}")


async def test_all_account_balances_pipeline():
    """
    Test the account details pipeline.
    """
    account = LiabilityAccount(name="Keepsats Hold", sub="keepsats")
    # call without specifying date (should default to "now", handled inside)
    pipeline = all_account_balances_pipeline(account=account)
    assert isinstance(pipeline, list)
    assert len(pipeline) > 0

    # same call with explicit None should behave identically
    pipeline_none = all_account_balances_pipeline(account=account, as_of_date=None)
    assert isinstance(pipeline_none, list)
    assert len(pipeline_none) == len(pipeline)

    # There should be an early top-level $match that short-circuits documents
    # by checking both `debit.*` and `credit.*` fields with an `$or` so the
    # `$facet` stage processes far fewer documents.
    or_stage = next(
        (
            s
            for s in pipeline
            if "$match" in s and isinstance(s["$match"], dict) and "$or" in s["$match"]
        ),
        None,
    )
    assert or_stage is not None, "expected top-level $match with $or for account filtering"
    assert {
        "debit.name": account.name,
        "debit.sub": account.sub,
        "debit.account_type": account.account_type,
    } in or_stage["$match"]["$or"]
    assert {
        "credit.name": account.name,
        "credit.sub": account.sub,
        "credit.account_type": account.account_type,
    } in or_stage["$match"]["$or"]

    # verify that the date match stage exists
    date_stage = next(
        (
            s
            for s in pipeline
            if "$match" in s and isinstance(s["$match"], dict) and "timestamp" in s["$match"]
        ),
        None,
    )
    assert date_stage is not None

    # default call (no as_of_date, no age) uses an existence check
    ts_query = date_stage["$match"]["timestamp"]
    assert ts_query == {"$exists": True}

    # explicit as_of_date without age should produce a $lte-only filter
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    pipeline_date = all_account_balances_pipeline(account=account, as_of_date=now, age=None)
    date_stage2 = next(
        (
            s
            for s in pipeline_date
            if "$match" in s and isinstance(s["$match"], dict) and "timestamp" in s["$match"]
        ),
        None,
    )
    assert date_stage2 is not None
    assert date_stage2["$match"]["timestamp"] == {"$lte": now}

    # providing only age should return a range with both $gte and $lte
    one_week = timedelta(days=7)
    pipeline_age = all_account_balances_pipeline(account=account, as_of_date=None, age=one_week)
    date_stage3 = next(
        (
            s
            for s in pipeline_age
            if "$match" in s and isinstance(s["$match"], dict) and "timestamp" in s["$match"]
        ),
        None,
    )
    assert date_stage3 is not None
    tsq3 = date_stage3["$match"]["timestamp"]
    assert "$gte" in tsq3 and "$lte" in tsq3

    # age plus explicit as_of_date should use the provided end date
    asof = datetime(2025, 1, 1, tzinfo=timezone.utc)
    age = timedelta(days=30)
    pipeline_age2 = all_account_balances_pipeline(account=account, as_of_date=asof, age=age)
    date_stage4 = next(
        (
            s
            for s in pipeline_age2
            if "$match" in s and isinstance(s["$match"], dict) and "timestamp" in s["$match"]
        ),
        None,
    )
    assert date_stage4 is not None
    assert date_stage4["$match"]["timestamp"]["$gte"] == asof - age
    assert date_stage4["$match"]["timestamp"]["$lte"] == asof


async def test_all_account_balances():
    """Test to get all account balances."""
    balances = await all_account_balances()
    assert isinstance(balances, AccountBalances)

    for item in balances.root:
        print(item.balances_printout())
        for currency, lines in item.balances.items():
            last_running_total = lines[-1].amount_running_total
            print(f"  Last Running Total: {last_running_total:,.2f}  {currency}")


async def test_one_account_balances():
    """Test to get all account balances."""
    account = LiabilityAccount(name="VSC Liability", sub="v4vapp-test")
    balance = await one_account_balance(account=account)
    assert isinstance(balance, LedgerAccountDetails)

    for currency, lines in balance.balances.items():
        print(f"Currency: {currency}")
        for line in lines:
            print(f"  {line.timestamp} {line.amount_running_total:,.2f} {line.unit}")
        if not lines:
            print("  No lines found for this currency.")

            last_running_total = lines[-1].amount_running_total
            print(f"  Last Running Total: {last_running_total:,.2f}  {currency}")

    units = set(balance.balances.keys())
    for unit in units:
        if balance.balances[unit]:
            for row in balance.balances[unit]:
                timestamp = f"{row.timestamp:%Y-%m-%d %H:%M}" if row.timestamp else "N/A"
                print(timestamp)
    pprint(balance)


async def test_get_account_balance_printout():
    account = LiabilityAccount(name="VSC Liability", sub="v4vapp-test")
    result, details = await account_balance_printout(account, line_items=True)
    print(result)
    result, details = await account_balance_printout(account, line_items=False)
    print(result)
    result, details = await account_balance_printout("v4vapp-test", line_items=False)
    print(result)
    accounts = await list_all_accounts()
    for account in accounts:
        result, details = await account_balance_printout(account)


async def test_get_keepsats_balance():
    cust_id = "v4vapp.qrc"
    net_sats, details = await keepsats_balance(cust_id=cust_id)
    pprint(details.model_dump())
    print(f"Net Sats for {cust_id}: {net_sats}")


async def test_account_balance_printout_ksats_positive_and_negative():
    """Verify the printout switches entire MSATS section to KSATS when abs(total) >= 1,000,000 sats."""
    from datetime import datetime, timezone
    from decimal import Decimal

    from v4vapp_backend_v2.accounting.account_balances import account_balance_printout
    from v4vapp_backend_v2.accounting.accounting_classes import (
        AccountBalanceLine,
        LedgerAccountDetails,
    )
    from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount
    from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
    from v4vapp_backend_v2.helpers.currency_class import Currency

    # Positive large total (msats -> 1_500_000 sats => 1_500 KSATS threshold test requires 1,000,000 sats so use larger)
    big_msats = Decimal(1_500_000_000)  # 1,500,000 sats
    line = AccountBalanceLine(
        short_id="0000-test",
        ledger_type="open_bal",
        timestamp=datetime.now(tz=timezone.utc),
        description="Big positive balance",
        cust_id="custA",
        amount=Decimal(0),
        amount_signed=Decimal(0),
        unit=Currency.MSATS,
        side="debit",
        amount_running_total=big_msats,
    )
    ledger_details = LedgerAccountDetails(
        name="VSC Liability",
        account_type=AccountType.LIABILITY,
        sub="custA",
        balances={Currency.MSATS: [line]},
    )

    result, _ = await account_balance_printout(
        LiabilityAccount(name="VSC Liability", sub="custA"),
        line_items=False,
        ledger_account_details=ledger_details,
        quote=QuoteResponse(),
    )

    assert "Unit: KSATS" in result
    assert "KSATS" in result.splitlines()[2] or "KSATS" in result  # ensure KSATS appears

    # Negative large total should also switch (abs check)
    neg_msats = Decimal(-2_000_000_000)  # -2,000,000 sats
    line_neg = AccountBalanceLine(
        short_id="0000-test",
        ledger_type="open_bal",
        timestamp=datetime.now(tz=timezone.utc),
        description="Big negative balance",
        cust_id="custB",
        amount=Decimal(0),
        amount_signed=Decimal(0),
        unit=Currency.MSATS,
        side="debit",
        amount_running_total=neg_msats,
    )
    ledger_details_neg = LedgerAccountDetails(
        name="VSC Liability",
        account_type=AccountType.LIABILITY,
        sub="custB",
        balances={Currency.MSATS: [line_neg]},
    )

    result_neg, _ = await account_balance_printout(
        LiabilityAccount(name="VSC Liability", sub="custB"),
        line_items=False,
        ledger_account_details=ledger_details_neg,
        quote=QuoteResponse(),
    )

    assert "Unit: KSATS" in result_neg

    # --- New: grouped_by_customer version ---
    result_grouped, _ = await account_balance_printout_grouped_by_customer(
        LiabilityAccount(name="VSC Liability", sub="custA"),
        line_items=False,
        ledger_account_details=ledger_details,
    )
    assert "Unit: KSATS" in result_grouped


async def test_account_balance_printout_keeps_sats_when_below_threshold():
    """Verify printout remains in SATS when total is below the KSATS threshold."""
    from datetime import datetime, timezone
    from decimal import Decimal

    from v4vapp_backend_v2.accounting.account_balances import account_balance_printout
    from v4vapp_backend_v2.accounting.accounting_classes import (
        AccountBalanceLine,
        LedgerAccountDetails,
    )
    from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount
    from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
    from v4vapp_backend_v2.helpers.currency_class import Currency

    small_msats = Decimal(500_000_000)  # 500,000 sats < 1,000,000 threshold
    line = AccountBalanceLine(
        short_id="0000-test2",
        ledger_type="open_bal",
        timestamp=datetime.now(tz=timezone.utc),
        description="Small balance",
        cust_id="custC",
        amount=Decimal(0),
        amount_signed=Decimal(0),
        unit=Currency.MSATS,
        side="debit",
        amount_running_total=small_msats,
    )
    ledger_details = LedgerAccountDetails(
        name="VSC Liability",
        account_type=AccountType.LIABILITY,
        sub="custC",
        balances={Currency.MSATS: [line]},
    )

    result, _ = await account_balance_printout(
        LiabilityAccount(name="VSC Liability", sub="custC"),
        line_items=False,
        ledger_account_details=ledger_details,
        quote=QuoteResponse(),
    )

    # Should display SATS, not KSATS
    assert "Unit: SATS" in result
    assert "KSATS" not in result
