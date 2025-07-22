import json
from pathlib import Path
from pprint import pprint

import pytest
from bson import json_util

from v4vapp_backend_v2.accounting.account_balance_pipelines import (
    account_balance_details_pipeline,
    all_account_balances_pipeline,
)
from v4vapp_backend_v2.accounting.account_balances import (
    all_account_balances,
    get_account_balance,
    get_account_balance_printout,
    get_account_balance_printout2,
    list_all_accounts,
    one_account_balance,
)
from v4vapp_backend_v2.accounting.accounting_classes import AccountBalances, LedgerAccountDetails
from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn


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


async def test_get_account_balance():
    """
    Test to get the balance of a specific account.
    """
    account = LiabilityAccount(name="Customer Liability", sub="v4vapp-test")
    balance_df = await get_account_balance(account)
    assert balance_df is not None
    assert not balance_df.empty
    print(balance_df)


async def test_get_account_balance_printout():
    """
    Test to get the balance of a specific account.
    """
    account = LiabilityAccount(name="Customer Liability", sub="v4vapp-test")
    balance_printout, balance_data = await get_account_balance_printout(account, line_items=True)
    print(balance_printout)
    pprint(balance_data)
    account = LiabilityAccount(name="Customer Liability", sub="v4vapp-test")
    balance_printout, balance_data = await get_account_balance_printout(account)
    print(balance_printout)
    # pprint(balance_data)


async def test_account_details_pipeline():
    """
    Test the account details pipeline.
    """
    account = LiabilityAccount(name="Customer Liability", sub="v4vapp.dev")
    pipeline = account_balance_details_pipeline(account)
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
    pipeline = all_account_balances_pipeline(account=account)
    assert isinstance(pipeline, list)
    assert len(pipeline) > 0


async def test_all_account_balances():
    """Test to get all account balances."""
    balances = await all_account_balances()
    assert isinstance(balances, AccountBalances)

    for item in balances.root:
        print(item)
        for currency, lines in item.balances.items():
            last_running_total = lines[-1].amount_running_total
            print(f"  Last Running Total: {last_running_total:,.2f}  {currency}")


async def test_one_account_balances():
    """Test to get all account balances."""
    account = LiabilityAccount(name="Customer Liability", sub="v4vapp-test")
    balance = await one_account_balance(account=account)
    assert isinstance(balance, LedgerAccountDetails)

    print(balance)
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


async def test_get_account_balance_printout2():
    account = LiabilityAccount(name="Customer Liability", sub="v4vapp-test")
    result = await get_account_balance_printout2(account, line_items=True)
    print(result)
    accounts = await list_all_accounts()
    for account in accounts:
        result = await get_account_balance_printout2(account)
