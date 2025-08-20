import json
from pathlib import Path
from pprint import pprint

import pytest
from bson import json_util

from v4vapp_backend_v2.accounting.account_balance_pipelines import all_account_balances_pipeline
from v4vapp_backend_v2.accounting.account_balances import (
    account_balance_printout,
    all_account_balances,
    get_keepsats_balance,
    list_all_accounts,
    one_account_balance,
)
from v4vapp_backend_v2.accounting.accounting_classes import AccountBalances, LedgerAccountDetails
from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount
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
    pipeline = all_account_balances_pipeline(account=account)
    assert isinstance(pipeline, list)
    assert len(pipeline) > 0


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
    net_sats, details = await get_keepsats_balance(cust_id=cust_id)
    pprint(details.model_dump())
    print(f"Net Sats for {cust_id}: {net_sats}")
