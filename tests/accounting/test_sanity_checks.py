import json
from decimal import Decimal
from pathlib import Path
from pprint import pprint

import pytest
from bson import json_util

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
    yield
    await i_c.db["ledger"].drop()
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


async def load_ledger_events(data_file: str = "tests/accounting/test_data/v4vapp-dev.ledger.json"):
    # This function should load ledger events from a file or database.
    await InternalConfig.db["ledger"].drop()
    with open(data_file) as f:
        raw_data = f.read()
        json_data = json.loads(raw_data, object_hook=json_util.object_hook)

    for ledger_entry_raw in json_data:
        ledger_entry = LedgerEntry.model_validate(ledger_entry_raw)
        await ledger_entry.save()


async def patch_account_hive_balances_from_ledger(module_monkeypatch):
    """Patch sanity_checks.account_hive_balances to return Amounts derived from the
    ledger's 'Customer Deposits Hive' balances.

    Assumes the ledger has already been loaded by the caller.
    """
    from nectar.amount import Amount

    from v4vapp_backend_v2.accounting.account_balances import one_account_balance
    from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount
    from v4vapp_backend_v2.config.setup import InternalConfig
    from v4vapp_backend_v2.helpers.currency_class import Currency

    server_id = InternalConfig().server_id
    customer_deposits_account = AssetAccount(name="Customer Deposits Hive", sub=server_id)
    deposits_details = await one_account_balance(customer_deposits_account)

    hive_deposits = deposits_details.balances_net.get(Currency.HIVE, Decimal(0.0))
    hbd_deposits = deposits_details.balances_net.get(Currency.HBD, Decimal(0.0))

    def fake_account_hive_balances(hive_accname: str = ""):
        return {
            "HIVE": Amount(f"{hive_deposits} HIVE"),
            "HBD": Amount(f"{hbd_deposits} HBD"),
        }

    module_monkeypatch.setattr(
        "v4vapp_backend_v2.accounting.sanity_checks.account_hive_balances",
        fake_account_hive_balances,
    )


# Depreciated individual tests.


# async def test_sanity_check_server_account_balances():
#     from v4vapp_backend_v2.accounting.sanity_checks import server_account_balances

#     # This ledger date has incorrect VSC Liability balance
#     await load_ledger_events("tests/accounting/test_data/v4vapp-dev.ledger-bad-vsc-liability.json")
#     result = await server_account_balances()
#     assert not result.is_valid, f"Sanity check failed: {result.details}"


# async def test_check_balance_sheet():
#     from v4vapp_backend_v2.accounting.sanity_checks import balanced_balance_sheet

#     # Load good data set
#     await load_ledger_events("tests/accounting/test_data/v4vapp-dev.ledger.json")
#     result = await balanced_balance_sheet()
#     print(result)
#     assert result.is_valid, f"Balance sheet sanity check failed: {result.details}"


# async def test_server_account_hive_balances(module_monkeypatch):
#     from v4vapp_backend_v2.accounting.sanity_checks import server_account_hive_balances

#     # Load good data set
#     await load_ledger_events("tests/accounting/test_data/v4vapp-dev.ledger.json")

#     # Patch sanity_checks.account_hive_balances from ledger values
#     await patch_account_hive_balances_from_ledger(module_monkeypatch)

#     result = await server_account_hive_balances()
#     print(result)
#     assert result.is_valid, f"Server account HIVE balances sanity check failed: {result.details}"


# @pytest.mark.skip(reason="Requires a different data set which passes all checks")
async def test_run_all_sanity_checks(module_monkeypatch):
    from v4vapp_backend_v2.accounting.sanity_checks import run_all_sanity_checks

    await load_ledger_events("tests/accounting/test_data/v4vapp-dev.ledger.json")

    # Patch sanity_checks.account_hive_balances from ledger values
    await patch_account_hive_balances_from_ledger(module_monkeypatch)

    results = await run_all_sanity_checks()
    for check_name, sanity_result in results.results:
        assert sanity_result.is_valid, (
            f"Sanity check '{check_name}' failed: {sanity_result.details}"
        )

    await load_ledger_events("tests/accounting/test_data/v4vapp-dev.ledger.json")
    results = await run_all_sanity_checks()
    for check_name, sanity_result in results.results:
        assert sanity_result.is_valid, (
            f"Sanity check '{check_name}' failed: {sanity_result.details}"
        )

    # Bad ledger – historically this dataset produced an out-of-tolerance
    # balance on one of the server accounts.  The `server_account_balances`
    # check has since been tightened to only inspect the keepsats and
    # configured hive server account, so the original "bad" file no longer
    # triggers a failure by itself.  To keep the spirit of the test we
    # explicitly introduce a tiny imbalance that the current checks will catch.
    await load_ledger_events("tests/accounting/test_data/v4vapp-dev.ledger-bad-vsc-liability.json")

    # inject a small ledger entry that pushes the keepsats account out of
    # the 2‑sats tolerance used by server_account_balances
    from datetime import datetime, timezone
    from decimal import Decimal

    from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, LiabilityAccount
    from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
    from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType

    bad_entry = LedgerEntry(
        group_id="test_bad",
        short_id="test_bad",
        ledger_type=LedgerType.UNSET,
        timestamp=datetime.now(timezone.utc),
        description="force keepsats imbalance",
        cust_id="keepsats",
        debit_amount=Decimal("3000"),
        debit_unit="msats",
        credit_amount=Decimal("3000"),
        credit_unit="msats",
        # conv_signed must exist or the balance pipeline will skip the entry
        conv_signed={"debit": {"msats": Decimal("0")}, "credit": {"msats": Decimal("0")}},
        # Use a permitted asset account name so validation passes
        debit=AssetAccount(name="Exchange Holdings", sub="x"),
        credit=LiabilityAccount(name="VSC Liability", sub="keepsats"),
    )
    await bad_entry.save()

    # clear the cache so subsequent sanity checks see the new entry
    from v4vapp_backend_v2.accounting.ledger_cache import invalidate_all_ledger_cache

    await invalidate_all_ledger_cache()

    # sanity-check that the keepsats account reflects the injected entry
    from v4vapp_backend_v2.accounting.account_balances import one_account_balance

    bal = await one_account_balance(account="keepsats", use_cache=False)
    print("keepsats after injection", bal.msats)
    assert abs(bal.msats) >= Decimal("3000"), "injected entry did not affect keepsats balance"

    results = await run_all_sanity_checks()
    pprint(results.model_dump())
    assert results.failed, "Expected failure not found (artificial imbalance was added)"


async def test_server_account_hive_balances_formatting_handles_amount(module_monkeypatch):
    """Ensure formatting a mismatched Amount doesn't raise a formatting error and produces a mismatch message."""
    from decimal import Decimal

    from nectar.amount import Amount

    from v4vapp_backend_v2.accounting.account_balances import one_account_balance
    from v4vapp_backend_v2.accounting.in_progress_results_class import InProgressResults
    from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount
    from v4vapp_backend_v2.accounting.sanity_checks import server_account_hive_balances
    from v4vapp_backend_v2.config.setup import InternalConfig
    from v4vapp_backend_v2.helpers.currency_class import Currency

    # Load good data set
    await load_ledger_events("tests/accounting/test_data/v4vapp-dev.ledger.json")

    server_id = InternalConfig().server_id
    customer_deposits_account = AssetAccount(name="Customer Deposits Hive", sub=server_id)
    deposits_details = await one_account_balance(customer_deposits_account)

    hive_deposits = deposits_details.balances_net.get(Currency.HIVE, Decimal(0.0))
    hbd_deposits = deposits_details.balances_net.get(Currency.HBD, Decimal(0.0))

    def fake_account_hive_balances(hive_accname: str = ""):
        # Create an intentional mismatch so the code formats Amount values in the failure string
        return {
            "HIVE": Amount(f"{hive_deposits + Decimal('1.0')} HIVE"),
            "HBD": Amount(f"{hbd_deposits + Decimal('1.0')} HBD"),
        }

    module_monkeypatch.setattr(
        "v4vapp_backend_v2.accounting.sanity_checks.account_hive_balances",
        fake_account_hive_balances,
    )

    result = await server_account_hive_balances(InProgressResults([]))
    assert not result.is_valid
    # Ensure it's the mismatch message and not an internal formatting failure
    assert "balances mismatch" in result.details
    assert "Failed to check customer deposits" not in result.details
