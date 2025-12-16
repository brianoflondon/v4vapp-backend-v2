import json
from decimal import Decimal
from pathlib import Path
from pprint import pprint

import pytest
from bson import json_util

from v4vapp_backend_v2.accounting.balance_sheet import (
    balance_sheet_all_currencies_printout,
    balance_sheet_printout,
    check_balance_sheet_mongodb,
    generate_balance_sheet_mongodb,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.profit_and_loss import (
    generate_profit_and_loss_report,
    profit_and_loss_printout,
)
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


async def test_balance_sheet():
    balance_sheet_dict = await generate_balance_sheet_mongodb()

    pprint(balance_sheet_dict)
    assert balance_sheet_dict["is_balanced"], "Balance sheet isn't balanced."
    bs_printout = balance_sheet_printout(balance_sheet_dict)
    print(bs_printout)

    bs_printout_currencies = balance_sheet_all_currencies_printout(balance_sheet_dict)
    print(bs_printout_currencies)


async def test_check_balance_sheet_mongodb():
    is_balanced, tolerance = await check_balance_sheet_mongodb()

    print(is_balanced, tolerance)
    assert is_balanced, "Balance sheet isn't balanced"


async def test_generate_profit_and_loss_report():
    pl_report = await generate_profit_and_loss_report()
    print(pl_report)

    assert "Revenue" in pl_report, "Profit and Loss report does not contain Revenue."
    assert "Expenses" in pl_report, "Profit and Loss report does not contain Expenses."
    assert "Net Income" in pl_report, "Profit and Loss report does not contain Net Income."

    pl_printout = await profit_and_loss_printout(pl_report=pl_report)
    print(pl_printout)


async def test_exc_conv_nets_to_zero():
    """Each exc_conv entry should have debit and credit sides that net to zero (per unit)."""
    cursor = LedgerEntry.collection().find({"ledger_type": "exc_conv"})
    entries = await cursor.to_list(length=None)

    def D(v):
        if isinstance(v, dict):
            if "$numberDecimal" in v:
                return Decimal(v["$numberDecimal"])
            return Decimal(0)
        if v is None:
            return Decimal(0)
        return Decimal(str(v))

    for e in entries:
        conv = e.get("conv_signed")
        if not conv:
            continue
        d = conv.get("debit", {})
        c = conv.get("credit", {})
        # msats check (preferred canonical integer unit)
        assert D(d.get("msats")) + D(c.get("msats")) == 0
        # hive check
        assert D(d.get("hive")) + D(c.get("hive")) == 0


async def test_exc_fee_balances():
    """exc_fee entries should balance between Expense (debit) and Exchange Holdings (credit)."""
    cursor = LedgerEntry.collection().find({"ledger_type": "exc_fee"})
    entries = await cursor.to_list(length=None)

    def D(v):
        if isinstance(v, dict):
            if "$numberDecimal" in v:
                return Decimal(v["$numberDecimal"])
            return Decimal(0)
        if v is None:
            return Decimal(0)
        return Decimal(str(v))

    for e in entries:
        conv = e.get("conv_signed")
        if not conv:
            continue
        d = conv.get("debit", {})
        c = conv.get("credit", {})
        # msats must sum to zero across debit/credit for the fee entry
        assert D(d.get("msats")) + D(c.get("msats")) == 0


async def test_report_contains_explanatory_note():
    balance_sheet_dict = await generate_balance_sheet_mongodb()
    s = balance_sheet_all_currencies_printout(balance_sheet_dict)
    assert "Unit lines represent values converted into each unit" in s


async def test_db_checks_reject_exc_conv_msats_mismatch():
    """db_checks should reject exc_conv entries whose conv sides don't net to zero (msats)."""
    doc = await LedgerEntry.collection().find_one({"ledger_type": "exc_conv"})
    assert doc, "No exc_conv entry found in test data"
    entry = LedgerEntry.model_validate(doc)
    # Corrupt the msats so they don't net to zero
    entry.debit_conv.msats = entry.debit_conv.msats + 10000
    from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntryCreationException

    with pytest.raises(LedgerEntryCreationException):
        entry.db_checks()


async def test_db_checks_reject_exc_fee_msats_mismatch():
    """db_checks should reject exc_fee entries whose conv sides don't net to zero (msats)."""
    doc = await LedgerEntry.collection().find_one({"ledger_type": "exc_fee"})
    assert doc, "No exc_fee entry found in test data"
    entry = LedgerEntry.model_validate(doc)
    # Corrupt the msats so they don't net to zero
    entry.debit_conv.msats = entry.debit_conv.msats + 5000
    from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntryCreationException

    with pytest.raises(LedgerEntryCreationException):
        entry.db_checks()
