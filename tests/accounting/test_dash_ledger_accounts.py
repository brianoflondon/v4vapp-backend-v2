from datetime import UTC, datetime
from decimal import Decimal

from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AccountType,
    AssetAccount,
    ExpenseAccount,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import (
    LedgerType,
    ledger_type_details_for_value,
    list_all_ledger_type_details,
)
from v4vapp_backend_v2.dash.amounts import DUFFS_PER_DASH
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency


def test_dash_account_names_validate():
    treasury = AssetAccount(name="Treasury Dash", sub="dash-mainnet")
    tests = AssetAccount(name="Dash Payment Tests", sub="devser.v4vapp")
    fee_income = RevenueAccount(name="Fee Income Dash", sub="dash-mainnet")
    fee_expense = ExpenseAccount(name="Fee Expenses Dash", sub="dash-mainnet")
    assert treasury.account_type == AccountType.ASSET
    assert tests.account_type == AccountType.ASSET
    assert "Treasury Dash" in AssetAccount.allowed_names()
    assert "Dash Payment Tests" in AssetAccount.allowed_names()
    assert tests.sub == "devser.v4vapp"
    assert "Fee Income Dash" in RevenueAccount.allowed_names()
    assert "Fee Expenses Dash" in ExpenseAccount.allowed_names()
    assert treasury.sub == "dash-mainnet"
    assert fee_income.name == "Fee Income Dash"
    assert fee_expense.name == "Fee Expenses Dash"


def test_conv_dash_to_sats_ledger_type():
    assert LedgerType.CONV_DASH_TO_SATS.value == "d_conv_s"
    assert LedgerType.DASH_TEST_PAY.value == "d_test_p"
    assert len(LedgerType.CONV_DASH_TO_SATS.value) <= 10
    assert len(LedgerType.DASH_TEST_PAY.value) <= 10
    details = ledger_type_details_for_value("d_conv_s")
    assert details is not None
    assert details.ledger_type is LedgerType.CONV_DASH_TO_SATS
    assert details.label == "Dash Conversion"
    assert details.icon == "💠"
    assert any(
        d.ledger_type is LedgerType.CONV_DASH_TO_SATS for d in list_all_ledger_type_details()
    )


def test_dummy_dash_conversion_ledger_entry():
    quote = QuoteResponse(
        hive_usd=Decimal("0.24"),
        hbd_usd=Decimal(1),
        btc_usd=Decimal(83000),
        hive_hbd=Decimal("0.24"),
        dash_usd=Decimal("32.5"),
        source="test",
        fetch_date=datetime(2026, 1, 1, tzinfo=UTC),
    )
    conv = CryptoConversion(conv_from=Currency.DUFFS, value=DUFFS_PER_DASH, quote=quote).conversion
    entry = LedgerEntry(
        cust_id="server",
        group_id="invoice1_d_conv_s",
        ledger_type=LedgerType.CONV_DASH_TO_SATS,
        description="Convert inbound Dash to sats payable",
        debit=AssetAccount(name="Treasury Dash", sub="dash-mainnet"),
        debit_unit=Currency.DUFFS,
        debit_amount=conv.duffs,
        debit_conv=conv,
        credit=LiabilityAccount(name="VSC Liability", sub="server"),
        credit_unit=Currency.MSATS,
        credit_amount=conv.msats,
        credit_conv=conv,
    )
    assert entry.ledger_type is LedgerType.CONV_DASH_TO_SATS
    assert entry.debit.name == "Treasury Dash"
    assert entry.credit.name == "VSC Liability"
    assert entry.is_completed
