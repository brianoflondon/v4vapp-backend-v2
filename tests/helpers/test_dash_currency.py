from datetime import UTC, datetime
from decimal import Decimal

from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.dash.amounts import DUFFS_PER_DASH
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import (
    HiveRatesDB,
    QuoteResponse,
    hive_rates_db_record,
)
from v4vapp_backend_v2.helpers.currency_class import Currency


def _quote() -> QuoteResponse:
    return QuoteResponse(
        hive_usd=Decimal("0.24"),
        hbd_usd=Decimal(1),
        btc_usd=Decimal(83000),
        hive_hbd=Decimal("0.24"),
        dash_usd=Decimal("32.5"),
        source="test",
        fetch_date=datetime(2026, 1, 1, tzinfo=UTC),
    )


def test_currency_enum_has_dash_and_duffs():
    assert Currency.DASH.value == "dash"
    assert Currency.DUFFS.value == "duffs"


def test_conversion_from_one_dash():
    quote = _quote()
    conv = CryptoConversion(conv_from=Currency.DASH, value=Decimal(1), quote=quote)
    assert conv.dash == Decimal(1)
    assert conv.duffs == DUFFS_PER_DASH
    assert conv.sats > 0
    assert conv.msats > 0
    assert conv.usd > 0
    assert conv.conversion.duffs == DUFFS_PER_DASH
    assert conv.conversion.sats_dash == quote.sats_per_dash_p
    assert "DASH" in conv.conversion.formatted_amount(Currency.DASH)
    assert conv.conversion.formatted_amount(Currency.DUFFS).endswith("DASH")


def test_conversion_from_duffs_round_trips_to_sats():
    quote = _quote()
    one_dash_duffs = DUFFS_PER_DASH
    from_duffs = CryptoConversion(conv_from=Currency.DUFFS, value=one_dash_duffs, quote=quote)
    from_sats = CryptoConversion(conv_from=Currency.SATS, value=from_duffs.sats, quote=quote)
    assert abs(from_sats.duffs - one_dash_duffs) < Decimal(2)
    assert abs(from_sats.dash - Decimal(1)) < Decimal("0.0000001")


def test_hive_conversion_fills_dash_when_quote_has_dash_usd():
    quote = _quote()
    conv = CryptoConversion(conv_from=Currency.HIVE, value=Decimal(10), quote=quote)
    assert conv.sats > 0
    assert conv.dash > 0
    assert conv.duffs > 0


def test_missing_dash_usd_does_not_break_hive_conversion():
    quote = QuoteResponse(
        hive_usd=Decimal("0.24"),
        hbd_usd=Decimal(1),
        btc_usd=Decimal(83000),
        hive_hbd=Decimal("0.24"),
        dash_usd=Decimal(0),
        source="test",
        fetch_date=datetime(2026, 1, 1, tzinfo=UTC),
    )
    conv = CryptoConversion(conv_from=Currency.HIVE, value=Decimal(10), quote=quote)
    assert conv.sats > 0
    assert conv.dash == Decimal(0)
    assert conv.duffs == Decimal(0)


def test_hive_rates_db_persists_dash():
    quote = _quote()
    record = hive_rates_db_record(quote)
    assert record.dash_usd == quote.dash_usd
    assert record.sats_dash == quote.sats_per_dash_p


def test_hive_rates_db_loads_legacy_docs_without_dash():
    record = HiveRatesDB.model_validate(
        {
            "timestamp": datetime(2026, 1, 1, tzinfo=UTC),
            "hive_usd": "0.24",
            "hbd_usd": "1",
            "btc_usd": "83000",
            "hive_hbd": "0.24",
            "sats_hive": "289",
            "sats_usd": "1204",
            "sats_hbd": "1204",
        }
    )
    assert record.dash_usd == Decimal(0)
    assert record.sats_dash == Decimal(0)


def test_ledger_entry_duffs_to_msats_balances():
    quote = _quote()
    conv = CryptoConversion(conv_from=Currency.DUFFS, value=DUFFS_PER_DASH, quote=quote).conversion
    entry = LedgerEntry(
        cust_id="server",
        group_id="dash-test_d_conv_s",
        ledger_type=LedgerType.UNSET,
        description="Dash inbound conversion",
        debit=AssetAccount(name="Unset"),
        debit_unit=Currency.DUFFS,
        debit_amount=conv.duffs,
        debit_conv=conv,
        credit=AssetAccount(name="Unset"),
        credit_unit=Currency.MSATS,
        credit_amount=conv.msats,
        credit_conv=conv,
    )
    assert getattr(entry.debit_conv, Currency.DUFFS) == conv.duffs
    assert entry.debit_unit == Currency.DUFFS
    assert entry.credit_unit == Currency.MSATS
    assert "DASH" in conv.formatted_amount(Currency.DUFFS)
