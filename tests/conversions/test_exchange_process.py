from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.conversion.exchange_process import exchange_accounting
from v4vapp_backend_v2.conversion.exchange_protocol import ExchangeOrderResult
from v4vapp_backend_v2.conversion.exchange_rebalance import RebalanceResult
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency, QuoteResponse


@pytest.fixture(autouse=True)
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    # reuse the same config fixture pattern from other tests
    from pathlib import Path

    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


@pytest.fixture
def patch_ledger_save(monkeypatch):
    """Prevent tests from touching a real MongoDB instance.

    Instead of inserting to the database, saved entries are collected in
    the returned list so tests can inspect them.
    """
    saved_entries: list[LedgerEntry] = []

    async def fake_save(self, *args, **kwargs):
        saved_entries.append(self)
        return None

    monkeypatch.setattr(LedgerEntry, "save", fake_save)
    return saved_entries


async def make_quote_unit_rates():
    """Return a minimal quote object with sane non-zero rates."""
    q = MagicMock(spec=QuoteResponse)
    # set attributes used by CryptoConversion
    q.sats_hive_p = Decimal("1")
    q.sats_hbd_p = Decimal("1")
    q.sats_usd_p = Decimal("1")
    q.btc_usd = Decimal("1")
    # conversion code also reads source field for log, so provide one
    q.source = "unit-test"
    q.fetch_date = datetime.now(tz=timezone.utc)
    return q


@pytest.mark.asyncio
async def test_exchange_accounting_sell_direction(patch_ledger_save):
    quote = await make_quote_unit_rates()

    # setup order result representing a sell of 2 Hive for some msats
    order = ExchangeOrderResult(
        exchange="binance",
        symbol="HIVEBTC",
        order_id="ord-S",
        client_order_id="short-S",
        side="SELL",
        status="FILLED",
        requested_qty=Decimal("2"),
        executed_qty=Decimal("2"),
        quote_qty=Decimal("0.00001"),
        avg_price=Decimal("0"),
        fee_msats=Decimal("0"),
        fee_original=Decimal("0"),
        fee_asset="BTC",
        raw_response={},
        trade_quote=quote,
    )

    rebalance_result = RebalanceResult(
        executed=True, order_result=order, ledger_description="sell test"
    )
    tracked_op = MagicMock(short_id="short-S", cust_id="cust", group_id="grp")

    await exchange_accounting(rebalance_result, tracked_op)

    # our fake_save fixture captured the entry instead of writing it to Mongo
    assert len(patch_ledger_save) == 1
    entry = patch_ledger_save[0]

    # for a sell we should credit Hive and debit msats
    assert entry.debit_unit == Currency.MSATS
    assert entry.credit_unit == Currency.HIVE
    assert float(entry.debit_amount) >= 0
    # debit_amount_signed should be positive, credit_amount_signed negative
    assert entry.debit_amount_signed > 0
    assert entry.credit_amount_signed < 0

    # verify numeric values match conversion
    conv = CryptoConversion(
        conv_from=Currency.HIVE, value=order.executed_qty, quote=quote
    ).conversion
    assert entry.debit_amount == conv.msats
    assert entry.credit_amount == conv.hive


@pytest.mark.asyncio
async def test_exchange_accounting_buy_direction(patch_ledger_save):
    quote = await make_quote_unit_rates()

    order = ExchangeOrderResult(
        exchange="binance",
        symbol="HIVEBTC",
        order_id="ord-B",
        client_order_id="short-B",
        side="BUY",
        status="FILLED",
        requested_qty=Decimal("5"),
        executed_qty=Decimal("5"),
        quote_qty=Decimal("0.00005"),
        avg_price=Decimal("0"),
        fee_msats=Decimal("0"),
        fee_original=Decimal("0"),
        fee_asset="BTC",
        raw_response={},
        trade_quote=quote,
    )

    rebalance_result = RebalanceResult(
        executed=True, order_result=order, ledger_description="buy test"
    )
    tracked_op = MagicMock(short_id="short-B", cust_id="cust2", group_id="grp2")

    await exchange_accounting(rebalance_result, tracked_op)

    # our fake_save fixture captured the entry instead of writing it to Mongo
    assert len(patch_ledger_save) == 1
    entry = patch_ledger_save[0]

    # buy: hive is acquired (debit), msats are spent (credit)
    assert entry.debit_unit == Currency.HIVE
    assert entry.credit_unit == Currency.MSATS
    assert entry.debit_amount_signed > 0
    assert entry.credit_amount_signed < 0

    conv = CryptoConversion(
        conv_from=Currency.MSATS,
        value=order.quote_qty * Decimal("100_000_000_000"),
        quote=quote,
    ).conversion
    assert entry.debit_amount == conv.hive
    assert entry.credit_amount == conv.msats
