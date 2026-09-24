from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.conversion.exchange_process import exchange_accounting
from v4vapp_backend_v2.conversion.exchange_protocol import ExchangeOrderResult
from v4vapp_backend_v2.conversion.exchange_rebalance import RebalanceResult
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion, QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency


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

    monkeypatch.setattr(LedgerEntry, "save", fake_save)
    return saved_entries


async def make_quote_unit_rates():
    """Return a minimal quote object with sane non-zero rates."""
    q = MagicMock(spec=QuoteResponse)
    # set attributes used by CryptoConversion
    q.sats_hive_p = Decimal(1)
    q.sats_hbd_p = Decimal(1)
    q.sats_usd_p = Decimal(1)
    q.sats_per_dash_p = Decimal(0)
    q.btc_usd = Decimal(1)
    # conversion code also reads source field for log, so provide one
    q.source = "unit-test"
    q.fetch_date = datetime.now(tz=UTC)
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
        requested_qty=Decimal(2),
        executed_qty=Decimal(2),
        quote_qty=Decimal("0.00001"),
        avg_price=Decimal(0),
        fee_msats=Decimal(0),
        fee_original=Decimal(0),
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
        requested_qty=Decimal(5),
        executed_qty=Decimal(5),
        quote_qty=Decimal("0.00005"),
        avg_price=Decimal(0),
        fee_msats=Decimal(0),
        fee_original=Decimal(0),
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
        value=order.quote_qty * Decimal(100_000_000_000),
        quote=quote,
    ).conversion
    assert entry.debit_amount == conv.hive
    assert entry.credit_amount == conv.msats


def _dash_quote() -> QuoteResponse:
    return QuoteResponse(
        hive_usd=Decimal("0.24"),
        hbd_usd=Decimal(1),
        btc_usd=Decimal(83000),
        hive_hbd=Decimal("0.24"),
        dash_usd=Decimal("32.5"),
        source="unit-test",
        fetch_date=datetime.now(tz=UTC),
    )


@pytest.mark.asyncio
async def test_exchange_accounting_sell_dash(patch_ledger_save):
    quote = _dash_quote()
    order = ExchangeOrderResult(
        exchange="binance_convert",
        symbol="DASHBTC",
        order_id="ord-D",
        client_order_id="short-D",
        side="SELL",
        status="SUCCESS",
        requested_qty=Decimal(1),
        executed_qty=Decimal(1),
        quote_qty=Decimal("0.00039157"),
        avg_price=Decimal("0.00039157"),
        fee_msats=Decimal(0),
        fee_original=Decimal(0),
        fee_asset="",
        raw_response={},
        base_asset="DASH",
        quote_asset="BTC",
        trade_quote=quote,
    )
    rebalance_result = RebalanceResult(
        executed=True, order_result=order, ledger_description="sell dash"
    )
    tracked_op = MagicMock(short_id="short-D", cust_id="server", group_id="inv1")

    await exchange_accounting(rebalance_result, tracked_op)

    assert len(patch_ledger_save) == 1
    entry = patch_ledger_save[0]
    assert entry.debit_unit == Currency.MSATS
    assert entry.credit_unit == Currency.DUFFS
    conv = CryptoConversion(
        conv_from=Currency.DASH, value=order.executed_qty, quote=quote
    ).conversion
    assert entry.debit_amount == conv.msats
    assert entry.credit_amount == conv.duffs


@pytest.mark.asyncio
async def test_rebalance_queue_task_passes_dash_pair(monkeypatch):
    from unittest.mock import AsyncMock

    from v4vapp_backend_v2.conversion.exchange_process import rebalance_queue_task
    from v4vapp_backend_v2.conversion.exchange_rebalance import RebalanceDirection

    captured: dict = {}

    async def fake_pending(**kwargs):
        captured.update(kwargs)
        return RebalanceResult(executed=False, reason="below min")

    monkeypatch.setattr("v4vapp_backend_v2.conversion.exchange_process.asyncio.sleep", AsyncMock())
    monkeypatch.setattr(
        "v4vapp_backend_v2.conversion.exchange_process.add_pending_rebalance",
        fake_pending,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.conversion.exchange_process.get_exchange_adapter",
        lambda: object(),
    )
    tracked = MagicMock(short_id="inv1", cust_id="server", group_id="inv1", log_extra={})
    await rebalance_queue_task(
        direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        currency=Currency.DASH,
        hive_qty=Decimal("1.25"),
        tracked_op=tracked,
        base_asset="DASH",
        quote_asset="BTC",
    )
    assert captured["base_asset"] == "DASH"
    assert captured["quote_asset"] == "BTC"
    assert captured["qty"] == Decimal("1.25")
    assert captured["direction"] is RebalanceDirection.SELL_BASE_FOR_QUOTE
