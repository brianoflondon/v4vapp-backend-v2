"""Read-only Magi shadow lines for a Binance rebalance decision."""

import logging
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from v4vapp_backend_v2.conversion.exchange_protocol import ExchangeOrderResult
from v4vapp_backend_v2.conversion.exchange_rebalance import _log_magi_shadow
from v4vapp_backend_v2.conversion.magi_quote import VenueQuote
from v4vapp_backend_v2.conversion.magi_shadow import (
    edge_bps,
    log_magi_rebalance_shadow,
    quantize_asset,
)

FIXED_AT = datetime(2026, 11, 24, tzinfo=UTC)


def _quote(
    *,
    side: str,
    amount_in: Decimal,
    amount_out: Decimal,
    sats_per_base: Decimal,
    fee_bps: int = 20,
    abort: str | None = None,
    amount_in_units: int = 12_000,
    reserve_in: int = 60_000_000,
) -> VenueQuote:
    return VenueQuote(
        venue="MagiSwap",
        base_asset="HIVE",
        quote_asset="BTC",
        side=side,
        amount_in=amount_in,
        amount_out=amount_out,
        sats_per_base=sats_per_base,
        fee_bps=fee_bps,
        min_amount_out=amount_out,
        pool_impact_pct=Decimal("0.01"),
        raw={
            "abort": abort,
            "aborts": [abort] if abort else [],
            "hops": 2,
            "hop_detail": [
                {
                    "pool": "hive-hbd",
                    "asset_in": "hive",
                    "asset_out": "hbd",
                    "amount_in": amount_in_units,
                    "reserve_in": reserve_in,
                    "reserve_out": 2_000_000,
                    "expected": 1,
                    "fee_bps": fee_bps,
                }
            ],
            "units_in": amount_in_units,
            "units_out": 720,
            "slippage_bps": 50,
        },
        fetched_at=FIXED_AT,
    )


def _order(*, side: str, executed: Decimal, quote_qty: Decimal) -> ExchangeOrderResult:
    return ExchangeOrderResult(
        exchange="binance_convert",
        symbol="HIVEBTC",
        order_id="order-1",
        side=side,
        status="SUCCESS",
        requested_qty=executed,
        executed_qty=executed,
        quote_qty=quote_qty,
        avg_price=quote_qty / executed,
        fee_msats=Decimal(0),
        fee_original=Decimal(0),
        fee_asset="BTC",
        raw_response={"ignored": True},
        base_asset="HIVE",
        quote_asset="BTC",
    )


async def _fair():
    return Decimal("0.05"), Decimal(100_000)


def _record(caplog: pytest.LogCaptureFixture) -> logging.LogRecord:
    matches = [record for record in caplog.records if getattr(record, "magi_shadow", False)]
    assert matches, "expected a Magi shadow log line"
    return matches[-1]


@pytest.mark.asyncio
async def test_below_minimum_sell_logs_magi_against_fair(caplog):
    caplog.set_level(logging.INFO, logger="backend")
    seen = {}

    async def preview(asset_in, asset_out, amount_in):
        seen["args"] = (asset_in, asset_out, amount_in)
        return _quote(
            side="SELL",
            amount_in=amount_in,
            amount_out=Decimal("0.00000720"),
            sats_per_base=Decimal(60),
        )

    await log_magi_rebalance_shadow(
        base_asset="HIVE",
        quote_asset="BTC",
        direction="sell",
        qty=Decimal("12.3456"),
        stage="below_minimum",
        reason="Pending qty 12.346 HIVE below minimum 50.000 HIVE",
        min_qty=Decimal(50),
        min_notional=Decimal("0.0001"),
        transaction_id="trx-1",
        exchange="binance_convert",
        preview=preview,
        fair_book=_fair,
    )

    record = _record(caplog)
    assert record.notification is False
    assert record.stage == "below_minimum"
    assert record.binance_sent is False
    assert record.would_take_magi is True
    assert Decimal(record.magi_sats_per_base) == Decimal(60)
    assert Decimal(record.fair_sats_per_base) == Decimal(50)
    assert Decimal(record.edge_vs_fair_bps) == Decimal(2000)
    assert "binance_not_sent" in record.message
    assert "would_take true" in record.message
    detail = record.magi_shadow_detail
    assert detail["min_qty"] == "50"
    assert detail["transaction_id"] == "trx-1"
    assert detail["magi"]["hop_detail"][0]["reserve_in"] == 60_000_000
    assert detail["router_check"]["reference"] == "fair_usdt"
    assert detail["binance"] is None
    assert "raw_response" not in str(detail)
    assert seen["args"] == ("HIVE", "BTC", Decimal("12.345"))


@pytest.mark.asyncio
async def test_small_edge_and_abort_do_not_clear_the_label(caplog):
    caplog.set_level(logging.INFO, logger="backend")

    async def preview(asset_in, asset_out, amount_in):
        return _quote(
            side="SELL",
            amount_in=amount_in,
            amount_out=Decimal("0.000006012"),
            sats_per_base=Decimal("50.1"),
            abort="reserve",
        )

    await log_magi_rebalance_shadow(
        base_asset="HIVE",
        quote_asset="BTC",
        direction="sell",
        qty=Decimal(1),
        stage="below_minimum",
        reason="below minimum",
        min_qty=Decimal(50),
        min_notional=Decimal(0),
        preview=preview,
        fair_book=_fair,
    )

    record = _record(caplog)
    assert record.would_take_magi is False
    detail = record.magi_shadow_detail
    assert detail["router_check"]["passes_edge"] is False
    assert detail["router_check"]["passes_abort"] is False
    assert detail["magi"]["abort"] == "reserve"
    assert Decimal(detail["edge_vs_fair_bps"]) == Decimal(20)


@pytest.mark.asyncio
async def test_sent_sell_compares_magi_with_the_binance_fill(caplog):
    caplog.set_level(logging.INFO, logger="backend")
    order = _order(side="SELL", executed=Decimal(12), quote_qty=Decimal("0.00000600"))

    async def preview(asset_in, asset_out, amount_in):
        return _quote(
            side="SELL",
            amount_in=amount_in,
            amount_out=Decimal("0.00000720"),
            sats_per_base=Decimal(60),
        )

    await log_magi_rebalance_shadow(
        base_asset="HIVE",
        quote_asset="BTC",
        direction="sell",
        qty=Decimal(12),
        stage="binance_send",
        reason="OK",
        min_qty=Decimal(50),
        min_notional=Decimal(0),
        binance_order=order,
        preview=preview,
        fair_book=_fair,
    )

    record = _record(caplog)
    assert record.binance_sent is True
    assert Decimal(record.binance_sats_per_base) == Decimal(50)
    assert Decimal(record.edge_vs_binance_bps) == Decimal(2000)
    assert record.would_take_magi is True
    detail = record.magi_shadow_detail
    assert detail["router_check"]["reference"] == "binance_convert"
    assert detail["binance"]["order_id"] == "order-1"
    assert "ignored" not in detail["binance"]


@pytest.mark.asyncio
async def test_buy_quotes_the_btc_binance_spent(caplog):
    caplog.set_level(logging.INFO, logger="backend")
    order = _order(side="BUY", executed=Decimal(100), quote_qty=Decimal("0.00005000"))
    seen = {}

    async def preview(asset_in, asset_out, amount_in):
        seen["args"] = (asset_in, asset_out, amount_in)
        return _quote(
            side="BUY",
            amount_in=amount_in,
            amount_out=Decimal(125),
            sats_per_base=Decimal(40),
        )

    await log_magi_rebalance_shadow(
        base_asset="HIVE",
        quote_asset="BTC",
        direction="buy",
        qty=Decimal(100),
        stage="binance_send",
        reason="OK",
        min_qty=Decimal(50),
        min_notional=Decimal(0),
        binance_order=order,
        preview=preview,
        fair_book=_fair,
    )

    record = _record(caplog)
    assert seen["args"] == ("BTC", "HIVE", Decimal("0.00005000"))
    assert record.would_take_magi is True
    assert Decimal(record.edge_vs_binance_bps) == Decimal(2000)
    assert record.magi_shadow_detail["lower_is_better"] is True


@pytest.mark.asyncio
async def test_quote_error_is_logged_and_does_not_raise(caplog):
    caplog.set_level(logging.INFO, logger="backend")

    async def preview(asset_in, asset_out, amount_in):
        raise RuntimeError("indexer down")

    await log_magi_rebalance_shadow(
        base_asset="HIVE",
        quote_asset="BTC",
        direction="sell",
        qty=Decimal(12),
        stage="binance_send",
        reason="OK",
        min_qty=Decimal(50),
        min_notional=Decimal(0),
        preview=preview,
        fair_book=_fair,
    )

    record = _record(caplog)
    assert record.would_take_magi is False
    assert "indexer down" in record.magi_shadow_detail["error"]
    assert "indexer down" in record.message


@pytest.mark.asyncio
async def test_dash_pair_skips_magi(caplog):
    caplog.set_level(logging.INFO, logger="backend")
    called = {"preview": 0, "fair": 0}

    async def preview(asset_in, asset_out, amount_in):
        called["preview"] += 1
        return _quote(
            side="SELL",
            amount_in=amount_in,
            amount_out=Decimal(1),
            sats_per_base=Decimal(1),
        )

    async def fair():
        called["fair"] += 1
        return Decimal(1), Decimal(1)

    await log_magi_rebalance_shadow(
        base_asset="DASH",
        quote_asset="BTC",
        direction="sell",
        qty=Decimal("1.5"),
        stage="below_minimum",
        reason="below minimum",
        min_qty=Decimal("0.5"),
        min_notional=Decimal(0),
        preview=preview,
        fair_book=fair,
    )

    record = _record(caplog)
    assert called == {"preview": 0, "fair": 0}
    assert "unsupported_pair" in record.message
    assert record.would_take_magi is False


@pytest.mark.asyncio
async def test_zero_qty_does_not_log(caplog):
    caplog.set_level(logging.INFO, logger="backend")
    await log_magi_rebalance_shadow(
        base_asset="HIVE",
        quote_asset="BTC",
        direction="sell",
        qty=Decimal(0),
        stage="below_minimum",
        reason="empty",
        min_qty=Decimal(50),
        min_notional=Decimal(0),
        preview=_fail_if_called,
        fair_book=_fair,
    )
    assert not [record for record in caplog.records if getattr(record, "magi_shadow", False)]


async def _fail_if_called(*args, **kwargs):
    raise AssertionError("should not be called")


@pytest.mark.asyncio
async def test_shadow_helper_swallows_logger_failures():
    async def boom(**kwargs):
        raise RuntimeError("indexer down")

    # Patched at the call site inside the helper's module.
    from v4vapp_backend_v2.conversion import exchange_rebalance

    original = exchange_rebalance.log_magi_rebalance_shadow
    exchange_rebalance.log_magi_rebalance_shadow = boom
    try:
        await _log_magi_shadow(
            base_asset="HIVE",
            quote_asset="BTC",
            direction=exchange_rebalance.RebalanceDirection.SELL_BASE_FOR_QUOTE,
            qty=Decimal(12),
            stage="below_minimum",
            reason="below minimum",
            min_qty=Decimal(50),
            min_notional=Decimal(0),
            transaction_id="trx-1",
            exchange="binance_convert",
        )
    finally:
        exchange_rebalance.log_magi_rebalance_shadow = original


def test_edge_sign_and_quantize():
    assert edge_bps(Decimal(60), Decimal(50), lower_is_better=False) == Decimal(2000)
    assert edge_bps(Decimal(40), Decimal(50), lower_is_better=True) == Decimal(2000)
    assert quantize_asset(Decimal("12.3459"), "HIVE") == Decimal("12.345")
    assert quantize_asset(Decimal("0.000050009"), "BTC") == Decimal("0.00005000")
