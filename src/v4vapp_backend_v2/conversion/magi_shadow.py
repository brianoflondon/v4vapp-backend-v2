"""Read-only Magi quote beside a Binance rebalance decision.

Logs one JSON status line. Does not broadcast, accept a Convert quote, or
change whether Binance trades. A quote failure is data in that line.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from decimal import ROUND_DOWN, Decimal
from typing import Any

import httpx

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.conversion.exchange_protocol import ExchangeOrderResult
from v4vapp_backend_v2.conversion.magi_quote import (
    SATS_PER_BTC,
    VenueQuote,
    fair_sats_per_base,
    preview_swap,
)

# Labels for the log, matching the planned router. They do not route trades.
SHADOW_SLIPPAGE_BPS = 50
ROUTER_MIN_EDGE_BPS = 50
ROUTER_FEE_ABORT_BPS = 300
ROUTER_MAX_POOL_FRACTION = Decimal("0.10")
QUOTE_TIMEOUT_SECONDS = 8.0
BOOK_TIMEOUT_SECONDS = 5.0
BINANCE_BOOK = "https://api.binance.com/api/v3/ticker/bookTicker"

PreviewFn = Callable[[str, str, Decimal], Awaitable[VenueQuote | None]]
FairFn = Callable[[], Awaitable[tuple[Decimal, Decimal]]]


def _text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _signed_bps(value: Decimal | None) -> str | None:
    if value is None:
        return None
    text = format(value.quantize(Decimal("0.1")), "f")
    if value > 0 and not text.startswith("+"):
        return f"+{text}"
    return text


def quantize_asset(amount: Decimal, asset: str) -> Decimal:
    """Whole milli-units for HIVE/HBD, whole sats for BTC."""
    key = asset.upper()
    if key in ("HIVE", "HBD"):
        return amount.quantize(Decimal("0.001"), rounding=ROUND_DOWN)
    if key == "BTC":
        sats = (amount * Decimal(SATS_PER_BTC)).to_integral_value(rounding=ROUND_DOWN)
        return Decimal(int(sats)) / Decimal(SATS_PER_BTC)
    return amount


def edge_bps(candidate: Decimal, reference: Decimal, *, lower_is_better: bool) -> Decimal | None:
    """Positive means the candidate is the better price for this side."""
    if reference == 0:
        return None
    if lower_is_better:
        return (reference - candidate) / reference * Decimal(10_000)
    return (candidate - reference) / reference * Decimal(10_000)


def _book_mid(tickers: list[dict[str, Any]], symbol: str) -> Decimal:
    for ticker in tickers:
        if ticker.get("symbol") != symbol:
            continue
        bid = Decimal(str(ticker["bidPrice"]))
        ask = Decimal(str(ticker["askPrice"]))
        return (bid + ask) / Decimal(2)
    raise ValueError(f"Binance book has no {symbol}")


async def public_usdt_book() -> tuple[Decimal, Decimal]:
    """Public HIVEUSDT and BTCUSDT mids. No Convert quote and no account."""
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(BOOK_TIMEOUT_SECONDS, connect=3.0)
    ) as client:
        response = await client.get(BINANCE_BOOK, params={"symbols": '["HIVEUSDT","BTCUSDT"]'})
        response.raise_for_status()
        tickers = response.json()
    return _book_mid(tickers, "HIVEUSDT"), _book_mid(tickers, "BTCUSDT")


async def _preview_live(asset_in: str, asset_out: str, amount_in: Decimal) -> VenueQuote | None:
    return await preview_swap(asset_in, asset_out, amount_in, slippage_bps=SHADOW_SLIPPAGE_BPS)


async def _timed(awaitable: Awaitable[Any], seconds: float, label: str) -> tuple[Any, str | None]:
    try:
        return await asyncio.wait_for(awaitable, seconds), None
    except TimeoutError:
        return None, f"{label} timed out"
    except Exception as exc:
        return None, f"{label}: {exc}"


def _binance_sats_per_base(order: ExchangeOrderResult) -> Decimal | None:
    quote_asset = (order.quote_asset or "").upper()
    if quote_asset != "BTC" or order.executed_qty <= 0:
        return None
    return order.quote_qty * Decimal(SATS_PER_BTC) / order.executed_qty


def _reserve_fraction(quote: VenueQuote) -> Decimal | None:
    hops = quote.raw.get("hop_detail") or []
    if not hops:
        return None
    first = hops[0]
    reserve_in = first.get("reserve_in") or 0
    if reserve_in <= 0:
        return None
    return Decimal(first.get("amount_in") or 0) / Decimal(reserve_in)


def _router_check(
    quote: VenueQuote | None,
    *,
    reference: str | None,
    reference_edge: Decimal | None,
) -> dict[str, Any]:
    fraction = _reserve_fraction(quote) if quote is not None else None
    passes_abort = bool(quote is not None and not quote.raw.get("abort"))
    passes_fee = bool(quote is not None and quote.fee_bps <= ROUTER_FEE_ABORT_BPS)
    passes_size = bool(fraction is not None and fraction <= ROUTER_MAX_POOL_FRACTION)
    passes_edge = bool(
        reference_edge is not None and reference_edge >= Decimal(ROUTER_MIN_EDGE_BPS)
    )
    would_take = passes_abort and passes_fee and passes_size and passes_edge
    return {
        "min_edge_bps": ROUTER_MIN_EDGE_BPS,
        "fee_abort_bps": ROUTER_FEE_ABORT_BPS,
        "max_pool_fraction": _text(ROUTER_MAX_POOL_FRACTION),
        "reference": reference,
        "input_reserve_fraction": _text(fraction),
        "passes_abort": passes_abort,
        "passes_fee": passes_fee,
        "passes_size": passes_size,
        "passes_edge": passes_edge,
        "would_take_magi": would_take,
        "note": "Telemetry only. Positive edge means Magi is better for this side.",
    }


def _status_line(
    *,
    stage: str,
    direction: str,
    qty: Decimal,
    base_asset: str,
    quote_asset: str,
    magi_sats: str | None,
    edge_fair: str | None,
    edge_binance: str | None,
    fee_bps: int | None,
    hops: int | None,
    abort: str | None,
    would_take: bool | None,
    binance_sent: bool,
    error: str | None,
) -> str:
    head = f"Magi shadow {stage} {direction} {format(qty, 'f')} {base_asset}/{quote_asset}"
    if magi_sats is None:
        tail = error or "no Magi quote"
        return f"{head} {tail}"
    bits = [f"magi {magi_sats} sats/{base_asset}"]
    if edge_fair is not None:
        bits.append(f"vs fair {edge_fair} bps")
    if edge_binance is not None:
        bits.append(f"vs binance {edge_binance} bps")
    if fee_bps is not None:
        bits.append(f"fee {fee_bps} bps")
    if hops is not None:
        bits.append(f"hops {hops}")
    bits.append(f"abort {abort or '-'}")
    if would_take is not None:
        bits.append(f"would_take {str(would_take).lower()}")
    bits.append("binance_sent" if binance_sent else "binance_not_sent")
    return f"{head} {' '.join(bits)}"


async def log_magi_rebalance_shadow(
    *,
    base_asset: str,
    quote_asset: str,
    direction: str,
    qty: Decimal,
    stage: str,
    reason: str,
    min_qty: Decimal,
    min_notional: Decimal,
    transaction_id: str | None = None,
    exchange: str = "",
    binance_order: ExchangeOrderResult | None = None,
    binance_error: str | None = None,
    preview: PreviewFn | None = None,
    fair_book: FairFn | None = None,
) -> None:
    """Quote this size on Magi and write one analysis line. Never raises."""
    if qty <= 0:
        return

    preview_fn = preview or _preview_live
    fair_fn = fair_book or public_usdt_book
    base = base_asset.upper()
    quote = quote_asset.upper()
    side = str(direction)
    sell = side == "sell"
    lower_is_better = not sell
    binance_sent = stage == "binance_send" and binance_order is not None

    errors: list[str] = []
    if binance_error:
        errors.append(binance_error)

    magi_quote: VenueQuote | None = None
    fair_pair: tuple[Decimal, Decimal] | None = None
    quoted_amount_in: Decimal | None = None
    asset_in: str | None = None
    asset_out: str | None = None
    size_source: str | None = None

    supported = base in ("HIVE", "HBD") and quote == "BTC"
    if not supported:
        errors.append(f"unsupported_pair {base}/{quote}")
    else:
        try:
            fair_awaitable = fair_fn()
        except Exception as exc:
            fair_awaitable = None
            errors.append(f"fair book: {exc}")
        if fair_awaitable is not None:
            fair_pair, fair_error = await _timed(fair_awaitable, BOOK_TIMEOUT_SECONDS, "fair book")
            if fair_error:
                errors.append(fair_error)

        btc_in: Decimal | None = None
        if not sell:
            if (
                binance_order is not None
                and binance_order.quote_qty > 0
                and (binance_order.quote_asset or "").upper() == "BTC"
            ):
                btc_in = quantize_asset(binance_order.quote_qty, "BTC")
                size_source = "binance_fill"
            elif fair_pair is not None:
                hive_usdt, btc_usdt = fair_pair
                try:
                    fair_sats = fair_sats_per_base(base, hive_usdt, btc_usdt)
                    btc_in = quantize_asset(qty * fair_sats / Decimal(SATS_PER_BTC), "BTC")
                    size_source = "fair_usdt"
                except (ValueError, ArithmeticError) as exc:
                    errors.append(f"fair size: {exc}")
            else:
                errors.append("buy has no BTC size")

        if sell:
            quoted_amount_in = quantize_asset(qty, base)
            asset_in, asset_out = base, "BTC"
            size_source = "pending_qty"
        else:
            quoted_amount_in = btc_in
            asset_in, asset_out = "BTC", base

        if (
            quoted_amount_in is None
            or quoted_amount_in <= 0
            or asset_in is None
            or asset_out is None
        ):
            errors.append("amount rounds to zero")
        else:
            try:
                quote_awaitable = preview_fn(asset_in, asset_out, quoted_amount_in)
            except Exception as exc:
                quote_awaitable = None
                errors.append(f"Magi quote: {exc}")
            if quote_awaitable is not None:
                magi_quote, magi_error = await _timed(
                    quote_awaitable,
                    QUOTE_TIMEOUT_SECONDS,
                    "Magi quote",
                )
                if magi_error:
                    errors.append(magi_error)
                elif magi_quote is None:
                    errors.append("no Magi route")

    fair_sats_value: Decimal | None = None
    fair_detail: dict[str, Any] | None = None
    if fair_pair is not None and supported:
        hive_usdt, btc_usdt = fair_pair
        try:
            fair_sats_value = fair_sats_per_base(base, hive_usdt, btc_usdt)
            fair_detail = {
                "hive_usdt": _text(hive_usdt),
                "btc_usdt": _text(btc_usdt),
                "hbd_peg_usd": "1",
                "sats_per_base": _text(fair_sats_value),
            }
        except (ValueError, ArithmeticError) as exc:
            errors.append(f"fair value: {exc}")

    binance_sats = _binance_sats_per_base(binance_order) if binance_order is not None else None
    magi_sats = magi_quote.sats_per_base if magi_quote is not None else None
    fair_edge = (
        edge_bps(magi_sats, fair_sats_value, lower_is_better=lower_is_better)
        if magi_sats is not None and fair_sats_value is not None
        else None
    )
    binance_edge = (
        edge_bps(magi_sats, binance_sats, lower_is_better=lower_is_better)
        if magi_sats is not None and binance_sats is not None
        else None
    )
    if binance_sats is not None:
        reference = "binance_convert"
        reference_edge = binance_edge
    elif fair_sats_value is not None:
        reference = "fair_usdt"
        reference_edge = fair_edge
    else:
        reference = None
        reference_edge = None

    router = _router_check(magi_quote, reference=reference, reference_edge=reference_edge)
    would_take = bool(router["would_take_magi"]) if magi_quote is not None else False
    error_text = "; ".join(errors) or None

    magi_detail: dict[str, Any] | None = None
    if magi_quote is not None:
        magi_detail = {
            "venue": magi_quote.venue,
            "side": magi_quote.side,
            "asset_in": asset_in,
            "asset_out": asset_out,
            "size_source": size_source,
            "quoted_amount_in": _text(quoted_amount_in),
            "amount_in": _text(magi_quote.amount_in),
            "amount_out": _text(magi_quote.amount_out),
            "sats_per_base": _text(magi_quote.sats_per_base),
            "fee_bps": magi_quote.fee_bps,
            "min_amount_out": _text(magi_quote.min_amount_out),
            "pool_impact_pct": _text(magi_quote.pool_impact_pct),
            "hops": magi_quote.raw.get("hops"),
            "abort": magi_quote.raw.get("abort"),
            "aborts": magi_quote.raw.get("aborts") or [],
            "hop_detail": magi_quote.raw.get("hop_detail") or [],
            "slippage_bps": magi_quote.raw.get("slippage_bps", SHADOW_SLIPPAGE_BPS),
            "units_in": magi_quote.raw.get("units_in"),
            "units_out": magi_quote.raw.get("units_out"),
        }

    binance_detail: dict[str, Any] | None = None
    if binance_order is not None:
        binance_detail = {
            "sent": binance_sent,
            "exchange": binance_order.exchange,
            "side": binance_order.side,
            "order_id": binance_order.order_id,
            "status": binance_order.status,
            "executed_qty": _text(binance_order.executed_qty),
            "quote_qty": _text(binance_order.quote_qty),
            "sats_per_base": _text(binance_sats),
            "avg_price": _text(binance_order.avg_price),
        }

    detail = {
        "stage": stage,
        "exchange": exchange,
        "base_asset": base,
        "quote_asset": quote,
        "direction": side,
        "qty": _text(qty),
        "reason": reason,
        "min_qty": _text(min_qty),
        "min_notional": _text(min_notional),
        "transaction_id": transaction_id,
        "binance_sent": binance_sent,
        "binance_error": binance_error,
        "lower_is_better": lower_is_better,
        "edge_vs_fair_bps": _text(fair_edge),
        "edge_vs_binance_bps": _text(binance_edge),
        "fair": fair_detail,
        "magi": magi_detail,
        "binance": binance_detail,
        "router_check": router,
        "error": error_text,
    }
    message = _status_line(
        stage=stage,
        direction=side,
        qty=qty,
        base_asset=base,
        quote_asset=quote,
        magi_sats=_text(magi_sats),
        edge_fair=_signed_bps(fair_edge),
        edge_binance=_signed_bps(binance_edge),
        fee_bps=magi_quote.fee_bps if magi_quote is not None else None,
        hops=magi_quote.raw.get("hops") if magi_quote is not None else None,
        abort=magi_quote.raw.get("abort") if magi_quote is not None else None,
        would_take=would_take if magi_quote is not None else None,
        binance_sent=binance_sent,
        error=error_text,
    )
    logger.info(
        message,
        extra={
            "notification": False,
            "magi_shadow": True,
            "stage": stage,
            "base_asset": base,
            "quote_asset": quote,
            "direction": side,
            "qty": _text(qty),
            "transaction_id": transaction_id,
            "magi_sats_per_base": _text(magi_sats),
            "fair_sats_per_base": _text(fair_sats_value),
            "binance_sats_per_base": _text(binance_sats),
            "edge_vs_fair_bps": _text(fair_edge),
            "edge_vs_binance_bps": _text(binance_edge),
            "magi_fee_bps": magi_quote.fee_bps if magi_quote is not None else None,
            "magi_abort": magi_quote.raw.get("abort") if magi_quote is not None else None,
            "would_take_magi": would_take if magi_quote is not None else False,
            "binance_sent": binance_sent,
            "magi_shadow_detail": detail,
        },
    )
