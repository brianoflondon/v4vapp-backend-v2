#!/usr/bin/env python3
"""Print Magi DEX quotes next to the USDT fair value.

Read-only. Hits the Magi indexer and the public Binance book. Does not
broadcast, and does not request a Binance Convert quote.

Usage:
    uv run python scripts/test_magi_swap_quote.py
    uv run python scripts/test_magi_swap_quote.py --hive 50 --hbd 10
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from decimal import Decimal

import httpx

sys.path.insert(0, "src")

from v4vapp_backend_v2.conversion.magi_quote import (
    MagiQuoteError,
    fair_sats_per_base,
    fetch_pools,
    preview_from_pools,
)

BINANCE_BOOK = "https://api.binance.com/api/v3/ticker/bookTicker"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare Magi DEX quotes to USDT fair value")
    parser.add_argument("--hive", type=Decimal, default=Decimal(50), help="HIVE size to sell")
    parser.add_argument("--hbd", type=Decimal, default=Decimal(10), help="HBD size to sell")
    parser.add_argument("--slippage-bps", type=int, default=50)
    return parser.parse_args()


def book_mid(tickers: list[dict], symbol: str) -> Decimal:
    for ticker in tickers:
        if ticker.get("symbol") != symbol:
            continue
        bid = Decimal(ticker["bidPrice"])
        ask = Decimal(ticker["askPrice"])
        return (bid + ask) / Decimal(2)
    raise MagiQuoteError(f"Binance book has no {symbol}")


async def public_book() -> tuple[Decimal, Decimal]:
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
        response = await client.get(BINANCE_BOOK, params={"symbols": '["HIVEUSDT","BTCUSDT"]'})
        response.raise_for_status()
        tickers = response.json()
    return book_mid(tickers, "HIVEUSDT"), book_mid(tickers, "BTCUSDT")


def edge_bps(magi: Decimal, fair: Decimal) -> Decimal:
    if fair == 0:
        return Decimal(0)
    return (magi - fair) / fair * Decimal(10_000)


def print_sell(label: str, quote, fair_sats: Decimal) -> None:
    if quote is None:
        print(f"  {label}: no route")
        return
    abort = quote.raw["abort"] or "-"
    print(
        f"  {label}: {quote.sats_per_base:.4f} sats  "
        f"out {quote.amount_out:.8f} BTC  "
        f"fee {quote.fee_bps} bps  "
        f"hops {quote.raw['hops']}  "
        f"abort {abort}  "
        f"fair {fair_sats:.4f}  "
        f"edge {edge_bps(quote.sats_per_base, fair_sats):+.1f} bps"
    )


async def main() -> None:
    args = parse_args()
    pools = await fetch_pools()
    hive_usdt, btc_usdt = await public_book()
    fair_hive = fair_sats_per_base("HIVE", hive_usdt, btc_usdt)
    fair_hbd = fair_sats_per_base("HBD", hive_usdt, btc_usdt)

    print(f"HIVEUSDT mid {hive_usdt}   BTCUSDT mid {btc_usdt}")
    print(f"fair {fair_hive:.4f} sats/HIVE   {fair_hbd:.4f} sats/HBD ($1 peg)")
    print("pools:")
    for pool in pools:
        if {pool.asset0, pool.asset1} <= {"hive", "hbd", "btc"}:
            print(
                f"  {pool.asset0}/{pool.asset1} {pool.contract_id}  "
                f"r0 {pool.reserve0}  r1 {pool.reserve1}"
            )

    print("sells:")
    for hive in (Decimal(1), Decimal(10), Decimal(20), args.hive, Decimal(100)):
        quote = preview_from_pools(pools, "HIVE", "BTC", hive, args.slippage_bps)
        print_sell(f"{hive} HIVE", quote, fair_hive)
    for hbd in (Decimal(1), args.hbd, Decimal(50)):
        quote = preview_from_pools(pools, "HBD", "BTC", hbd, args.slippage_bps)
        print_sell(f"{hbd} HBD", quote, fair_hbd)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except MagiQuoteError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)
