"""Local quotes for the Magi DEX.

Integer math matches altera-app ``src/lib/pools/swapCalc.ts`` ``calculateSwap``
with ``SLIP_FEE_RULE = "off"``. The quote assumes the pendulum stabilizer is
at its cap (``m = 2``), so ``expected`` is a floor on what the contract pays.

``@vsc.eco/crosschain-core`` ``calculateSwap`` omits the stabilizer and the
CLP scale. Do not port that function.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

PROTOCOL_FEE_BPS = 8
STABILIZER_CAP_BPS = 20_000
BPS_SCALE = 10_000
CLP_SCALE_BPS = 625
FEE_ABORT_BPS = 300
SATS_PER_BTC = 100_000_000
ASSET_SCALE = {"HIVE": 1_000, "HBD": 1_000, "BTC": SATS_PER_BTC}
DEX_BASE = "hbd"
INDEXER_GRAPHQL = "https://indexer.magi.milohpr.com/v1/graphql"
POOL_CACHE_SECONDS = 5.0

POOL_QUERY = """{
  dex_pool_registry { pool_contract asset0 asset1 }
  dex_pool_liquidity { pool_contract reserve0 reserve1 }
}"""


class MagiQuoteError(Exception):
    """Indexer or quote input failed."""


@dataclass(frozen=True)
class PoolDepths:
    contract_id: str
    asset0: str
    asset1: str
    reserve0: int
    reserve1: int


@dataclass(frozen=True)
class SwapCalc:
    gross_out: int
    protocol_fee: int
    clp_fee: int
    total_fee: int
    expected_output: int
    min_amount_out: int
    slippage_bps: int
    fee_bps: int


@dataclass(frozen=True)
class SwapHop:
    pool: PoolDepths
    asset_in: str
    asset_out: str


class VenueQuote(BaseModel):
    """One venue's price for a size. Human amounts, not chain units."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    venue: str
    base_asset: str
    quote_asset: str
    side: str
    amount_in: Decimal
    amount_out: Decimal
    sats_per_base: Decimal
    fee_bps: int
    min_amount_out: Decimal | None = None
    pool_impact_pct: Decimal | None = None
    raw: dict[str, Any] = {}
    fetched_at: datetime


def to_units(amount: Decimal, asset: str) -> int:
    """Convert a human amount into the chain integer for ``asset``."""
    key = asset.upper()
    if key not in ASSET_SCALE:
        raise ValueError(f"unsupported asset {asset}")
    scaled = amount * Decimal(ASSET_SCALE[key])
    if scaled != scaled.to_integral_value():
        raise ValueError(f"{amount} {asset} is not a whole number of smallest units")
    units = int(scaled)
    if units < 0:
        raise ValueError("amount must be >= 0")
    return units


def from_units(units: int, asset: str) -> Decimal:
    """Convert a chain integer into a human amount."""
    key = asset.upper()
    if key not in ASSET_SCALE:
        raise ValueError(f"unsupported asset {asset}")
    return Decimal(units) / Decimal(ASSET_SCALE[key])


def calculate_swap(x: int, reserve_in: int, reserve_out: int, slippage_bps: int) -> SwapCalc:
    """Quote one pool hop. ``x`` and reserves are smallest units."""
    slip = max(0, min(int(slippage_bps), BPS_SCALE))
    if x <= 0 or reserve_in <= 0 or reserve_out <= 0:
        return SwapCalc(0, 0, 0, 0, 0, 0, slip, 0)

    new_x = reserve_in + x
    gross_out = reserve_out - (reserve_in * reserve_out) // new_x

    base_protocol = (gross_out * PROTOCOL_FEE_BPS) // BPS_SCALE
    if base_protocol == 0:
        base_protocol = 1
    base_clp = (x * x * reserve_out) // (new_x * new_x)
    if base_clp == 0:
        base_clp = 1

    protocol_fee = (base_protocol * STABILIZER_CAP_BPS) // BPS_SCALE
    clp_fee = (base_clp * CLP_SCALE_BPS) // BPS_SCALE
    total_fee = protocol_fee + clp_fee
    expected = gross_out - total_fee
    expected = max(expected, 0)
    min_out = (expected * (BPS_SCALE - slip)) // BPS_SCALE

    structural_protocol = ((gross_out * PROTOCOL_FEE_BPS) // BPS_SCALE) * (
        STABILIZER_CAP_BPS // BPS_SCALE
    )
    structural_clp = (x * x * reserve_out) // (new_x * new_x)
    structural = structural_protocol + (structural_clp * CLP_SCALE_BPS) // BPS_SCALE
    fee_bps = (structural * BPS_SCALE) // gross_out if gross_out > 0 else 0

    return SwapCalc(
        gross_out=gross_out,
        protocol_fee=protocol_fee,
        clp_fee=clp_fee,
        total_fee=total_fee,
        expected_output=expected,
        min_amount_out=min_out,
        slippage_bps=slip,
        fee_bps=fee_bps,
    )


def ordered_reserves(pool: PoolDepths, asset_in: str) -> tuple[int, int] | None:
    """Return ``(input reserve, output reserve)`` for ``asset_in``."""
    asset = asset_in.lower()
    if pool.asset0 == asset:
        return pool.reserve0, pool.reserve1
    if pool.asset1 == asset:
        return pool.reserve1, pool.reserve0
    return None


def find_pool(pools: list[PoolDepths], asset_a: str, asset_b: str) -> PoolDepths | None:
    pair = {asset_a.lower(), asset_b.lower()}
    for pool in pools:
        if {pool.asset0, pool.asset1} == pair:
            return pool
    return None


def build_route(pools: list[PoolDepths], asset_in: str, asset_out: str) -> list[SwapHop] | None:
    """Direct pool, or two hops through HBD. ``None`` when the pair cannot trade."""
    left = asset_in.lower()
    right = asset_out.lower()
    if not left or not right or left == right:
        return None
    direct = find_pool(pools, left, right)
    if direct is not None:
        return [SwapHop(direct, left, right)]
    if left == DEX_BASE or right == DEX_BASE:
        return None
    first = find_pool(pools, left, DEX_BASE)
    second = find_pool(pools, DEX_BASE, right)
    if first is None or second is None:
        return None
    return [
        SwapHop(first, left, DEX_BASE),
        SwapHop(second, DEX_BASE, right),
    ]


def _compound_impact(fractions: list[Decimal]) -> Decimal:
    survival = Decimal(1)
    for fraction in fractions:
        survival *= Decimal(1) - fraction
    return (Decimal(1) - survival) * Decimal(100)


def _pair_assets(asset_in: str, asset_out: str) -> tuple[str, str, str]:
    """Return ``(base, quote, side)`` with BTC as the quote when it is in the pair."""
    left = asset_in.upper()
    right = asset_out.upper()
    if right == "BTC":
        return left, "BTC", "SELL"
    if left == "BTC":
        return right, "BTC", "BUY"
    return left, right, "SELL"


def preview_from_pools(
    pools: list[PoolDepths],
    asset_in: str,
    asset_out: str,
    amount_in: Decimal,
    slippage_bps: int = 50,
    fetched_at: datetime | None = None,
) -> VenueQuote | None:
    """Quote ``amount_in`` of ``asset_in`` through ``pools``.

    Unknown pairs return ``None``. Abort reasons (``reserve``, ``fee``, ``zero``)
    stay on the quote in ``raw["abort"]`` so a caller can log the price it refused.
    """
    route = build_route(pools, asset_in, asset_out)
    if route is None:
        return None

    units_in = to_units(amount_in, asset_in)
    amount = units_in
    aborts: list[str] = []
    hops: list[dict[str, Any]] = []
    impacts: list[Decimal] = []
    fee_bps = 0
    min_units = 0

    for index, hop in enumerate(route):
        reserves = ordered_reserves(hop.pool, hop.asset_in)
        if reserves is None:
            return None
        reserve_in, reserve_out = reserves
        if amount * 2 > reserve_in:
            aborts.append("reserve")
        slip = slippage_bps if index == len(route) - 1 else 0
        calc = calculate_swap(amount, reserve_in, reserve_out, slip)
        fee_bps += calc.fee_bps
        if reserve_in + amount > 0:
            impacts.append(Decimal(amount) / Decimal(reserve_in + amount))
        hops.append(
            {
                "pool": hop.pool.contract_id,
                "asset_in": hop.asset_in,
                "asset_out": hop.asset_out,
                "amount_in": amount,
                "reserve_in": reserve_in,
                "reserve_out": reserve_out,
                "gross_out": calc.gross_out,
                "protocol_fee": calc.protocol_fee,
                "clp_fee": calc.clp_fee,
                "expected": calc.expected_output,
                "fee_bps": calc.fee_bps,
            }
        )
        min_units = calc.min_amount_out
        amount = calc.expected_output
        if amount <= 0:
            aborts.append("zero")
            break

    if fee_bps > FEE_ABORT_BPS:
        aborts.append("fee")

    base_asset, quote_asset, side = _pair_assets(asset_in, asset_out)
    out_asset = asset_out.upper()
    amount_out = from_units(amount, out_asset)
    min_amount_out = from_units(min_units, out_asset) if hops else None

    if side == "SELL" and quote_asset == "BTC":
        sats_per_base = Decimal(amount) / amount_in if amount_in else Decimal(0)
    elif side == "BUY" and quote_asset == "BTC":
        sats_in = to_units(amount_in, "BTC")
        sats_per_base = Decimal(sats_in) / amount_out if amount_out else Decimal(0)
    else:
        sats_per_base = Decimal(0)

    primary = next((name for name in ("reserve", "fee", "zero") if name in aborts), None)
    return VenueQuote(
        venue="MagiSwap",
        base_asset=base_asset,
        quote_asset=quote_asset,
        side=side,
        amount_in=amount_in,
        amount_out=amount_out,
        sats_per_base=sats_per_base,
        fee_bps=fee_bps,
        min_amount_out=min_amount_out,
        pool_impact_pct=_compound_impact(impacts) if impacts else None,
        raw={
            "abort": primary,
            "aborts": aborts,
            "hops": len(route),
            "hop_detail": hops,
            "units_in": units_in,
            "units_out": amount,
            "slippage_bps": slippage_bps,
        },
        fetched_at=fetched_at or datetime.now(UTC),
    )


def fair_sats_per_base(base_asset: str, hive_usdt: Decimal, btc_usdt: Decimal) -> Decimal:
    """USDT-triangle sats for one HIVE, or one HBD at the $1 peg used by ``QuoteResponse``."""
    if btc_usdt <= 0:
        raise ValueError("btc_usdt must be positive")
    asset = base_asset.upper()
    if asset == "HIVE":
        return hive_usdt / btc_usdt * Decimal(SATS_PER_BTC)
    if asset == "HBD":
        return Decimal(SATS_PER_BTC) / btc_usdt
    raise ValueError(f"fair value is defined for HIVE and HBD, not {base_asset}")


def sell_venue_quote(
    *,
    venue: str,
    base_asset: str,
    amount_in: Decimal,
    amount_out_btc: Decimal,
    fee_bps: int = 0,
    raw: dict[str, Any] | None = None,
    fetched_at: datetime | None = None,
) -> VenueQuote:
    """Build a sell quote from a BTC amount out. Used for fair value and Binance Convert."""
    if amount_in <= 0:
        raise ValueError("amount_in must be positive")
    sats = amount_out_btc * Decimal(SATS_PER_BTC)
    return VenueQuote(
        venue=venue,
        base_asset=base_asset.upper(),
        quote_asset="BTC",
        side="SELL",
        amount_in=amount_in,
        amount_out=amount_out_btc,
        sats_per_base=sats / amount_in,
        fee_bps=fee_bps,
        raw=raw or {},
        fetched_at=fetched_at or datetime.now(UTC),
    )


def fair_sell_quote(
    base_asset: str,
    amount_in: Decimal,
    hive_usdt: Decimal,
    btc_usdt: Decimal,
    fetched_at: datetime | None = None,
) -> VenueQuote:
    """Sell quote at the USDT triangle. Not executable."""
    sats_per = fair_sats_per_base(base_asset, hive_usdt, btc_usdt)
    amount_out_btc = amount_in * sats_per / Decimal(SATS_PER_BTC)
    return sell_venue_quote(
        venue="fair_usdt",
        base_asset=base_asset,
        amount_in=amount_in,
        amount_out_btc=amount_out_btc,
        raw={"hive_usdt": str(hive_usdt), "btc_usdt": str(btc_usdt), "hbd_peg_usd": "1"},
        fetched_at=fetched_at,
    )


def parse_pool_payload(data: dict[str, Any]) -> list[PoolDepths]:
    """Turn an indexer GraphQL ``data`` object into pool depths."""
    liquidity: dict[str, tuple[int, int]] = {}
    for row in data.get("dex_pool_liquidity") or []:
        contract_id = row.get("pool_contract")
        if not contract_id or row.get("reserve0") is None or row.get("reserve1") is None:
            continue
        liquidity[str(contract_id)] = (int(row["reserve0"]), int(row["reserve1"]))

    pools: list[PoolDepths] = []
    for row in data.get("dex_pool_registry") or []:
        contract_id = str(row.get("pool_contract") or "")
        reserves = liquidity.get(contract_id)
        if reserves is None:
            continue
        pools.append(
            PoolDepths(
                contract_id=contract_id,
                asset0=str(row.get("asset0") or "").lower(),
                asset1=str(row.get("asset1") or "").lower(),
                reserve0=reserves[0],
                reserve1=reserves[1],
            )
        )
    return pools


_pool_cache: tuple[float, list[PoolDepths]] | None = None


def clear_pool_cache() -> None:
    global _pool_cache
    _pool_cache = None


async def fetch_pools(
    endpoint: str = INDEXER_GRAPHQL,
    client: httpx.AsyncClient | None = None,
) -> list[PoolDepths]:
    """Load pool registry and reserves from the Magi indexer."""
    owns_client = client is None
    http = client or httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0))
    try:
        response = await http.post(endpoint, json={"query": POOL_QUERY})
        response.raise_for_status()
        body = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise MagiQuoteError(f"Magi indexer request failed: {exc}") from exc
    finally:
        if owns_client:
            await http.aclose()

    errors = body.get("errors")
    if errors:
        raise MagiQuoteError(f"Magi indexer GraphQL errors: {errors}")
    pools = parse_pool_payload(body.get("data") or {})
    if not pools:
        raise MagiQuoteError("Magi indexer returned no pools")
    return pools


async def get_pools(force: bool = False, endpoint: str = INDEXER_GRAPHQL) -> list[PoolDepths]:
    """Return indexer pools, reusing a short cache."""
    global _pool_cache
    now = time.monotonic()
    if not force and _pool_cache is not None and now - _pool_cache[0] < POOL_CACHE_SECONDS:
        return _pool_cache[1]
    pools = await fetch_pools(endpoint)
    _pool_cache = (now, pools)
    return pools


async def preview_swap(
    asset_in: str,
    asset_out: str,
    amount_in: Decimal,
    slippage_bps: int = 50,
    endpoint: str = INDEXER_GRAPHQL,
) -> VenueQuote | None:
    """Quote a swap from the live indexer. Does not broadcast."""
    pools = await get_pools(endpoint=endpoint)
    return preview_from_pools(pools, asset_in, asset_out, amount_in, slippage_bps)
