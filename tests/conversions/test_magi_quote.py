"""Integer quotes for the Magi DEX, matched to altera-app swapCalc.ts rule off."""

from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from v4vapp_backend_v2.conversion.magi_quote import (
    FEE_ABORT_BPS,
    PoolDepths,
    calculate_swap,
    clear_pool_cache,
    fair_sats_per_base,
    fair_sell_quote,
    fetch_pools,
    get_pools,
    parse_pool_payload,
    preview_from_pools,
    sell_venue_quote,
)

FIXED_AT = datetime(2026, 11, 23, tzinfo=UTC)

# Indexer reserves observed 2026-11-23.
HIVE_HBD = PoolDepths(
    "vsc1BoaniA5HW56GuQy6pVdoZfMcVaaDfnC8kp",
    "hbd",
    "hive",
    2_270_294,
    61_375_943,
)
BTC_HBD = PoolDepths(
    "vsc1BVb95YKRHAEy24XgRSaW4L6d9vB88AdwjM",
    "btc",
    "hbd",
    2_350_142,
    1_526_257,
)
LIVE_POOLS = [HIVE_HBD, BTC_HBD]


def test_calculate_swap_matches_altera_fixture():
    """100 HBD into the published pre-swap BTC pool, slippage 100 bps."""
    result = calculate_swap(100_000, 504_685, 661_105, 100)

    assert result.gross_out == 109_331
    assert result.protocol_fee == 174
    assert result.clp_fee == 1_130
    assert result.total_fee == 1_304
    assert result.expected_output == 108_027
    assert result.min_amount_out == 106_946
    assert result.fee_bps == 119


def test_calculate_swap_floors_dust_fees_without_inflating_fee_bps():
    """The 1-unit fee floor is charged, and the guard ignores it."""
    result = calculate_swap(1, 10**12, 10**12, 0)

    assert result.protocol_fee == 2
    assert result.clp_fee == 0
    assert result.fee_bps == 0


def test_calculate_swap_rejects_empty_reserves():
    result = calculate_swap(100, 0, 100, 50)
    assert result.expected_output == 0
    assert result.fee_bps == 0


@pytest.mark.parametrize(
    "hive, sats",
    [
        (Decimal(1), 52),
        (Decimal(10), 565),
        (Decimal(50), 2836),
        (Decimal(100), 5659),
    ],
)
def test_hive_sell_routes_through_hbd(hive: Decimal, sats: int):
    quote = preview_from_pools(
        LIVE_POOLS, "HIVE", "BTC", hive, slippage_bps=50, fetched_at=FIXED_AT
    )

    assert quote is not None
    assert quote.venue == "MagiSwap"
    assert quote.side == "SELL"
    assert quote.base_asset == "HIVE"
    assert quote.raw["hops"] == 2
    assert quote.raw["abort"] is None
    assert quote.raw["units_out"] == sats
    assert quote.sats_per_base == Decimal(sats) / hive


@pytest.mark.parametrize(
    "hbd, sats",
    [
        (Decimal(1), 1537),
        (Decimal(10), 15268),
        (Decimal(50), 74284),
    ],
)
def test_hbd_sell_is_one_hop(hbd: Decimal, sats: int):
    quote = preview_from_pools(LIVE_POOLS, "HBD", "BTC", hbd, slippage_bps=50, fetched_at=FIXED_AT)

    assert quote is not None
    assert quote.raw["hops"] == 1
    assert quote.raw["abort"] is None
    assert quote.raw["units_out"] == sats
    assert quote.quote_asset == "BTC"


def test_unknown_pair_returns_empty():
    assert preview_from_pools(LIVE_POOLS, "HIVE", "LASSECASH", Decimal(1)) is None


def test_reserve_abort_when_input_exceeds_half_the_reserve():
    shallow = [
        PoolDepths("hive-hbd", "hbd", "hive", 1_000_000, 1_000_000),
        PoolDepths("btc-hbd", "btc", "hbd", 5_000_000, 5_000_000),
    ]
    quote = preview_from_pools(shallow, "HIVE", "BTC", Decimal(600), slippage_bps=50)

    assert quote is not None
    assert quote.raw["abort"] == "reserve"
    assert "reserve" in quote.raw["aborts"]


def test_fee_abort_when_two_hops_sum_past_300_bps():
    """Each hop sits at half the input reserve, so only the summed fee trips."""
    pools = [
        PoolDepths("hive-hbd", "hive", "hbd", 1_000_000, 1_000_000),
        PoolDepths("btc-hbd", "hbd", "btc", 651_716, 2_000_000),
    ]
    quote = preview_from_pools(pools, "HIVE", "BTC", Decimal(500), slippage_bps=0)

    assert quote is not None
    assert quote.fee_bps > FEE_ABORT_BPS
    assert quote.raw["abort"] == "fee"
    assert "reserve" not in quote.raw["aborts"]


def test_buy_reports_sats_spent_per_base_received():
    quote = preview_from_pools(
        LIVE_POOLS, "BTC", "HIVE", Decimal("0.00002836"), slippage_bps=50, fetched_at=FIXED_AT
    )

    assert quote is not None
    assert quote.side == "BUY"
    assert quote.base_asset == "HIVE"
    assert quote.amount_out > 0
    assert quote.sats_per_base == Decimal(2836) / quote.amount_out


def test_fair_sell_uses_usdt_triangle_and_hbd_peg():
    hive = fair_sats_per_base("HIVE", Decimal("0.03515"), Decimal("62643.145"))
    hbd = fair_sats_per_base("HBD", Decimal("0.03515"), Decimal("62643.145"))

    assert hive == Decimal("0.03515") / Decimal("62643.145") * Decimal(100_000_000)
    assert hbd == Decimal(100_000_000) / Decimal("62643.145")

    quote = fair_sell_quote("HIVE", Decimal(50), Decimal("0.03515"), Decimal("62643.145"))
    assert quote.venue == "fair_usdt"
    assert quote.sats_per_base == hive
    assert quote.amount_out * Decimal(100_000_000) == Decimal(50) * hive


def test_sell_venue_quote_from_btc_out():
    quote = sell_venue_quote(
        venue="binance_convert",
        base_asset="HIVE",
        amount_in=Decimal(50),
        amount_out_btc=Decimal("0.00002800"),
    )
    assert quote.sats_per_base == Decimal(2800) / Decimal(50)
    assert quote.side == "SELL"


def test_parse_pool_payload_joins_registry_and_liquidity():
    pools = parse_pool_payload(
        {
            "dex_pool_registry": [
                {"pool_contract": "pool-a", "asset0": "HBD", "asset1": "HIVE"},
                {"pool_contract": "pool-missing", "asset0": "eth", "asset1": "hbd"},
            ],
            "dex_pool_liquidity": [
                {"pool_contract": "pool-a", "reserve0": 10, "reserve1": 20},
            ],
        }
    )
    assert pools == [PoolDepths("pool-a", "hbd", "hive", 10, 20)]


async def test_fetch_pools_reads_graphql_data():
    payload = {
        "data": {
            "dex_pool_registry": [{"pool_contract": "pool-a", "asset0": "btc", "asset1": "hbd"}],
            "dex_pool_liquidity": [{"pool_contract": "pool-a", "reserve0": 5, "reserve1": 6}],
        }
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/v1/graphql")
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        pools = await fetch_pools("https://indexer.example/v1/graphql", client=client)

    assert pools == [PoolDepths("pool-a", "btc", "hbd", 5, 6)]


async def test_get_pools_uses_cache(monkeypatch):
    clear_pool_cache()
    calls = {"n": 0}

    async def fake_fetch(endpoint: str = "") -> list[PoolDepths]:
        calls["n"] += 1
        return LIVE_POOLS

    monkeypatch.setattr("v4vapp_backend_v2.conversion.magi_quote.fetch_pools", fake_fetch)
    first = await get_pools()
    second = await get_pools()

    assert first == second == LIVE_POOLS
    assert calls["n"] == 1
    clear_pool_cache()
