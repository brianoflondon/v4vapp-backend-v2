from decimal import Decimal

import httpx
import pytest

from v4vapp_backend_v2.dash.amounts import msats_to_sats, rpc_dash_to_duffs
from v4vapp_backend_v2.dash.errors import ApiError
from v4vapp_backend_v2.dash.quotes.service import (
    duffs_from_sats,
    fetch_quote,
    quote_for_sats,
    reset_quote_cache,
)


def test_worked_example_25000_sats() -> None:
    duffs = duffs_from_sats(25_000, Decimal("0.0005"))
    assert duffs == 50_000_000


def test_ceil_rounding() -> None:
    assert duffs_from_sats(1, Decimal("0.0005")) == 2000
    assert duffs_from_sats(3, Decimal("0.0007")) == 4286


def test_rpc_dash_float_to_duffs() -> None:
    assert rpc_dash_to_duffs("0.50000000") == Decimal(50_000_000)
    assert rpc_dash_to_duffs(0.5) == Decimal(50_000_000)


def test_msats_to_sats_rounds_up() -> None:
    assert msats_to_sats("25000500") == Decimal(25001)
    assert msats_to_sats(1000) == Decimal(1)
    assert msats_to_sats(1001) == Decimal(2)


def test_fetch_quote_coingecko_and_cache() -> None:
    reset_quote_cache()
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(
            200,
            json={
                "dash": {"btc": 0.0005, "usd": 32.5},
                "bitcoin": {"btc": 1, "usd": 65000},
            },
        )

    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport)
    quote = fetch_quote(client=client, force=True)
    assert quote.source == "coingecko"
    assert quote.dash_btc == Decimal("0.0005")
    priced = quote_for_sats(25_000, quote)
    assert priced.duffs_quoted == 50_000_000
    assert priced.dash_quoted == "0.50000000"

    fetch_quote(client=client, force=False)
    assert calls["n"] == 1


def test_fetch_quote_unavailable() -> None:
    reset_quote_cache()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="down")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    with pytest.raises(ApiError) as exc:
        fetch_quote(client=client, force=True)
    assert exc.value.code == "quote_unavailable"
    assert exc.value.status_code == 422
