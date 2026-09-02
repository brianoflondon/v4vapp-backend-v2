from datetime import UTC, datetime
from decimal import Decimal

import pytest

from v4vapp_backend_v2.conversion.binance_swap_adapter import (
    BinanceSwapAdapter,
    ExchangeConnectionError,
    ExchangeMinimums,
)
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse


class DummyClient:
    def __init__(self, pairs):
        self._pairs = pairs

    def list_all_convert_pairs(self, fromAsset, toAsset):
        # return same list regardless of parameters for simplicity
        return self._pairs


def test_min_qty_override(monkeypatch):
    """Verify that very small min_qty values are bumped up by the override."""
    adapter = BinanceSwapAdapter(testnet=False)

    # prepare client that reports an impractically low minimum
    tiny = Decimal("0.00000001")
    dummy_pairs = [
        {
            "fromAsset": "HIVE",
            "toAsset": "BTC",
            "fromAssetMinAmount": str(tiny),
            "toAssetMinAmount": "0",
        }
    ]

    monkeypatch.setattr(adapter, "_get_client", lambda: DummyClient(dummy_pairs))

    mins = adapter.get_min_order_requirements("HIVE", "BTC")

    # override defined in adapter should raise min_qty to at least 50
    assert isinstance(mins, ExchangeMinimums)
    assert mins.min_qty >= Decimal(50), "min_qty should be overridden to 50 HIVE"


def test_min_qty_no_override_for_other_asset(monkeypatch):
    """When base asset isn't configured for override, reported minimum should pass through."""
    adapter = BinanceSwapAdapter(testnet=False)

    reported = Decimal("0.001")
    dummy_pairs = [
        {
            "fromAsset": "ABC",
            "toAsset": "BTC",
            "fromAssetMinAmount": str(reported),
            "toAssetMinAmount": "0",
        }
    ]
    monkeypatch.setattr(adapter, "_get_client", lambda: DummyClient(dummy_pairs))

    mins = adapter.get_min_order_requirements("ABC", "BTC")
    assert mins.min_qty == reported


def test_execute_swap_empty_quote_id_raises(monkeypatch):
    adapter = BinanceSwapAdapter(testnet=False)

    class DummyClientNoQuote(DummyClient):
        def send_quote_request(self, *args, **kwargs):
            return {
                # missing quoteId intentionally to simulate service data issue
                "ratio": "1",
                "inverseRatio": "1",
                "validTimestamp": 10_000_000_000,
                "fromAmount": "100",
                "toAmount": "0.001",
            }

    monkeypatch.setattr(adapter, "_get_client", lambda: DummyClientNoQuote([]))

    with pytest.raises(ExchangeConnectionError, match="missing quoteId"):
        adapter.market_sell("HIVE", "BTC", Decimal(100))


def test_min_qty_override_for_dash(monkeypatch):
    adapter = BinanceSwapAdapter(testnet=False)
    dummy_pairs = [
        {
            "fromAsset": "DASH",
            "toAsset": "BTC",
            "fromAssetMinAmount": "0.0001",
            "toAssetMinAmount": "0",
        }
    ]
    monkeypatch.setattr(adapter, "_get_client", lambda: DummyClient(dummy_pairs))
    mins = adapter.get_min_order_requirements("DASH", "BTC")
    assert mins.min_qty >= Decimal("0.5")


def test_build_trade_quote_dash_btc(monkeypatch):
    market = QuoteResponse(
        hive_usd=Decimal("0.24"),
        hbd_usd=Decimal(1),
        btc_usd=Decimal(83000),
        hive_hbd=Decimal("0.24"),
        dash_usd=Decimal(30),
        source="mkt",
        fetch_date=datetime.now(tz=UTC),
    )

    class FakeAllQuotes:
        quote = market

        async def get_all_quotes(self, *args, **kwargs):
            return None

    monkeypatch.setattr(
        "v4vapp_backend_v2.helpers.crypto_prices.AllQuotes",
        FakeAllQuotes,
    )
    adapter = BinanceSwapAdapter(testnet=False)
    ratio = Decimal("0.000391566")
    quote = adapter._build_trade_quote("DASH", "BTC", ratio)
    assert abs(quote.dash_usd - ratio * Decimal(83000)) < Decimal("0.01")
    assert quote.hive_usd == market.hive_usd
    assert quote.btc_usd == Decimal(83000)


def test_build_trade_quote_btc_dash(monkeypatch):
    market = QuoteResponse(
        hive_usd=Decimal("0.24"),
        hbd_usd=Decimal(1),
        btc_usd=Decimal(83000),
        hive_hbd=Decimal("0.24"),
        dash_usd=Decimal(30),
        source="mkt",
        fetch_date=datetime.now(tz=UTC),
    )

    class FakeAllQuotes:
        quote = market

        async def get_all_quotes(self, *args, **kwargs):
            return None

    monkeypatch.setattr(
        "v4vapp_backend_v2.helpers.crypto_prices.AllQuotes",
        FakeAllQuotes,
    )
    adapter = BinanceSwapAdapter(testnet=False)
    ratio = Decimal("2553.7")
    quote = adapter._build_trade_quote("BTC", "DASH", ratio)
    expected = (Decimal(1) / ratio) * Decimal(83000)
    assert abs(quote.dash_usd - expected) < Decimal("0.01")
