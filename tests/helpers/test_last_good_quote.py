"""A failed quote fetch must not replace the last good price with zero."""

import pickle
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion, PricingUnavailableError
from v4vapp_backend_v2.helpers.crypto_prices import (
    GLOBAL_QUOTE_REDIS_KEY,
    LAST_GOOD_QUOTE_MAX_AGE_S,
    LAST_GOOD_QUOTE_REDIS_KEY,
    AllQuotes,
    Binance,
    CoinGecko,
    CoinMarketCap,
    HiveInternalMarket,
    QuoteResponse,
    quote_is_usable,
    save_last_good_quote,
)
from v4vapp_backend_v2.helpers.currency_class import Currency


class MemoryRedis:
    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}

    def get(self, key: str):
        return self.store.get(key)

    def set(self, name: str, value: bytes, ex: int | None = None):
        self.store[name] = value
        return True

    def close(self) -> None:
        return None

    def ping(self) -> bool:
        return True


def _good_quote(source: str = "Binance", age_s: float = 0) -> QuoteResponse:
    return QuoteResponse(
        hive_usd=Decimal("0.25"),
        hbd_usd=Decimal("1.01"),
        btc_usd=Decimal("80000"),
        hive_hbd=Decimal("0.2475"),
        source=source,
        fetch_date=datetime.now(tz=UTC) - timedelta(seconds=age_s),
        error="",
    )


@pytest.fixture
def memory_redis(monkeypatch: pytest.MonkeyPatch) -> MemoryRedis:
    redis = MemoryRedis()

    def _keep_test_redis(self) -> None:
        InternalConfig.redis = redis

    # InternalConfig() replaces the client inside setup_redis. Keep the test store.
    monkeypatch.setattr(InternalConfig, "setup_redis", _keep_test_redis)
    monkeypatch.setattr(InternalConfig, "redis", redis)
    return redis


def _patch_sources(mocker, quote_for):
    async def fake_get_quote(self, use_cache=True):
        return quote_for(type(self).__name__)

    for cls in (CoinGecko, Binance, CoinMarketCap, HiveInternalMarket):
        mocker.patch.object(cls, "get_quote", fake_get_quote)


@pytest.mark.asyncio
async def test_failed_fetch_does_not_overwrite_a_good_cache(mocker, memory_redis: MemoryRedis):
    def quote_for(name: str) -> QuoteResponse:
        return _good_quote(name)

    _patch_sources(mocker, quote_for)
    first = AllQuotes()
    await first.get_all_quotes(use_cache=False, store_db=False)
    assert quote_is_usable(first.quote)
    assert first.quote.stale is False
    assert GLOBAL_QUOTE_REDIS_KEY in memory_redis.store
    assert LAST_GOOD_QUOTE_REDIS_KEY in memory_redis.store
    stored_global = memory_redis.store[GLOBAL_QUOTE_REDIS_KEY]
    stored_last_good = memory_redis.store[LAST_GOOD_QUOTE_REDIS_KEY]

    def failed(name: str) -> QuoteResponse:
        return QuoteResponse(source=name, error="Service timeout after 15 seconds")

    _patch_sources(mocker, failed)
    second = AllQuotes()
    await second.get_all_quotes(use_cache=False, store_db=False)

    assert memory_redis.store[GLOBAL_QUOTE_REDIS_KEY] == stored_global
    assert memory_redis.store[LAST_GOOD_QUOTE_REDIS_KEY] == stored_last_good
    assert second.quote.stale is True
    assert quote_is_usable(second.quote)
    assert second.quote.btc_usd == Decimal("80000")
    assert second.legacy_api_format()["stale"] is True
    assert second.legacy_api_format()["quote_age_s"] >= 0
    assert second.legacy_api_format()["BTC_USD"] == Decimal("80000")


@pytest.mark.asyncio
async def test_last_good_past_the_limit_is_not_usable(mocker, memory_redis: MemoryRedis):
    save_last_good_quote(_good_quote(age_s=LAST_GOOD_QUOTE_MAX_AGE_S + 60))

    def failed(name: str) -> QuoteResponse:
        return QuoteResponse(source=name, error="Network is unreachable")

    _patch_sources(mocker, failed)
    all_quotes = AllQuotes()
    await all_quotes.get_all_quotes(use_cache=False, store_db=False)

    assert all_quotes.quote.stale is True
    assert all_quotes.quote.btc_usd == Decimal("80000")
    assert quote_is_usable(all_quotes.quote) is False
    assert "pricing temporarily unavailable" in all_quotes.quote.error
    assert GLOBAL_QUOTE_REDIS_KEY not in memory_redis.store
    with pytest.raises(PricingUnavailableError, match="pricing temporarily unavailable"):
        CryptoConversion(conv_from=Currency.SATS, value=1000, quote=all_quotes.quote)


@pytest.mark.asyncio
async def test_no_last_good_quote_refuses_a_zero_conversion(mocker, memory_redis: MemoryRedis):
    def failed(name: str) -> QuoteResponse:
        return QuoteResponse(source=name, error="down")

    _patch_sources(mocker, failed)
    all_quotes = AllQuotes()
    await all_quotes.get_all_quotes(use_cache=False, store_db=False)

    assert all_quotes.quote.error == "No valid quote found"
    assert all_quotes.quote.btc_usd == 0
    assert memory_redis.store == {}
    with pytest.raises(PricingUnavailableError):
        CryptoConversion(conv_from=Currency.HIVE, value=Decimal("10"), quote=all_quotes.quote)


def test_zero_quote_does_not_compute_zero_usd():
    with pytest.raises(PricingUnavailableError):
        CryptoConversion(conv_from=Currency.USD, value=Decimal("5"), quote=QuoteResponse())


def test_in_limit_last_good_still_converts(memory_redis: MemoryRedis):
    save_last_good_quote(_good_quote(age_s=30))
    raw = memory_redis.store[LAST_GOOD_QUOTE_REDIS_KEY]
    payload = pickle.loads(raw)
    assert "fetched_at" in payload
    quote = AllQuotes()._fallback_last_good()
    assert quote.stale is True
    conv = CryptoConversion(conv_from=Currency.SATS, value=1000, quote=quote)
    assert conv.usd > 0
    assert conv.hive > 0
    assert conv.hbd > 0
