"""Switches that decide which price services AllQuotes.get_all_quotes calls."""

import json

import pytest

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.helpers.crypto_prices import (
    QUOTE_SERVICES_REDIS_KEY,
    enabled_quote_service_clients,
    publish_quote_service_flags,
    read_quote_service_flags,
)
from v4vapp_backend_v2.hive.v4v_config import (
    QUOTE_SERVICE_NAMES,
    V4VConfig,
    V4VConfigData,
    carry_quote_services,
    default_quote_service_flags,
    normalize_quote_service_flags,
)


def test_normalize_quote_service_flags_fills_missing_and_drops_unknown():
    flags = normalize_quote_service_flags(
        {"CoinGecko": False, "Binance": "false", "CoinMarketCap": "on", "extra": False}
    )
    assert flags["CoinGecko"] is False
    assert flags["Binance"] is False
    assert flags["CoinMarketCap"] is True
    assert flags["HiveInternalMarket"] is True
    assert "extra" not in flags
    assert normalize_quote_service_flags(None) == default_quote_service_flags()
    assert normalize_quote_service_flags("nope") == default_quote_service_flags()


def test_legacy_hive_config_keeps_every_quote_service_on():
    data = V4VConfigData.model_validate({"hive_return_fee": "0.002"})
    assert data.quote_services == default_quote_service_flags()

    partial = V4VConfigData.model_validate({"quote_services": {"CoinGecko": False}})
    assert partial.quote_services["CoinGecko"] is False
    assert partial.quote_services["Binance"] is True
    assert set(partial.quote_services) == set(QUOTE_SERVICE_NAMES)


def test_carry_quote_services_keeps_a_disabled_source():
    existing = V4VConfigData.model_validate({"quote_services": {"CoinGecko": False}})
    updated = carry_quote_services(existing, V4VConfigData())
    assert updated.quote_services["CoinGecko"] is False
    updated.quote_services["Binance"] = False
    assert existing.quote_services["Binance"] is True


def test_read_quote_service_flags_prefers_redis(monkeypatch):
    class FakeRedis:
        def get(self, key):
            assert key == QUOTE_SERVICES_REDIS_KEY
            return b'{"CoinGecko": false, "Binance": true}'

    monkeypatch.setattr(InternalConfig, "redis", FakeRedis(), raising=False)
    monkeypatch.setattr(V4VConfig, "_instance", object())

    flags = read_quote_service_flags()
    assert flags["CoinGecko"] is False
    assert flags["HiveInternalMarket"] is True


def test_read_quote_service_flags_falls_back_to_loaded_config(monkeypatch):
    class EmptyRedis:
        def get(self, key):
            return None

    class Inst:
        data = V4VConfigData.model_validate({"quote_services": {"CoinGecko": False}})

    monkeypatch.setattr(InternalConfig, "redis", EmptyRedis(), raising=False)
    monkeypatch.setattr(V4VConfig, "_instance", Inst())
    assert read_quote_service_flags()["CoinGecko"] is False


def test_invalid_redis_payload_uses_loaded_config(monkeypatch):
    class BadRedis:
        def get(self, key):
            return b"not-json"

    class Inst:
        data = V4VConfigData.model_validate({"quote_services": {"CoinGecko": False}})

    monkeypatch.setattr(InternalConfig, "redis", BadRedis(), raising=False)
    monkeypatch.setattr(V4VConfig, "_instance", Inst())
    assert read_quote_service_flags()["CoinGecko"] is False


def test_missing_config_leaves_services_enabled(monkeypatch):
    monkeypatch.setattr(InternalConfig, "redis", None, raising=False)
    monkeypatch.setattr(V4VConfig, "_instance", None)
    assert read_quote_service_flags() == default_quote_service_flags()
    names = [type(client).__name__ for client in enabled_quote_service_clients()]
    assert names == list(QUOTE_SERVICE_NAMES)


def test_publish_quote_service_flags_can_avoid_overwrite(monkeypatch):
    class RecordingRedis:
        def __init__(self):
            self.calls = []

        def set(self, key, value, nx=False):
            self.calls.append((key, value, nx))

    redis = RecordingRedis()
    monkeypatch.setattr(InternalConfig, "redis", redis, raising=False)
    publish_quote_service_flags({"CoinGecko": False}, overwrite=False)
    publish_quote_service_flags({"CoinGecko": False}, overwrite=True)

    assert redis.calls[0][0] == QUOTE_SERVICES_REDIS_KEY
    assert redis.calls[0][2] is True
    assert redis.calls[1][2] is False
    assert json.loads(redis.calls[0][1])["CoinGecko"] is False
    assert json.loads(redis.calls[0][1])["Binance"] is True


def test_publish_quote_service_flags_ignores_missing_redis(monkeypatch):
    monkeypatch.setattr(InternalConfig, "redis", None, raising=False)
    publish_quote_service_flags({"CoinGecko": False})


@pytest.mark.asyncio
async def test_update_quote_services_pushes_redis_when_hive_save_fails(monkeypatch):
    published = []

    def fake_publish(flags, overwrite=True):
        published.append((dict(flags), overwrite))

    monkeypatch.setattr(
        "v4vapp_backend_v2.helpers.crypto_prices.publish_quote_service_flags",
        fake_publish,
    )
    config = object.__new__(V4VConfig)
    config.data = V4VConfigData()
    config.server_accname = "devser.v4vapp"

    async def fake_put(hive_client=None):
        return False

    config.put = fake_put
    saved = await config.update_quote_services({"CoinGecko": False})

    assert saved is False
    assert config.data.quote_services["CoinGecko"] is False
    assert config.data.quote_services["Binance"] is True
    assert published == [(config.data.quote_services, True)]


@pytest.mark.asyncio
async def test_update_quote_services_rejects_all_off():
    config = object.__new__(V4VConfig)
    config.data = V4VConfigData()

    async def fail_put(hive_client=None):
        raise AssertionError("Hive should not be written when every service is off")

    config.put = fail_put
    with pytest.raises(ValueError, match="At least one quote service"):
        await config.update_quote_services({name: False for name in QUOTE_SERVICE_NAMES})
