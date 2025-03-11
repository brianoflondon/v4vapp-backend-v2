import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest
from redis.exceptions import ConnectionError

from v4vapp_backend_v2.database.async_redis import V4VAsyncRedis


@pytest.fixture(autouse=True)
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path
    )
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    with patch(
        "v4vapp_backend_v2.config.mylogger.NotificationProtocol.send_notification",
        lambda self, message, record, alert_level=1: None,
    ):
        yield
    # Unpatch the monkeypatch
    monkeypatch.undo()


@pytest.fixture(autouse=True)
def reset_internal_config(monkeypatch: pytest.MonkeyPatch):
    # Reset the singleton instance before each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    # Reset the singleton instance after each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


@pytest.mark.asyncio
async def test_redis_client():
    redis_client = V4VAsyncRedis(decode_responses=True)
    await redis_client.flush()
    assert redis_client is not None
    assert redis_client.redis is not None
    assert await redis_client.redis.ping()
    await redis_client.redis.set("test_key", "test_value")
    assert await redis_client.redis.get("test_key") == "test_value"
    assert await redis_client.redis.delete("test_key")


@pytest.mark.asyncio
async def test_redis_client_with_connection_str():
    redis_client = V4VAsyncRedis(
        connection_str="redis://localhost:6379/0", decode_responses=True
    )
    assert redis_client is not None


@pytest.mark.asyncio
async def test_redis_client_with_kwargs():
    redis_client = V4VAsyncRedis(
        host="localhost", port=6379, db=0, decode_responses=False
    )
    assert redis_client is not None
    assert redis_client.redis is not None
    assert await redis_client.redis.ping()
    await redis_client.redis.setex("test_key", 1, "test_value")
    assert await redis_client.redis.get("test_key") == b"test_value"
    await asyncio.sleep(1.001)
    assert await redis_client.redis.get("test_key") is None


@pytest.mark.asyncio
async def test_redis_client_context_manager():
    async with V4VAsyncRedis(
        decode_responses=True,
        host="localhost",
        port="6379",
        db=0,
    ) as redis_client:
        assert redis_client is not None
        assert await redis_client.ping()
        await redis_client.set("test_key", "test_value")
        assert await redis_client.get("test_key") == "test_value"
        await redis_client.flushdb()

    assert redis_client is not None
    await redis_client.aclose()


@pytest.mark.asyncio
async def test_redis_client_context_manager_with_bad_connection():
    with pytest.raises(ConnectionError):
        async with V4VAsyncRedis(
            decode_responses=True,
            host="localhost",
            port="1111",
            db=0,
        ) as redis_client:
            assert redis_client is not None
            assert await redis_client.ping()
            await redis_client.set("test_key", "test_value")
            assert await redis_client.get("test_key") == "test_value"
