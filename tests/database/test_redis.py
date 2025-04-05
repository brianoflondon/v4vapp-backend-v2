import asyncio
import json
from pathlib import Path
from unittest.mock import patch

import httpx
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
async def test_redis_client_default():
    """
    Test the default behavior of the V4VAsyncRedis client.
    Default is to decode responses and use the connection from the config.

    This test performs the following actions:
    1. Initializes a V4VAsyncRedis client.
    2. Flushes the Redis database.
    3. Asserts that the Redis client and its underlying Redis connection are not None.
    4. Pings the Redis server to ensure it is responsive.
    5. Sets a key-value pair in the Redis database.
    6. Retrieves the value for the set key and asserts it matches the expected value.
    7. Deletes the key from the Redis database and asserts the deletion was successful.
    """
    redis_client = V4VAsyncRedis()
    await redis_client.flush()
    assert redis_client is not None
    assert redis_client.redis is not None
    assert await redis_client.redis.ping()
    await redis_client.redis.set("test_key", "test_value")
    assert await redis_client.redis.get("test_key") == "test_value"
    assert await redis_client.redis.delete("test_key")


@pytest.mark.asyncio
async def test_redis_client_decode_true():
    redis_client = V4VAsyncRedis(decode_responses=True)
    await redis_client.flush()
    assert redis_client is not None
    assert redis_client.redis is not None
    assert await redis_client.redis.ping()
    await redis_client.redis.set("test_key", "test_value")
    assert await redis_client.redis.get("test_key") == "test_value"
    assert await redis_client.redis.delete("test_key")


@pytest.mark.asyncio
async def test_redis_client_no_config():
    """
    This uses no config (defaults, localhost:6379 db=0) and decode_responses=True.
    This will work with the Redis server running from docker in the local environment.
    """
    redis_client = V4VAsyncRedis(no_config=True)
    await redis_client.flush()
    assert redis_client is not None
    assert redis_client.redis is not None
    assert await redis_client.redis.ping()
    await redis_client.redis.set("test_key", "test_value")
    assert await redis_client.redis.get("test_key") == "test_value"
    assert await redis_client.redis.delete("test_key")


@pytest.mark.asyncio
async def test_redis_client_decode_false():
    redis_client = V4VAsyncRedis(decode_responses=False)
    await redis_client.flush()
    assert redis_client is not None
    assert redis_client.redis is not None
    assert await redis_client.redis.ping()
    await redis_client.redis.set("test_key", "test_value")
    assert await redis_client.redis.get("test_key") == b"test_value"
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


################ Sync Redis Tests ################


def test_sync_redis_client_default():
    """
    Test the default behavior of the V4VAsyncRedis client.
    Default is to decode responses and use the connection from the config.

    This test performs the following actions:
    1. Initializes a V4VAsyncRedis client.
    2. Flushes the Redis database.
    3. Asserts that the Redis client and its underlying Redis connection are not None.
    4. Pings the Redis server to ensure it is responsive.
    5. Sets a key-value pair in the Redis database.
    6. Retrieves the value for the set key and asserts it matches the expected value.
    7. Deletes the key from the Redis database and asserts the deletion was successful.
    """
    redis_sync_client = V4VAsyncRedis().sync_redis
    assert redis_sync_client is not None
    assert redis_sync_client.ping()
    redis_sync_client.set("test_key", "test_value")
    assert redis_sync_client.get("test_key") == "test_value"
    assert redis_sync_client.delete("test_key")
    redis_sync_client.close()


def test_sync_redis_client_context_manager():
    with V4VAsyncRedis() as redis_sync_client:
        assert redis_sync_client is not None
        assert redis_sync_client.ping()
        redis_sync_client.set("test_key", "test_value")
        assert redis_sync_client.get("test_key") == "test_value"
        redis_sync_client.flushdb()



@pytest.mark.asyncio
async def test_with_get_response():
    hive_accname = "blocktrades"
    url = f"https://api.syncad.com/hafbe-api/witnesses/{hive_accname}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=20)
        if response.status_code == 200:
            async with V4VAsyncRedis() as redis_client:
                await redis_client.set(
                    name=f"witness_{hive_accname}", value=json.dumps(response.json())
                )

            async with V4VAsyncRedis() as redis_client:
                witness_data = json.loads(
                    await redis_client.get(f"witness_{hive_accname}")
                )
                assert witness_data is not None
                assert witness_data["witness"]["witness_name"] == hive_accname
                await redis_client.delete(f"witness_{hive_accname}")
        else:
            print(f"Failed to get response for {hive_accname}")
            print(response.status_code)
