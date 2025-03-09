import pytest
from redis.exceptions import ConnectionError

from v4vapp_backend_v2.database.async_redis import V4VAsyncRedis


@pytest.mark.asyncio
async def test_redis_client():
    redis_client = V4VAsyncRedis(decode_responses=True)
    assert redis_client is not None
    assert redis_client.redis is not None
    assert await redis_client.redis.ping()
    await redis_client.redis.set("test_key", "test_value")
    assert await redis_client.redis.get("test_key") == "test_value"
    assert await redis_client.redis.close() is None


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
    await redis_client.redis.set("test_key", "test_value")
    assert await redis_client.redis.get("test_key") == b"test_value"
    assert await redis_client.redis.close() is None


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

    assert redis_client is not None
    redis_client.aclose()


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
