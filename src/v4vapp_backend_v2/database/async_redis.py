# import pickle
# from functools import wraps

from redis.asyncio import Redis, from_url
from redis.exceptions import ConnectionError

from v4vapp_backend_v2.config.setup import InternalConfig, logger


class V4VAsyncRedis:
    """
    Asynchronous Redis client for V4V application.

    Attributes:
        host (str): Redis server hostname.
        port (int): Redis server port.
        db (int): Redis database number.
        decode_responses (bool): Flag to decode responses.
        kwargs (dict): Additional keyword arguments for Redis connection.
        redis (Redis): Redis client instance.

    Methods:
        __init__(**kwargs):
            Initializes the Redis client with provided or default configuration.

        __aenter__() -> Redis:
            Asynchronous context manager entry. Pings the Redis server to
            ensure connection.

        __aexit__(exc_type, exc, tb):
            Asynchronous context manager exit. Closes the Redis connection.

        __del__():
            Destructor. Closes the Redis connection if it exists.
    """

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    decode_responses: bool = True
    kwargs: dict = {}
    redis: Redis

    def __init__(self, **kwargs):
        self.config = InternalConfig().config.redis
        if not kwargs:
            self.host = self.config.host
            self.port = self.config.port
            self.db = self.config.db
            self.kwargs = self.config.kwargs
            self.decode_responses = True
            self.redis = Redis(
                host=self.host, port=self.port, db=self.db, **self.kwargs
            )
        else:
            if connection_str := kwargs.get("connection_str"):
                self.redis = from_url(connection_str, **kwargs)

            else:
                if "host" in kwargs and "port" in kwargs:
                    if "db" not in kwargs:
                        kwargs["db"] = 0
                    self.redis = Redis(**kwargs)
                else:
                    self.redis = Redis(
                        host=self.host, port=self.port, db=self.db, **kwargs
                    )
            self.host = self.redis.connection_pool.connection_kwargs["host"]
            self.port = self.redis.connection_pool.connection_kwargs["port"]
            self.db = self.redis.connection_pool.connection_kwargs["db"]
            self.kwargs = kwargs

    async def __aenter__(self) -> Redis:
        try:
            _ = await self.redis.ping()
            return self.redis
        except ConnectionError as e:
            logger.warning(f"ConnectionError {self.host}:{self.port} - {e}")
            logger.warning(e)
            raise e

    async def __aexit__(self, exc_type, exc, tb):
        return self.redis.aclose()

    def __del__(self):
        if self.redis:
            self.redis.aclose()
            logger.debug(f"Redis connection closed {self.host}:{self.port}")


# # Async caching decorator using V4VAsyncRedis
# # Not really working needs more testing
# def cache_with_redis_async(func):
#     @wraps(func)
#     async def wrapper(*args, **kwargs):
#         # Initialize the async Redis client
#         async with V4VAsyncRedis(decode_responses=False) as redis_client:

#             # Create a unique key based on function name and arguments
#             key = f"{func.__name__}:{str(args)}:{str(kwargs)}"

#             # Check if result is in cache
#             cached_result = await redis_client.get(key)
#             if cached_result is not None and (
#                 use_cache := kwargs.get("use_cache", True)
#             ):
#                 logger.info(f"Cache hit {key}")
#                 # Since decode_responses=True, cached_result is a string;
#                 # we need to deserialize
#                 return pickle.loads(cached_result)  # Encode back to bytes for pickle

#             # If not cached or use_cache is false, compute and store
#             try:
#                 result = await func(*args, **kwargs)  # Await the async function
#             except Exception as e:
#                 raise e

#             # Store as bytes, since decode_responses=True expects strings
#             await redis_client.setex(key, 60, pickle.dumps(result))  # 1-hour TTL
#             return result

#     return wrapper
