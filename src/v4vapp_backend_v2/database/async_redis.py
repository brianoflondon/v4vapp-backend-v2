from redis import Redis as SyncRedis
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
        sync_redis (SyncRedis): Synchronous Redis client instance.
        no_config (bool): Flag to indicate whether to use the config file.

    Methods:
        __init__(**kwargs):
            Initializes the Redis client with provided or default configuration.

        __aenter__() -> Redis:
            Asynchronous context manager entry. Pings the Redis server to
            ensure connection.

        __aexit__(exc_type, exc, tb):
            Asynchronous context manager exit. Closes the Redis connection.

        __enter__() -> SyncRedis:
            Synchronous context manager entry. Pings the Redis server to
            ensure connection.

        __exit__(exc_type, exc, tb):
            Synchronous context manager exit. Closes the Redis connection.

        __del__():
            Destructor. Closes the Redis connection if it exists.
    """

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    decode_responses: bool = True
    kwargs: dict = {}
    redis: Redis
    sync_redis: SyncRedis
    no_config: bool = False  # If True will not use the config file

    def __init__(self, **kwargs):
        self.config = InternalConfig().config.redis
        no_config = kwargs.get("no_config", False)
        kwargs.pop("no_config", None)
        if not no_config and "host" not in kwargs and "port" not in kwargs:
            self.host = self.config.host
            self.port = self.config.port
            self.db = self.config.db
            self.kwargs = self.config.kwargs
            self.decode_responses = kwargs.get("decode_responses", True)
            self.redis = Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                decode_responses=self.decode_responses,
                **self.kwargs,
            )
            self.sync_redis = SyncRedis(
                host=self.host,
                port=self.port,
                db=self.db,
                decode_responses=self.decode_responses,
                **self.kwargs,
            )
        else:
            if connection_str := kwargs.get("connection_str"):
                self.redis = from_url(connection_str, **kwargs)

            else:
                if "host" in kwargs and "port" in kwargs:
                    if "db" not in kwargs:
                        kwargs["db"] = 0
                    self.redis = Redis(**kwargs)
                    self.sync_redis = SyncRedis(**kwargs)
                else:
                    self.redis = Redis(
                        host=self.host,
                        port=self.port,
                        db=self.db,
                        decode_responses=self.decode_responses,
                        **kwargs,
                    )
                    self.sync_redis = SyncRedis(
                        host=self.host,
                        port=self.port,
                        db=self.db,
                        decode_responses=self.decode_responses,
                        **kwargs,
                    )
            self.host = self.redis.connection_pool.connection_kwargs["host"]
            self.port = self.redis.connection_pool.connection_kwargs["port"]
            self.db = self.redis.connection_pool.connection_kwargs["db"]
            self.kwargs = kwargs

        logger.debug(
            f"Redis connection established {self.host}:{self.port} - "
            f"DB: {self.db} - Decode: {self.decode_responses}"
        )

    async def flush(self):
        try:
            async with self.redis as redis:
                await redis.flushdb()
                logger.info("Redis Database flushed successfully")
        except Exception as e:
            logger.error(f"Redis Error flushing database: {e}")

    async def __aenter__(self) -> Redis:
        try:
            _ = await self.redis.ping()
            return self.redis
        except ConnectionError as e:
            logger.warning(f"Redis ConnectionError {self.host}:{self.port} - {e}")
            logger.warning(e)
            raise e
        except Exception as e:
            logger.warning(f"Redis ConnectionError {self.host}:{self.port} - {e}")
            logger.warning(e)
            raise e

    async def __aexit__(self, exc_type, exc, tb):
        await self.redis.aclose()

    def __enter__(self) -> SyncRedis:
        try:
            _ = self.sync_redis.ping()
            return self.sync_redis
        except ConnectionError as e:
            logger.warning(f"Redis ConnectionError {self.host}:{self.port} - {e}")
            logger.warning(e)
            raise e
        except Exception as e:
            logger.warning(f"Redis ConnectionError {self.host}:{self.port} - {e}")
            logger.warning(e)
            raise e

    def __exit__(self, exc_type, exc, tb):
        if self.sync_redis:
            self.sync_redis.close()

    def __del__(self):
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
