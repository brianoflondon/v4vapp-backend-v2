import asyncio

from redis import Redis as SyncRedis
from redis.asyncio import ConnectionPool, Redis, from_url
from redis.connection import ConnectionPool as SyncConnectionPool  # Add this import
from redis.exceptions import ConnectionError

from v4vapp_backend_v2.config.setup import InternalConfig, logger


class V4VAsyncRedis:
    """
    Asynchronous Redis client for V4V application with connection pooling.

    Attributes:
        host (str): Redis server hostname.
        port (int): Redis server port.
        db (int): Redis database number.
        decode_responses (bool): Flag to decode responses.
        kwargs (dict): Additional keyword arguments for Redis connection.
        redis (Redis): Redis client instance.
        sync_redis (SyncRedis): Synchronous Redis client instance.
        no_config (bool): Flag to indicate whether to use the config file.
    """

    # Replace the single pool variables with dictionaries
    # Class-level connection pools
    _async_pools = {}  # {decode_responses: pool}
    _sync_pools = {}  # {decode_responses: pool}

    @classmethod
    def get_async_pool(cls, decode_responses=True, force_new=False):
        """Get or create a shared async connection pool with specific decode setting."""
        pool_key = f"decode_{decode_responses}"

        if pool_key not in cls._async_pools or force_new:
            config = InternalConfig().config.redis
            cls._async_pools[pool_key] = ConnectionPool(
                host=config.host,
                port=config.port,
                db=config.db,
                decode_responses=decode_responses,  # Use the parameter value
                **config.kwargs,
            )
            logger.debug(
                f"Created new async Redis connection pool: {config.host}:{config.port} (decode={decode_responses})"
            )

        return cls._async_pools[pool_key]

    @classmethod
    def get_sync_pool(cls, decode_responses=True, force_new=False):
        """Get or create a shared sync connection pool with specific decode setting."""
        pool_key = f"decode_{decode_responses}"

        if pool_key not in cls._sync_pools or force_new:
            config = InternalConfig().config.redis
            cls._sync_pools[pool_key] = SyncConnectionPool(
                host=config.host,
                port=config.port,
                db=config.db,
                decode_responses=decode_responses,  # Use the parameter value
                **config.kwargs,
            )
            logger.debug(
                f"Created new sync Redis connection pool: {config.host}:{config.port} (decode={decode_responses})"
            )

        return cls._sync_pools[pool_key]

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    decode_responses: bool = True
    kwargs: dict = {}
    redis: Redis
    sync_redis: SyncRedis
    no_config: bool = False  # If True will not use the config file
    using_pool: bool = False

    def __init__(self, **kwargs):
        """
        Initialize Redis client with connection pooling by default.

        Args:
            use_pool (bool): Whether to use connection pooling (default: True)
            no_config (bool): Whether to ignore config file settings (default: False)
            connection_str (str, optional): Redis connection string
            Other Redis connection parameters can be passed directly
        """
        try:
            # Verify we have a running event loop
            asyncio.get_running_loop()
        except RuntimeError:
            logger.warning("No running event loop when initializing Redis client")
            # Continue anyway, but async operations will fail later

        self.no_config = kwargs.pop("no_config", False)
        use_pool = kwargs.pop("use_pool", True)

        # If using config and pooling (default behavior)
        if not self.no_config and use_pool:
            self.config = InternalConfig().config.redis
            self.host = self.config.host
            self.port = self.config.port
            self.db = self.config.db
            self.decode_responses = kwargs.get("decode_responses", True)

            # Use shared connection pool with matching decode_responses setting
            pool = self.get_async_pool(decode_responses=self.decode_responses)
            self.redis = Redis(connection_pool=pool, decode_responses=self.decode_responses)
            sync_pool = self.get_sync_pool(decode_responses=self.decode_responses)
            self.sync_redis = SyncRedis(connection_pool=sync_pool, decode_responses=self.decode_responses)
            self.using_pool = True

        # Otherwise create individual connections based on parameters
        else:
            self.config = InternalConfig().config.redis if not self.no_config else None

            if connection_str := kwargs.get("connection_str"):
                kwargs.pop("connection_str", None)
                self.redis = from_url(connection_str, **kwargs)
                # For sync client, convert the connection string
                sync_conn_str = connection_str.replace("redis://", "")
                self.sync_redis = SyncRedis.from_url(f"redis://{sync_conn_str}", **kwargs)
            else:
                if "host" in kwargs and "port" in kwargs:
                    self.host = kwargs.pop("host")
                    self.port = kwargs.pop("port")
                    self.db = kwargs.pop("db", 0)
                    self.decode_responses = kwargs.pop("decode_responses", True)
                elif not self.no_config:
                    self.host = self.config.host
                    self.port = self.config.port
                    self.db = self.config.db
                    self.decode_responses = kwargs.pop("decode_responses", True)
                    kwargs.update(self.config.kwargs)

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
                self.using_pool = False

            # Extract connection details for logging
            if hasattr(self.redis, "connection_pool") and hasattr(
                self.redis.connection_pool, "connection_kwargs"
            ):
                self.host = self.redis.connection_pool.connection_kwargs.get("host", self.host)
                self.port = self.redis.connection_pool.connection_kwargs.get("port", self.port)
                self.db = self.redis.connection_pool.connection_kwargs.get("db", self.db)

            self.kwargs = kwargs

        logger.debug(
            f"Redis {'pooled' if self.using_pool else 'direct'} connection established "
            f"{self.host}:{self.port} - DB: {self.db}"
        )

    async def flush(self):
        """Flush the current Redis database."""
        try:
            async with self.redis as redis:
                await redis.flushdb()
                logger.info("Redis Database flushed successfully")
        except Exception as e:
            logger.error(f"Redis Error flushing database: {e}")

    async def __aenter__(self) -> Redis:
        """Async context manager entry that returns the Redis client."""
        try:
            _ = await self.redis.ping()
            return self.redis
        except ConnectionError as e:
            logger.warning(f"Redis ConnectionError {self.host}:{self.port} - {e}")
            raise
        except Exception as e:
            logger.warning(f"Redis ConnectionError {self.host}:{self.port} - {e}")
            raise

    async def __aexit__(self, exc_type, exc, tb):
        """Async context manager exit that closes connection only if not using pool."""
        if not self.using_pool:
            try:
                await self.redis.aclose()
            except RuntimeError:
                # Ignore "event loop is closed" errors
                pass
            except Exception as e:
                logger.warning(f"Error closing Redis connection: {e}")

    def __enter__(self) -> SyncRedis:
        """Sync context manager entry that returns the sync Redis client."""
        try:
            _ = self.sync_redis.ping()
            return self.sync_redis
        except ConnectionError as e:
            logger.warning(f"Redis ConnectionError {self.host}:{self.port} - {e}")
            raise
        except Exception as e:
            logger.warning(f"Redis ConnectionError {self.host}:{self.port} - {e}")
            raise

    def __exit__(self, exc_type, exc, tb):
        """Sync context manager exit that closes connection only if not using pool."""
        if not self.using_pool and self.sync_redis:
            self.sync_redis.close()

    def __del__(self):
        """Destructor that properly logs and cleans up resources."""
        try:
            if hasattr(self, "redis") and hasattr(self, "using_pool") and not self.using_pool:
                # For non-pooled connections, use sync close since we're in a non-async context
                if hasattr(self, "sync_redis") and hasattr(self.sync_redis, "close"):
                    try:
                        self.sync_redis.close()
                    except Exception:
                        pass  # Ignore errors during cleanup
            if hasattr(self, "host") and hasattr(self, "port"):
                logger.debug(f"Redis client disposed {self.host}:{self.port}")
        except Exception:
            # Ignore errors during cleanup
            pass
