from redis.asyncio import Redis, from_url
from redis.exceptions import ConnectionError

from v4vapp_backend_v2.config.setup import Config, InternalConfig, logger


class V4VAsyncRedis:
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
