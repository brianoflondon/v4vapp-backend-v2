import asyncio
import signal
import sys
from datetime import datetime, timezone
from pprint import pprint
from typing import Annotated, Any, Mapping, Sequence

import typer
from pydantic import BaseModel, Field

from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.config.setup import DEFAULT_CONFIG_FILENAME, InternalConfig, logger
from v4vapp_backend_v2.database.async_redis import V4VAsyncRedis
from v4vapp_backend_v2.database.db import MongoDBClient

ICON = "🏆"
app = typer.Typer()

# Define a global flag to track shutdown
shutdown_event = asyncio.Event()

payment_pipeline: Sequence[Mapping[str, Any]] = [
    {
        "$project": {
            "fullDocument.creation_date": 1,
            "fullDocument.payment_hash": 1,
            "fullDocument.status": 1,
            "fullDocument.value_msat": 1,
        }
    }
]
invoice_pipeline: Sequence[Mapping[str, Any]] = [
    {
        "$project": {
            "fullDocument.creation_date": 1,
            "fullDocument.r_hash": 1,
            "fullDocument.state": 1,
            "fullDocument.amt_paid_msat": 1,
            "fullDocument.value_msat": 1,
            "fullDocument.memo": 1,
        }
    }
]
hive_ops_pipeline: Sequence[Mapping[str, Any]] = [
    {"$match": {"fullDocument.type": {"$ne": "block_marker"}}}
]


def get_mongodb_client() -> MongoDBClient:
    """
    Returns a MongoDB client instance.

    This function creates a MongoDB client instance using the default connection
    and database name from the configuration.

    Returns:
        MongoDBClient: The MongoDB client instance.
    """
    dbs_config = InternalConfig().config.dbs_config
    return MongoDBClient(
        db_conn=dbs_config.default_connection,
        db_name=dbs_config.default_name,
        db_user=dbs_config.default_user,
    )


class ResumeToken(BaseModel):
    data: Mapping[str, Any] | None = Field(
        None, description="Resume token for MongoDB change stream"
    )
    timestamp: datetime = Field(
        datetime.now(tz=timezone.utc), description="Timestamp when the token were created"
    )
    collection: str = Field("", description="Collection name for the change stream")
    redis_client: V4VAsyncRedis | None = Field(
        None, description="Redis client instance for storing the resume token"
    )

    model_config = {"arbitrary_types_allowed": True}

    def __init__(self, collection: str, **data: Any):
        """
        Initialize the ResumeToken instance.

        Args:
            collection (str): The name of the collection for the change stream.
            redis_client (V4VAsyncRedis, optional): The Redis client instance. Defaults to None.
            **data: Keyword arguments to initialize the ResumeToken instance.
        """
        super().__init__(**data)
        self.collection = collection

    async def set_token(self, token_data: Mapping[str, Any]):
        """
        Set the resume token and update the timestamp.

        Args:
            token_data (Mapping[str, Any]): The resume token data to set.
        """
        self.data = token_data
        serialized_token = repr(self.data)
        self.timestamp = datetime.now(tz=timezone.utc)
        redis_key = f"resume_token:{self.collection}"
        if not self.redis_client:
            self.redis_client = V4VAsyncRedis()
        async with self.redis_client:
            await self.redis_client.redis.set(
                name=redis_key,
                value=serialized_token,
            )

    async def get_token(self) -> Mapping[str, Any]:
        """
        Get the resume token from Redis.

        Returns:
            Mapping[str, Any]: The resume token data.
        """
        redis_key = f"resume_token:{self.collection}"
        if not self.redis_client:
            self.redis_client = V4VAsyncRedis()
        async with self.redis_client:
            resume_token = await self.redis_client.redis.get(redis_key)
            if resume_token:
                try:
                    resume_token = eval(resume_token)  # Deserialize the resume token
                    logger.info(f"Resume token deserialized: {resume_token}")
                except Exception as e:
                    logger.error(f"Failed to deserialize resume token: {e}")
                    resume_token = None
            else:
                resume_token = None
        return resume_token


async def subscribe_stream(
    collection_name: str = "invoices", pipeline: Sequence[Mapping[str, Any]] | None = None
):
    """
    Asynchronously subscribes to a stream and logs updates.

    Args:
        collection (str): The name of the collection to subscribe to.
        pipeline (Sequence[Mapping[str, Any]]): The aggregation pipeline to use for the stream.

    Returns:
        None
    """
    # resume_token = {
    #     "_data": "82680A4B16000000012B042C0100296E5A100427C3560459D34841BCB69B20139E3E3F463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F69640064680A4B1689058837EB259D00000004"
    # }
    logger.info(f"Subscribing to {collection_name} stream...")
    client = get_mongodb_client()
    collection = await client.get_collection(collection_name)
    resume = ResumeToken(collection=collection_name)
    try:
        resume_token = await resume.get_token()
        async with collection.watch(
            pipeline=pipeline,
            full_document="updateLookup",
            resume_after=resume_token,
        ) as stream:
            async for change in stream:
                if shutdown_event.is_set():
                    logger.info(f"{ICON} Shutdown signal received. Exiting stream...")
                    break
                await resume.set_token(change.get("_id", {}))
                pprint(f"-- {collection_name} --" * 5)
                pprint(change, indent=2)

    except (asyncio.CancelledError, KeyboardInterrupt):
        InternalConfig.notification_lock = True
        logger.info(f"{ICON} 👋 Received signal to stop. Exiting...")
        logger.info(
            f"{ICON} 👋 Goodbye! from {collection_name} stream",
            extra={"notification": True},
        )
        return

    except Exception as e:
        logger.error(f"{ICON} Error in stream subscription: {e}", extra={"error": e})
        raise e
    finally:
        logger.info(f"{ICON} Closed connection to {collection_name} stream.")


def handle_shutdown_signal():
    """
    Signal handler to set the shutdown event.
    """
    logger.info("Received shutdown signal. Setting shutdown event.")
    shutdown_event.set()


async def main_async_start():
    """
    Main function to run Database Monitor app.
    Args:
        node (str): example command line param.

    Returns:
        None
    """
    CONFIG = InternalConfig().config
    logger.info(
        f"{ICON} Notification bot: {CONFIG.logging.default_notification_bot_name} "
        f"🔗 Database Monitor connection: {CONFIG.dbs_config.default_connection} "
        f"🔗 Database Monitor name: {CONFIG.dbs_config.default_name} "
    )
    loop = asyncio.get_event_loop()
    # Register signal handlers for SIGTERM and SIGINT
    loop.add_signal_handler(signal.SIGTERM, handle_shutdown_signal)
    loop.add_signal_handler(signal.SIGINT, handle_shutdown_signal)
    try:
        logger.info(f"{ICON} Database Monitor App started.")
        # Simulate some work
        while not shutdown_event.is_set():
            tasks = [
                asyncio.create_task(
                    subscribe_stream(collection_name="invoices", pipeline=invoice_pipeline)
                ),
                asyncio.create_task(
                    subscribe_stream(collection_name="payments", pipeline=payment_pipeline)
                ),
                asyncio.create_task(
                    subscribe_stream(collection_name="hive_ops", pipeline=hive_ops_pipeline)
                ),
            ]
            await asyncio.gather(*tasks)

    except (asyncio.CancelledError, KeyboardInterrupt):
        InternalConfig.notification_lock = True
        logger.info(f"{ICON} 👋 Received signal to stop. Exiting...")
        logger.info(f"{ICON} 👋 Goodbye! from Database Monitor App", extra={"notification": True})
    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False})
        logger.error(f"{ICON} Irregular shutdown in Database Monitor App {e}", extra={"error": e})
        raise e
    finally:
        logger.info(f"{ICON} Cleaning up resources...")
        # Cancel all tasks except the current one
        if hasattr(InternalConfig, "notification_loop"):
            while InternalConfig.notification_lock:
                logger.info("Waiting for notification loop to complete...")
                await asyncio.sleep(0.5)  # Allow pending notifications to complete
        current_task = asyncio.current_task()
        tasks = [task for task in asyncio.all_tasks() if task is not current_task]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"{ICON} 👋 Goodbye! from Hive Monitor", extra={"notification": True})
        logger.info(f"{ICON} Clearing notifications")
        await asyncio.sleep(2)


@app.command()
def main(
    config_filename: Annotated[
        str,
        typer.Option(
            "-c",
            "--config",
            "--config-filename",
            help="The name of the config file (in a folder called ./config)",
            show_default=True,
        ),
    ] = DEFAULT_CONFIG_FILENAME,
):
    """
    Main function to do what you want.
    Args:
        config_filename (str): The name of the config file (in a folder called ./config).

    Returns:
        None
    """
    _ = InternalConfig(config_filename=config_filename)
    logger.info(
        f"{ICON} ✅ Database Monitor App. Started. Version: {__version__}",
        extra={"notification": True},
    )
    logger.info(
        f"{ICON} Database Monitor App. Config file: {config_filename}",
        extra={"notification": False},
    )

    asyncio.run(main_async_start())


if __name__ == "__main__":
    try:
        logger.name = "db_monitor_app"
        app()
        print("👋 Goodbye!")
    except KeyboardInterrupt:
        print("👋 Goodbye!")
        sys.exit(0)

    except Exception as e:
        logger.exception(e)
        sys.exit(1)
