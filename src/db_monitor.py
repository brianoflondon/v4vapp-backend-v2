import asyncio
import signal
import sys
from datetime import datetime, timezone
from typing import Annotated, Any, Mapping, Sequence

import typer
from pydantic import BaseModel, Field

from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.actions.tracked_all import tracked_any
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import DEFAULT_CONFIG_FILENAME, InternalConfig, logger
from v4vapp_backend_v2.database.async_redis import V4VAsyncRedis
from v4vapp_backend_v2.database.db import MongoDBClient

ICON = "🏆"
app = typer.Typer()

# Define a global flag to track shutdown
shutdown_event = asyncio.Event()

# Can't find any way to filter this in the pipeline, will do it in code.
pipeline_exclude_locked_changes: Sequence[Mapping[str, Any]] = []

payment_pipeline: Sequence[Mapping[str, Any]] = pipeline_exclude_locked_changes + [
    {
        "$project": {
            "fullDocument.creation_date": 1,
            "fullDocument.payment_hash": 1,
            "fullDocument.status": 1,
            "fullDocument.value_msat": 1,
        }
    },
]
invoice_pipeline: Sequence[Mapping[str, Any]] = pipeline_exclude_locked_changes + [
    {
        "$project": {
            "fullDocument.creation_date": 1,
            "fullDocument.r_hash": 1,
            "fullDocument.state": 1,
            "fullDocument.amt_paid_msat": 1,
            "fullDocument.value_msat": 1,
            "fullDocument.memo": 1,
        }
    },
]
hive_ops_pipeline: Sequence[Mapping[str, Any]] = pipeline_exclude_locked_changes + [
    {
        "$match": {
            "fullDocument.type": {"$ne": "block_marker"},
        }
    }
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
    """
    ResumeToken is a model for managing MongoDB change stream resume tokens.

    Attributes:
        data (Mapping[str, Any] | None): The resume token data for MongoDB change streams.
        timestamp (datetime): The timestamp when the token was created.
        redis_client (V4VAsyncRedis | None): The Redis client instance for storing the resume token.

    Methods:
        __init__(collection: str, **data: Any):
            Initialize the ResumeToken instance with a collection name and optional data.

        async set_token(token_data: Mapping[str, Any]):
            Set the resume token, update the timestamp, and store it in Redis.

        async get_token() -> Mapping[str, Any]:
            Retrieve the resume token from Redis and deserialize it.
    """

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
    redis_key: str = Field("", description="Redis key for storing the resume token")

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
        dbs_config = InternalConfig().config.dbs_config
        self.redis_key = (
            f"resume_token:{logger.name}:{dbs_config.default_connection}:"
            f"{dbs_config.default_name}:{self.collection}"
        )

    def set_token(self, token_data: Mapping[str, Any]):
        """
        Set the resume token and update the timestamp.

        Args:
            token_data (Mapping[str, Any]): The resume token data to set.
        """
        self.data = token_data
        self.timestamp = datetime.now(tz=timezone.utc)
        serialized_token = repr(self.data)

        if not self.redis_client:
            self.redis_client = V4VAsyncRedis()
        try:
            # Use the sync_redis client to store the token in Redis
            self.redis_client.sync_redis.set(self.redis_key, serialized_token)
        except Exception as e:
            logger.error(f"Error setting resume token for collection '{self.collection}': {e}")
            raise e

    @property
    def token(self) -> Mapping[str, Any] | None:
        """
        Retrieve the resume token from Redis and deserialize it.

        Returns:
            Mapping[str, Any] | None: The resume token data or None if not found.
        """
        try:
            if not self.redis_client:
                self.redis_client = V4VAsyncRedis()
            serialized_token: str = self.redis_client.sync_redis.get(self.redis_key)  # type: ignore
            if serialized_token:
                self.data = eval(serialized_token)  # Deserialize the token # type: ignore
                logger.info(
                    f"Resume token retrieved for collection '{self.collection}'",
                    extra={"resume_token": self.data},
                )
                return self.data
            else:
                logger.warning(f"No resume token found for collection '{self.collection}'.")
                return None
        except Exception as e:
            logger.error(f"Error retrieving resume token for collection '{self.collection}': {e}")
            raise e


def change_to_locked(change: Mapping[str, Any]) -> bool:
    update_description = change.get("updateDescription", {})
    updated_fields = update_description.get("updatedFields", {})
    removed_fields = update_description.get("removedFields", [])

    # Check if "locked" is in either updatedFields or removedFields
    if "locked" in updated_fields or "locked" in removed_fields:
        return True
    return False


async def process_op(change: Mapping[str, Any], collection: str):
    """
    Creates a ledger entry based on the document and collection name.

    Args:
        change (Mapping[str, Any]): The document containing the change data.
        collection (str): The name of the collection.

    Returns:
        None
    """
    # server_account_names = InternalConfig().config.hive.server_account_names
    full_document = change.get("fullDocument", {})
    if not full_document:
        logger.warning(f"{ICON} No fullDocument found in change: {change}")
        return
    op = tracked_any(full_document)
    logger.info(f"Processing {op.group_id_query}")
    await op.process()
    await asyncio.sleep(2)
    logger.info(f"Unlocking {op.group_id_query}")
    await op.unlock_op()


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
    TrackedBaseModel.db_client = client
    collection = await client.get_collection(collection_name)
    resume = ResumeToken(collection=collection_name)
    try:
        resume_token = resume.token
        async with collection.watch(
            pipeline=pipeline,
            full_document="updateLookup",
            resume_after=resume_token,
        ) as stream:
            async for change in stream:
                if not change_to_locked(change):
                    asyncio.create_task(process_op(change=change, collection=collection_name))
                resume.set_token(change.get("_id", {}))
                if shutdown_event.is_set():
                    logger.info(f"{ICON} Shutdown signal received. Exiting stream...")
                    break
                logger.info(
                    f"{ICON} Change detected in {collection_name}",
                    extra={"notification": False, "change": change},
                )

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
                asyncio.create_task(subscribe_stream(collection_name="invoices")),
                asyncio.create_task(subscribe_stream(collection_name="payments")),
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
        logger.name = "db_monitor"
        app()
        print("👋 Goodbye!")
    except KeyboardInterrupt:
        print("👋 Goodbye!")
        sys.exit(0)

    except Exception as e:
        logger.exception(e)
        sys.exit(1)
