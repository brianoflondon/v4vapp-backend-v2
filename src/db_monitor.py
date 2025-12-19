import asyncio
import signal
import sys
from contextlib import suppress
from datetime import datetime, timezone
from pprint import pprint
from typing import Annotated, Any, Mapping, Sequence

import bson
import typer
from colorama import Fore, Style
from pydantic import BaseModel, ConfigDict, Field
from pymongo.errors import (
    ConnectionFailure,
    NetworkTimeout,
    OperationFailure,
    ServerSelectionTimeoutError,
)

from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntryException
from v4vapp_backend_v2.accounting.pipelines.simple_pipelines import db_monitor_pipelines
from v4vapp_backend_v2.actions.tracked_any import tracked_any_filter
from v4vapp_backend_v2.config.setup import DEFAULT_CONFIG_FILENAME, InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.general_purpose_funcs import truncate_text
from v4vapp_backend_v2.process.lock_str_class import CustIDLockException, LockStr
from v4vapp_backend_v2.process.process_pending_hive import resend_transactions
from v4vapp_backend_v2.process.process_tracked_events import process_tracked_event

ICON = "🏆"
app = typer.Typer()

# Define a global flag to track shutdown
shutdown_event = asyncio.Event()


class ResumeToken(BaseModel):
    """
    ResumeToken is a model for managing MongoDB change stream resume tokens.

    Attributes:
        data (Mapping[str, Any] | None): The resume token data for MongoDB change streams.
        timestamp (datetime): The timestamp when the token was created.

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
    redis_key: str = Field("", description="Redis key for storing the resume token")

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, collection: str, **data: Any):
        """
        Initialize the ResumeToken instance.

        Args:
            collection (str): The name of the collection for the change stream.
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
        redis_client = InternalConfig.redis
        try:
            # Use the sync_redis client to store the token in Redis
            redis_client.set(self.redis_key, serialized_token)
        except Exception as e:
            logger.error(
                f"{ICON} Error setting resume token for collection '{self.collection}': {e}",
                extra={"notification": False},
            )
            raise e

    def delete_token(self):
        """
        Delete the resume token from Redis.
        """
        redis_client = InternalConfig.redis
        try:
            redis_client.delete(self.redis_key)
            logger.info(f"{ICON} Resume token deleted for collection '{self.collection}'")
        except Exception as e:
            logger.error(
                f"{ICON} Error deleting resume token for collection '{self.collection}': {e}",
                extra={"notification": False},
            )
            raise e

    @property
    def token(self) -> Mapping[str, Any] | None:
        """
        Retrieve the resume token from Redis and deserialize it.

        Returns:
            Mapping[str, Any] | None: The resume token data or None if not found.
        """
        try:
            redis_client = InternalConfig.redis
            serialized_token: str = redis_client.get(self.redis_key)  # type: ignore
            if serialized_token:
                self.data = eval(serialized_token)  # Deserialize the token # type: ignore
                logger.info(
                    f"{ICON} Resume token retrieved for collection '{self.collection}'",
                    extra={"resume_token": self.data},
                )
                return self.data
            else:
                logger.warning(f"{ICON} No resume token found for collection '{self.collection}'.")
                return None
        except Exception as e:
            logger.error(
                f"{ICON} Error retrieving resume token for collection '{self.collection}': {e}",
                extra={"notification": False},
            )
            return None


def ignore_changes(change: Mapping[str, Any]) -> bool:
    """
    Determines if the "locked" field is present in the updated or removed fields
    of a database change event.

    Args:
        change (Mapping[str, Any]): A dictionary representing a database change event.
            It is expected to contain an "updateDescription" key with details about
            the updated and removed fields.

    Returns:
        bool: True if the "locked" field is found in either the "updatedFields" or
        "removedFields" of the change event, otherwise False.
    """
    debugging = False
    if not debugging:
        return False

    update_description = change.get("updateDescription", {})
    updated_fields = update_description.get("updatedFields", {})
    removed_fields = update_description.get("removedFields", [])
    # logger.debug(
    #     f"Change detected Operation type: {change.get('operationType', '')} {change.get('ns', {})}"
    # )

    if update_description or updated_fields or removed_fields:
        print("update_descriptions")
        pprint(update_description)
        print("updated_fields")
        pprint(updated_fields)
        print("removed_fields")
        pprint(removed_fields)

    # Filter out custom_json sent purely for notifications
    # if "json" in updated_fields:
    #     if "notification" in updated_fields["json"]:
    #         if updated_fields["json"]["notification"] is True:
    #             return True

    # # Check if "locked" is in either updatedFields or removedFields
    # if "locked" in updated_fields or "locked" in removed_fields:
    #     return True
    # if "process_time" in updated_fields or "process_time" in removed_fields:
    #     # If process_time is present ignore the change
    #     return True
    # if (
    #     "change_conv" in updated_fields
    #     or "fee_conv" in updated_fields
    #     or "replies" in updated_fields
    # ):
    #     return True
    return False


async def process_op(change: Mapping[str, Any], collection: str) -> None:
    """
    Creates a ledger entry based on the document and collection name.

    Args:
        change (Mapping[str, Any]): The document containing the change data.
        collection (str): The name of the collection.

    Returns:
        None
    """
    # server_account_names = InternalConfig().config.hive.server_account_names
    try:
        full_document = change.get("fullDocument", {})
        if not full_document:
            logger.warning(
                f"{ICON} No fullDocument found in change: {change}",
                extra={"notification": False, "extra": {"change": change}},
            )
            return
    except Exception as e:
        logger.error(f"{ICON} Error extracting fullDocument: {e}", extra={"error": e})
        return
    o_id = full_document.get("_id")
    mongo_id = str(o_id) if o_id is not None else "unknown_id"
    async with LockStr(mongo_id).locked(
        timeout=None, blocking_timeout=None, request_details="db_monitor"
    ):
        if not full_document:
            logger.warning(
                f"{ICON} No fullDocument found in change: {change}", extra={"notification": False}
            )
            return
        try:
            op = tracked_any_filter(full_document)
        except ValueError as e:
            logger.info(f"{ICON} Error in tracked_any: {e}", extra={"notification": False})
            return
        logger.info(f"{ICON} Processing {op.group_id_query}")
        while True:
            try:
                ledger_entries = await process_tracked_event(op)
                logger.info(
                    f"{ICON} Lock release: {mongo_id} Processed operation: {op.group_id} result: {len(ledger_entries)} Ledger Entries",
                    extra={
                        **op.log_extra,
                        "ledger_entries": [le.model_dump() for le in ledger_entries],
                        "mongo_id": mongo_id,
                    },
                )
                return
            except ValueError as e:
                logger.exception(f"{ICON} Value error in process_tracked: {e}", extra={"error": e})
                return
            except NotImplementedError:
                logger.warning(
                    f"{ICON} Ignoring: {op.op_type} {op.short_id} {truncate_text(op.log_str, 40)}",
                    extra={"notification": False},
                )
                return
            except LedgerEntryException as e:
                logger.warning(f"{ICON} Ledger entry error: {e}", extra={"notification": False})
                return
            except CustIDLockException as e:
                logger.error(f"{ICON} CustID lock error: {e}", extra={"notification": False})
                await asyncio.sleep(5)
            finally:
                logger.info(f"{ICON} Lock release: {mongo_id}")


async def subscribe_stream(
    collection_name: str = "invoices",
    pipeline: Sequence[Mapping[str, Any]] | None = None,
    use_resume=True,
    error_count: int = 0,
    error_code: str = "",
) -> str | None:
    """
    Asynchronously subscribes to a stream and logs updates.

    Args:
        collection (str): The name of the collection to subscribe to.
        pipeline (Sequence[Mapping[str, Any]]): The aggregation pipeline to use for the stream.

    Returns:
        None
    """
    logger.info(f"{ICON} Subscribing to {collection_name} stream...")

    # Use two different mongo clients, one for the stream and the one for
    # the rest of the app.
    collection = InternalConfig.db[collection_name]
    resume = ResumeToken(collection=collection_name)
    try:
        if use_resume:
            resume_token = resume.token
        else:
            resume_token = None

        watch_kwargs = {
            "pipeline": pipeline,
            "full_document": "updateLookup",
        }
        if resume_token:
            watch_kwargs["resume_after"] = resume_token
        else:
            # Get the unix timestamp for 60 seconds ago
            unix_ts = int(datetime.now(tz=timezone.utc).timestamp()) - 60
            # The second argument (increment) is usually 0 for new watches
            watch_kwargs["start_at_operation_time"] = bson.Timestamp(unix_ts, 0)
            logger.warning(
                f"{ICON} {collection_name} stream started from 60s ago.",
                extra={"notification": False},
            )

        async with await collection.watch(**watch_kwargs) as stream:
            if error_code:
                logger.info(
                    f"{ICON} {collection_name} Resuming after stream error cleared: {error_code}",
                    extra={"notification": True, "error_code_clear": error_code},
                )
                error_count = 0
            error_code = f"db_monitor_{collection_name}"

            # Close the stream immediately when shutdown is requested
            async def _close_on_shutdown():
                await shutdown_event.wait()
                with suppress(Exception):
                    await stream.close()

            closer = asyncio.create_task(_close_on_shutdown())

            try:
                async for change in stream:
                    if shutdown_event.is_set():
                        logger.info(
                            f"{ICON} Shutdown requested; exiting {collection_name} stream loop."
                        )
                        break
                    full_document = change.get("fullDocument") or {}
                    group_id = full_document.get("group_id", None) or ""
                    logger.info(
                        f"{ICON}✳️ Change detected in {collection_name} {group_id}",
                        extra={"notification": False, "change": change},
                    )
                    if ignore_changes(change):
                        pass
                    else:
                        # Process the change if it is not a lock/unlock
                        asyncio.create_task(process_op(change=change, collection=collection_name))
                    resume.set_token(change.get("_id", {}))
                    if shutdown_event.is_set():
                        logger.info(
                            f"{ICON} Shutdown requested; exiting {collection_name} stream loop."
                        )
                        break
                    continue
            finally:
                closer.cancel()
                with suppress(asyncio.CancelledError):
                    await closer

    except (asyncio.CancelledError, KeyboardInterrupt) as e:
        logger.info(f"Keyboard interrupt or Cancelled: {collection_name} {e}")
        InternalConfig.notification_lock = True
        logger.info(f"{ICON} 👋 Received signal to stop. Exiting...")
        if await LockStr.any_locks_open():
            logger.info(f"{ICON} Open locks found. Please release them before shutdown.")
            while await LockStr.any_locks_open():
                logger.info(f"{ICON} Waiting for locks to be released...")
                await asyncio.sleep(5)
        logger.info(
            f"{ICON} 👋 Goodbye! from {collection_name} stream",
            extra={"notification": False},
        )
        raise e

    except OperationFailure as e:
        error_code = f"db_monitor_{collection_name}"
        error_count += 1
        logger.warning(
            f"{ICON} {collection_name} Operation failure in stream subscription: {e}",
            extra={"error_code": error_code, "notification": False},
        )
        if "resume" in str(e):
            resume.delete_token()
            if not shutdown_event.is_set():
                asyncio.create_task(
                    subscribe_stream(
                        collection_name=collection_name,
                        pipeline=pipeline,
                        error_count=error_count,
                        error_code=error_code,
                    )
                )
            return error_code

    except (
        ServerSelectionTimeoutError,
        NetworkTimeout,
        ConnectionFailure,
    ) as e:
        error_count += 1
        error_code = f"db_monitor_{collection_name}"
        sleep_time = min(30 * error_count, 180)
        logger.error(
            f"{ICON} Error {error_count} {collection_name} MongoDB connection error, will retry in {sleep_time}s: {truncate_text(e, 25)}",
            extra={"error_code": error_code, "notification": True, "error": e},
        )
        # Wait before attempting to reconnect
        await asyncio.sleep(sleep_time)
        logger.info(f"{ICON} Attempting to reconnect to {collection_name} stream...")
        if not shutdown_event.is_set():
            asyncio.create_task(
                subscribe_stream(
                    collection_name=collection_name,
                    pipeline=pipeline,
                    error_count=error_count,
                    error_code=error_code,
                )
            )
        return error_code

    except Exception as e:
        logger.error(
            f"{ICON} Error in stream subscription: {e}", extra={"error_code": "stream_error"}
        )
        raise e
    finally:
        logger.info(
            f"{ICON} Closed connection to {collection_name} stream. Error:{error_code} {error_count}"
        )


def handle_shutdown_signal():
    """
    Signal handler to set the shutdown event.
    """
    logger.info("Received shutdown signal. Setting shutdown event.")
    shutdown_event.set()


async def main_async_start(use_resume: bool = True):
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
    db_conn = DBConn()
    await db_conn.setup_database()
    # await LockStr.clear_all_locks()  # Clear any existing locks before starting
    await resend_transactions()

    loop = asyncio.get_event_loop()
    # Register signal handlers for SIGTERM and SIGINT
    loop.add_signal_handler(signal.SIGTERM, handle_shutdown_signal)
    loop.add_signal_handler(signal.SIGINT, handle_shutdown_signal)
    db_pipelines = db_monitor_pipelines()
    try:
        logger.info(f"{ICON} Database Monitor App started.")
        # Start streams once and wait for shutdown_event; then cancel streams
        tasks = [
            asyncio.create_task(
                subscribe_stream(
                    collection_name="invoices",
                    pipeline=db_pipelines["invoices"],
                    use_resume=use_resume,
                )
            ),
            asyncio.create_task(
                subscribe_stream(
                    collection_name="payments",
                    pipeline=db_pipelines["payments"],
                    use_resume=use_resume,
                )
            ),
            asyncio.create_task(
                subscribe_stream(
                    collection_name="hive_ops",
                    pipeline=db_pipelines["hive_ops"],
                    use_resume=use_resume,
                )
            ),
        ]
        await shutdown_event.wait()
        for t in tasks:
            t.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # NEW: Check for exceptions in task results and re-raise the first one
        for result in results:
            if isinstance(result, BaseException):
                raise result

    except (asyncio.CancelledError, KeyboardInterrupt):
        InternalConfig.notification_lock = True
        logger.info(f"{ICON} 👋 Received signal to stop. Checking for locks...")

    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False})
        logger.error(f"{ICON} Irregular shutdown in Database Monitor App {e}", extra={"error": e})
        raise e
    finally:
        logger.info(f"{ICON} Cleaning up resources...")
        logger.info(
            f"{ICON} 👋 Goodbye from Database Monitor App. Version: {__version__} on {InternalConfig().local_machine_name}",
            extra={"notification": True},
        )
        # Cancel all tasks except the current one
        if hasattr(InternalConfig, "notification_loop"):
            while InternalConfig.notification_lock:
                logger.info(f"{ICON} Waiting for notification loop to complete...")
                await asyncio.sleep(0.5)  # Allow pending notifications to complete
        current_task = asyncio.current_task()
        tasks = [task for task in asyncio.all_tasks() if task is not current_task]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
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
    use_resume: Annotated[
        bool,
        typer.Option(
            "--resume/--no-resume",  # Define both positive and negative flags
            "-r",
            help="Resume the stream from the last known token",
            is_flag=True,  # Mark as a flag option
        ),
    ] = True,
):
    """
    DB Monitor App.
    This app monitors the database for changes and processes them accordingly.
    It uses a change stream to listen for changes in the database and processes
    them in real-time.
    Args:
        config_filename (str): The name of the config file (in a folder called ./config).
        resume (bool): Whether to resume the stream from the last known token.

    Returns:
        None
    """
    _ = InternalConfig(config_filename=config_filename, log_filename="db_monitor.log.jsonl")
    logger.info(
        f"{ICON}{Fore.WHITE} ✅ Database Monitor App. Started. Version: {__version__} on {InternalConfig().local_machine_name}{Style.RESET_ALL}",
        extra={"notification": True},
    )
    logger.info(
        f"{ICON} Database Monitor App. Config file: {config_filename}",
        extra={"notification": False},
    )

    asyncio.run(main_async_start(use_resume=use_resume))


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

# --- IGNORE ---
