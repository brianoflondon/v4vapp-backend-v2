import asyncio
import signal
import sys
from typing import Annotated

import typer

from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.config.setup import DEFAULT_CONFIG_FILENAME, InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.magi.magi_classes import DB_MAGI_BTC_COLLECTION
from v4vapp_backend_v2.magi.stream_magi import stream_magi_transfer_events

ICON = "🧙‍♂️"
app = typer.Typer()

# Define a global flag to track shutdown
shutdown_event = asyncio.Event()


def handle_shutdown_signal():
    """
    Signal handler to set the shutdown event.
    """
    logger.info(f"{ICON} Received shutdown signal. Setting shutdown event.")
    shutdown_event.set()


async def get_last_indexer_id() -> int:
    """
    Query the database for the highest saved indexer_id to allow resuming.

    Returns:
        int: The last seen indexer_id, or 0 if no records exist.
    """
    try:
        collection = InternalConfig.db[DB_MAGI_BTC_COLLECTION]
        doc = await collection.find_one(
            filter={},
            sort=[("indexer_id", -1)],
        )
        if doc:
            return int(doc.get("indexer_id", 0))
    except Exception as e:
        logger.warning(
            f"{ICON} Could not retrieve last indexer_id from DB: {e}",
            extra={"notification": False},
        )
    return 0


async def main_async_start(from_indexer_id: int = 0) -> None:
    """
    Main async function to run the Magi BTC Transfer Monitor.

    Streams BTC transfer events from the MAGI GraphQL WebSocket endpoint and
    persists each event to the magi_btc MongoDB collection. Automatically
    resumes from the last saved indexer_id if from_indexer_id is 0.

    Args:
        from_indexer_id (int): Start scanning from this indexer_id.
            If 0, resumes from the last saved position in the database.
    """
    CONFIG = InternalConfig().config
    logger.info(
        f"{ICON} Magi Monitor: {CONFIG.logging.default_notification_bot_name} "
        f"🔗 Database: {CONFIG.dbs_config.default_name}"
    )
    loop = asyncio.get_event_loop()
    # Register signal handlers for SIGTERM and SIGINT
    loop.add_signal_handler(signal.SIGTERM, handle_shutdown_signal)
    loop.add_signal_handler(signal.SIGINT, handle_shutdown_signal)
    try:
        # If from_indexer_id is -1, try to resume from the last saved position
        cursor_id = from_indexer_id
        if cursor_id == -1:
            cursor_id = await get_last_indexer_id()
            if cursor_id:
                logger.info(f"{ICON} Resuming from last saved indexer_id={cursor_id}")
            else:
                logger.info(f"{ICON} No saved position found, streaming from the beginning")

        logger.info(f"{ICON} Magi BTC Transfer Monitor started from indexer_id={cursor_id}.")

        async def stream_task() -> None:
            async for event in stream_magi_transfer_events(from_indexer_id=cursor_id):
                if shutdown_event.is_set():
                    break
                try:
                    await event.update_conv()
                    await event.save()
                    logger.info(event.log_str, extra={"notification": False})
                except Exception as e:
                    logger.error(
                        f"{ICON} Failed to save event indexer_id={event.indexer_id}: {e}",
                        extra={"notification": False},
                    )

        stream = asyncio.create_task(stream_task(), name="magi_stream")

        # Wait until shutdown is requested
        await shutdown_event.wait()
        stream.cancel()
        await asyncio.gather(stream, return_exceptions=True)

    except (asyncio.CancelledError, KeyboardInterrupt):
        InternalConfig.notification_lock = True
        logger.info(f"{ICON} 👋 Received signal to stop. Exiting...")
        logger.info(f"{ICON} 👋 Goodbye! from Magi Monitor", extra={"notification": True})
    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False})
        logger.error(f"{ICON} Irregular shutdown in Magi Monitor {e}", extra={"error": e})
        raise e
    finally:
        logger.info(f"{ICON} Cleaning up resources...")
        if hasattr(InternalConfig, "notification_loop"):
            while InternalConfig.notification_lock:
                logger.info("Waiting for notification loop to complete...")
                await asyncio.sleep(0.5)
        current_task = asyncio.current_task()
        tasks = [task for task in asyncio.all_tasks() if task is not current_task]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"{ICON} 👋 Goodbye! from Magi Monitor", extra={"notification": True})
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
    from_indexer_id: Annotated[
        int,
        typer.Option(
            "--from-indexer-id",
            help="Start scanning from this indexer_id (-1 = resume from last saved position in DB)",
            show_default=True,
        ),
    ] = -1,
):
    """
    Magi BTC Transfer Monitor.

    Streams BTC transfer events from the MAGI indexer GraphQL WebSocket and
    stores each event in the magi_btc MongoDB collection.
    """
    _ = InternalConfig(config_filename=config_filename)
    db_conn = DBConn()
    asyncio.run(db_conn.setup_database())
    logger.info(
        f"{ICON} ✅ Magi Monitor Started. Version: {__version__} "
        f"on {InternalConfig().local_machine_name}",
        extra={"notification": True},
    )
    asyncio.run(main_async_start(from_indexer_id=from_indexer_id))


if __name__ == "__main__":
    try:
        logger.name = "magi_monitor"
        app()
        print("👋 Goodbye!")
    except KeyboardInterrupt:
        print("👋 Goodbye!")
        sys.exit(0)
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
