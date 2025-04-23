import asyncio
import signal
import sys
from typing import Annotated, Optional

import typer

from lnd_monitor_v2 import InternalConfig, logger

INTERNAL_CONFIG = InternalConfig()
CONFIG = INTERNAL_CONFIG.config
ICON = "🏆"
app = typer.Typer()

# Define a global flag to track shutdown
shutdown_event = asyncio.Event()


def handle_shutdown_signal():
    """
    Signal handler to set the shutdown event.
    """
    logger.info("Received shutdown signal. Setting shutdown event.")
    shutdown_event.set()


async def main_async_start(database_connection: str, db_name: str, lnd_connection: str):
    """
    Main function to run Template app.
    Args:
        node (str): example command line param.

    Returns:
        None
    """
    logger.info(
        f"🔗 Database connection: {database_connection} "
        f"🔗 Database name: {db_name} "
        f"🔗 Lightning node: {lnd_connection} "
    )
    loop = asyncio.get_event_loop()
    # Register signal handlers for SIGTERM and SIGINT
    loop.add_signal_handler(signal.SIGTERM, handle_shutdown_signal)
    loop.add_signal_handler(signal.SIGINT, handle_shutdown_signal)
    try:
        logger.info(f"{ICON} Template App started.")
        # Simulate some work
        while not shutdown_event.is_set():
            await asyncio.sleep(1)
            logger.info("Working...")
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info(f"{ICON} 👋 Received signal to stop. Exiting...")
        logger.info(f"{ICON} 👋 Goodbye! from Template App", extra={"notification": True})
    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False})
        logger.error(f"{ICON} Irregular shutdown in Template App {e}", extra={"error": e})
        raise e
    finally:
        logger.info(f"{ICON} Cleaning up resources...")


@app.command()
def main(
    database_connection: Annotated[
        str | None,
        typer.Argument(
            help=(f"The database connection to use. Choose from: {CONFIG.db_connections_names}")
        ),
    ] = CONFIG.default_db_connection,
    db_name: Annotated[
        Optional[str],
        typer.Argument(help=(f"The database to monitor.Choose from: {CONFIG.dbs_names}")),
    ] = CONFIG.default_db_name,
    lnd_connection: Annotated[
        Optional[str],
        typer.Argument(
            help=(
                f"The Lightning node to monitor. If not provided, "
                f"defaults to the value: "
                f"{CONFIG.default_lnd_connection}.\n"
                f"Choose from: {CONFIG.lnd_connections_names}"
            )
        ),
    ] = CONFIG.default_lnd_connection,
):
    f"""
    Main function to do what you want.
    Args:
        node (Annotated[Optional[str], Argument]): The node to monitor.
        Choose from:
        connections: {CONFIG.lnd_connections_names}
        databases: {CONFIG.dbs_names}

    Returns:
        None
    """
    icon = ICON
    logger.info(f"{icon} ✅ Template App. Started. Version: {CONFIG.min_version}")

    asyncio.run(main_async_start(database_connection, db_name, lnd_connection))


if __name__ == "__main__":
    try:
        logger.name = "template_app"
        app()
        print("👋 Goodbye!")
    except KeyboardInterrupt:
        print("👋 Goodbye!")
        sys.exit(0)

    except Exception as e:
        logger.exception(e)
        sys.exit(1)
