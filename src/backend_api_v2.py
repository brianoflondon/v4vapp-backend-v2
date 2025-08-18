import asyncio
import signal
import sys
from typing import Annotated

import typer
import uvicorn
from fastapi import FastAPI

from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.config.setup import DEFAULT_CONFIG_FILENAME, InternalConfig, logger

ICON = "🧩"
app = typer.Typer()

# Define the FastAPI app
api_app = FastAPI()


@api_app.get("/hello_world")
async def hello_world():
    return {"status": "OK"}


# Define a global flag to track shutdown
shutdown_event = asyncio.Event()


def handle_shutdown_signal():
    """
    Signal handler to set the shutdown event.
    """
    logger.info(f"{ICON} Received shutdown signal. Setting shutdown event.")
    shutdown_event.set()


async def main_async_start():
    """
    Main function to run Template app with FastAPI.
    Args:
        node (str): example command line param.

    Returns:
        None
    """
    CONFIG = InternalConfig().config
    logger.info(
        f"{ICON} Notification bot: {CONFIG.logging.default_notification_bot_name} "
        f"🔗 Database connection: {CONFIG.dbs_config.default_connection} "
        f"🔗 Database name: {CONFIG.dbs_config.default_name} "
    )
    loop = asyncio.get_event_loop()
    # Register signal handlers for SIGTERM and SIGINT
    loop.add_signal_handler(signal.SIGTERM, handle_shutdown_signal)
    loop.add_signal_handler(signal.SIGINT, handle_shutdown_signal)
    try:
        logger.info(f"{ICON} Template App started.")

        # Configure and run Uvicorn server
        uvicorn_config = uvicorn.Config(app=api_app, host="0.0.0.0", port=8000, log_level="info")
        server = uvicorn.Server(config=uvicorn_config)

        # Start the server in a task
        server_task = asyncio.create_task(server.serve())

        # Wait for shutdown event
        await shutdown_event.wait()

        # Initiate server shutdown
        server.should_exit = True

        # Wait for server to fully shut down
        await server_task

    except (asyncio.CancelledError, KeyboardInterrupt):
        InternalConfig.notification_lock = True
        logger.info(f"{ICON} 👋 Received signal to stop. Exiting...")
        logger.info(f"{ICON} 👋 Goodbye! from Template App", extra={"notification": True})
    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False})
        logger.error(f"{ICON} Irregular shutdown in Template App {e}", extra={"error": e})
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
    _ = InternalConfig(config_filename=config_filename, log_filename="backend_api.jsonl")
    logger.info(
        f"{ICON} ✅ Backend API. Started. Version: {__version__}", extra={"notification": True}
    )

    asyncio.run(main_async_start())


if __name__ == "__main__":
    try:
        logger.name = "backend_api_v2"
        app()
        print("👋 Goodbye!")
    except KeyboardInterrupt:
        print("👋 Goodbye!")
        sys.exit(0)

    except Exception as e:
        logger.exception(e)
        sys.exit(1)
