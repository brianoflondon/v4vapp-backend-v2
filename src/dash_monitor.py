import asyncio
import os
import signal
import sys
from typing import Annotated, Any

import typer

from status.status_api import StatusAPI
from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.config.setup import DEFAULT_CONFIG_FILENAME, InternalConfig, logger
from v4vapp_backend_v2.dash.runtime import (
    assemble_dash_health,
    ensure_dash_db,
    run_dashd_with_reconnect,
    watcher_info_from_state,
)
from v4vapp_backend_v2.dash.watcher.loop import WatcherState
from v4vapp_backend_v2.database.db_pymongo import DBConn

ICON = "💠"
app = typer.Typer()
shutdown_event = asyncio.Event()
WATCHER_STATE = WatcherState()
TASK_NAME = "dash_watch"


async def health_check() -> dict[str, Any]:
    """StatusAPI health: the watch task must be running or Docker restarts us."""
    stream_task_running = any(
        t.get_name() == TASK_NAME and not t.done() for t in asyncio.all_tasks()
    )
    if not stream_task_running:
        logger.warning(
            f"{ICON} {TASK_NAME} task is not running",
            extra={"notification": True, "error_code": "dash_watch_task_failure"},
        )
        shutdown_event.set()
        raise RuntimeError(f"{TASK_NAME} task is not running")
    logger.debug(
        f"{ICON} Dash Monitor health check passed",
        extra={"notification": False, "error_code_clear": "dash_watch_task_failure"},
    )
    conn = InternalConfig().config.dash_config.connection_config()
    return assemble_dash_health(
        version=__version__ or "0.0.0",
        conn=conn,
        mongo_ok=True,
        watcher_info=watcher_info_from_state(WATCHER_STATE),
    )


def handle_shutdown_signal() -> None:
    logger.info(f"{ICON} Received shutdown signal. Setting shutdown event.")
    shutdown_event.set()


async def dash_watch() -> None:
    conn = InternalConfig().config.dash_config.connection_config()
    if conn is None or not conn.rpc_configured:
        logger.info(f"{ICON} dash not configured; watcher idle")
        await shutdown_event.wait()
        return
    db = InternalConfig.db
    material = await ensure_dash_db(db, conn)
    await run_dashd_with_reconnect(
        db=db,
        conn=conn,
        material=material,
        watcher=WATCHER_STATE,
        stop=shutdown_event,
    )


async def main_async_start() -> None:
    InternalConfig.notification_loop = asyncio.get_running_loop()
    logger.info(
        f"{ICON} ✅ Dash Monitor Started. Version: {__version__} "
        f"on {InternalConfig().local_machine_name}",
        extra={"notification": True},
    )
    await asyncio.sleep(1)
    CONFIG = InternalConfig().config
    db_conn = DBConn()
    await db_conn.setup_database()
    logger.info(
        f"{ICON} Dash Monitor: {CONFIG.logging.default_notification_bot_name} "
        f"🔗 Database: {CONFIG.dbs_config.default_name}"
    )
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, handle_shutdown_signal)
    loop.add_signal_handler(signal.SIGINT, handle_shutdown_signal)

    process_name = os.path.splitext(os.path.basename(__file__))[0]
    health_check_port = os.environ.get("HEALTH_CHECK_PORT", "6001")
    status_api = StatusAPI(
        port=int(health_check_port),
        health_check_func=health_check,
        shutdown_event=shutdown_event,
        process_name=process_name,
        version=__version__ or "0.0.0",
    )

    try:
        tasks = [
            asyncio.create_task(dash_watch(), name=TASK_NAME),
            asyncio.create_task(status_api.start(), name="status_api"),
        ]
        await shutdown_event.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    except (asyncio.CancelledError, KeyboardInterrupt):
        InternalConfig.notification_lock = True
        logger.info(f"{ICON} 👋 Received signal to stop. Exiting...")
        logger.info(f"{ICON} 👋 Goodbye! from Dash Monitor", extra={"notification": True})
    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False})
        logger.error(f"{ICON} Irregular shutdown in Dash Monitor {e}", extra={"error": e})
        raise
    finally:
        logger.info(f"{ICON} Cleaning up resources...")
        if hasattr(InternalConfig, "notification_loop"):
            while InternalConfig.notification_lock:
                logger.info("Waiting for notification loop to complete...")
                await asyncio.sleep(0.5)
        current_task = asyncio.current_task()
        leftover = [task for task in asyncio.all_tasks() if task is not current_task]
        for task in leftover:
            task.cancel()
        await asyncio.gather(*leftover, return_exceptions=True)
        logger.info(f"{ICON} 👋 Goodbye! from Dash Monitor", extra={"notification": True})
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
    Dash invoice watcher.

    Polls dashd for payments on open Dash invoices and writes state to
    `dash_invoices`. Invoice create/read stays on api_v2.
    """
    _ = InternalConfig(config_filename=config_filename)
    asyncio.run(main_async_start())


if __name__ == "__main__":
    try:
        logger.name = "dash_monitor"
        app()
        print("👋 Goodbye!")
    except KeyboardInterrupt:
        print("👋 Goodbye!")
        sys.exit(0)
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
