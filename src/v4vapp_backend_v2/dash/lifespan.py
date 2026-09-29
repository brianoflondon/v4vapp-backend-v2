"""api_v2 Dash startup: Mongo + xpub only. dashd watching lives in dash_monitor."""

from __future__ import annotations

from fastapi import FastAPI

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.dash.runtime import ensure_dash_db
from v4vapp_backend_v2.dash.watcher.loop import WatcherState

ICON = "💠"


def _init_dash_state(app: FastAPI) -> None:
    app.state.dash_conn = None
    app.state.dashd = None
    app.state.dash_db = None
    app.state.dash_material = None
    app.state.watcher = WatcherState()


async def start_dash(app: FastAPI) -> None:
    """Prepare Dash invoice API state. Safe if unconfigured. Does not start the watcher."""
    _init_dash_state(app)
    conn = InternalConfig().config.dash_config.connection_config()
    if conn is None:
        logger.info(f"{ICON} dash not configured")
        return

    app.state.dash_conn = conn
    db = InternalConfig.db
    app.state.dash_db = db
    app.state.dash_material = await ensure_dash_db(db, conn)
    logger.info(f"{ICON} dash invoice API ready", extra={"network": conn.network})


async def stop_dash(app: FastAPI) -> None:
    app.state.dashd = None
