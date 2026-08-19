from __future__ import annotations

import asyncio
from typing import Any

import httpx
from fastapi import FastAPI

from v4vapp_backend_v2.config.setup import DashConnectionConfig, InternalConfig, logger
from v4vapp_backend_v2.dash.dashd.bootstrap import bootstrap_watch_wallet
from v4vapp_backend_v2.dash.dashd.rpc import Dashd, DashdError
from v4vapp_backend_v2.dash.db.indexes import ensure_indexes
from v4vapp_backend_v2.dash.db.wallet_state import ensure_wallet_state
from v4vapp_backend_v2.dash.keys import load_xpub_material
from v4vapp_backend_v2.dash.models.wallet import XpubMaterial
from v4vapp_backend_v2.dash.watcher.loop import WatcherState, run_watcher

ICON = "💠"
_RPC_RETRY_S = 10.0


def _init_dash_state(app: FastAPI) -> None:
    app.state.dash_conn = None
    app.state.dashd = None
    app.state.dash_db = None
    app.state.dash_material = None
    app.state.watcher = WatcherState()
    app.state.dash_stop = None
    app.state.dash_bg = []


async def _connect_dashd(conn: DashConnectionConfig, material: XpubMaterial | None) -> Dashd:
    dashd = Dashd(
        conn.rpc_url,
        user=conn.rpc_user,
        password=conn.rpc_password,
        wallet=conn.rpc_wallet,
    )
    try:
        await dashd.getblockchaininfo()
        if material is not None:
            await bootstrap_watch_wallet(
                dashd,
                network=conn.network,
                account_xpub=material.account_xpub,
                fingerprint=material.master_fingerprint,
                range_end=conn.descriptor_range_end,
            )
        return dashd
    except Exception:
        await dashd.aclose()
        raise


async def _dashd_reconnect(
    app: FastAPI,
    *,
    conn: DashConnectionConfig,
    material: XpubMaterial | None,
    db: Any,
    watcher: WatcherState,
    stop: asyncio.Event,
) -> None:
    """Retry dashd until it answers, then start the invoice watcher."""
    delay = _RPC_RETRY_S
    while not stop.is_set() and getattr(app.state, "dashd", None) is None:
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
            return
        except TimeoutError:
            pass
        try:
            dashd = await _connect_dashd(conn, material)
        except (httpx.TransportError, DashdError) as exc:
            logger.warning(
                f"{ICON} dashd rpc unavailable",
                extra={"rpc_url": conn.rpc_url, "err": str(exc)},
            )
            delay = min(60.0, delay * 2)
            continue
        app.state.dashd = dashd
        logger.info(f"{ICON} dashd rpc connected", extra={"rpc_url": conn.rpc_url})
        if db is not None:
            await run_watcher(db=db, dashd=dashd, conn=conn, state=watcher, stop=stop)
        return


async def start_dash(app: FastAPI) -> None:
    """Connect dashd, ensure indexes, and start the watcher. Safe if unconfigured."""
    _init_dash_state(app)
    conn = InternalConfig().config.dash_config.connection_config()
    if conn is None or not conn.rpc_configured:
        logger.info(f"{ICON} dash not configured")
        return

    app.state.dash_conn = conn
    db = InternalConfig.db
    app.state.dash_db = db
    try:
        await ensure_indexes(db)
    except Exception as exc:
        logger.warning(f"{ICON} dash index setup failed: {exc}")

    material = load_xpub_material(conn)
    app.state.dash_material = material
    if material is not None:
        try:
            await ensure_wallet_state(
                db,
                network=conn.network,
                account_xpub=material.account_xpub,
                fingerprint=material.master_fingerprint,
                descriptor_range_end=conn.descriptor_range_end,
            )
        except Exception as exc:
            logger.warning(f"{ICON} dash wallet state setup failed: {exc}")

    dashd: Dashd | None = None
    try:
        dashd = await _connect_dashd(conn, material)
        logger.info(f"{ICON} dashd rpc connected", extra={"rpc_url": conn.rpc_url})
    except (httpx.TransportError, DashdError) as exc:
        logger.warning(
            f"{ICON} dashd rpc unavailable",
            extra={"rpc_url": conn.rpc_url, "err": str(exc)},
        )
    app.state.dashd = dashd

    watcher: WatcherState = app.state.watcher
    stop = asyncio.Event()
    app.state.dash_stop = stop
    bg: list[asyncio.Task[None]] = []
    if dashd is not None:
        bg.append(
            asyncio.create_task(
                run_watcher(db=db, dashd=dashd, conn=conn, state=watcher, stop=stop)
            )
        )
    else:
        bg.append(
            asyncio.create_task(
                _dashd_reconnect(
                    app,
                    conn=conn,
                    material=material,
                    db=db,
                    watcher=watcher,
                    stop=stop,
                )
            )
        )
    app.state.dash_bg = bg


async def stop_dash(app: FastAPI) -> None:
    stop = getattr(app.state, "dash_stop", None)
    if stop is not None:
        stop.set()
    for task in getattr(app.state, "dash_bg", []) or []:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    live = getattr(app.state, "dashd", None)
    if live is not None:
        await live.aclose()
        app.state.dashd = None
