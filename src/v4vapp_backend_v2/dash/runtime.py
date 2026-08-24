"""Shared Dash node + health helpers used by api_v2 and dash_monitor."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import httpx

from v4vapp_backend_v2.config.setup import DashConnectionConfig, logger
from v4vapp_backend_v2.dash.dashd.bootstrap import bootstrap_watch_wallet
from v4vapp_backend_v2.dash.dashd.rpc import Dashd, DashdError
from v4vapp_backend_v2.dash.db.indexes import ensure_indexes
from v4vapp_backend_v2.dash.db.wallet_state import ensure_wallet_state
from v4vapp_backend_v2.dash.errors import ApiError
from v4vapp_backend_v2.dash.keys import check_dash_spend_keys, load_xpub_material
from v4vapp_backend_v2.dash.models.wallet import XpubMaterial
from v4vapp_backend_v2.dash.watcher.loop import WatcherState, run_watcher

ICON = "💠"
_RPC_RETRY_S = 10.0


@asynccontextmanager
async def dashd_session(
    conn: DashConnectionConfig, existing: Dashd | None = None
) -> AsyncIterator[Dashd]:
    """Reuse app.state.dashd if tests injected one; otherwise a short-lived RPC client."""
    if existing is not None:
        yield existing
        return
    if not conn.rpc_configured:
        raise ApiError(503, "wallet_unavailable", "Dash RPC is not configured")
    dashd = Dashd(
        conn.rpc_url,
        user=conn.rpc_user,
        password=conn.rpc_password,
        wallet=conn.rpc_wallet,
    )
    try:
        yield dashd
    finally:
        await dashd.aclose()


async def connect_dashd(conn: DashConnectionConfig, material: XpubMaterial | None) -> Dashd:
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


async def ensure_dash_db(db: Any, conn: DashConnectionConfig) -> XpubMaterial | None:
    """Indexes + HD wallet_state. Safe to call from both api_v2 and dash_monitor."""
    try:
        await ensure_indexes(db)
    except Exception as exc:
        logger.warning(f"{ICON} dash index setup failed: {exc}")
    material = load_xpub_material(conn)
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
    log_dash_spend_keys(conn, material)
    return material


def log_dash_spend_keys(conn: DashConnectionConfig, material: XpubMaterial | None) -> None:
    """Log fingerprint and payout readiness. Never logs mnemonic or xpub."""
    check = check_dash_spend_keys(conn)
    fp = check.fingerprint or (
        material.master_fingerprint.lower() if material is not None else ""
    )
    extra = {
        "network": conn.network,
        "fingerprint": fp,
        "payouts_enabled": check.payouts_enabled,
        "can_sign": check.can_sign,
        "notification": False,
    }
    if check.problem:
        logger.error(
            f"{ICON} dash payouts not ready fingerprint={fp} {check.problem}",
            extra={**extra, "err": check.problem},
        )
        return
    if check.can_sign:
        logger.info(
            f"{ICON} dash payouts ready fingerprint={fp} network={conn.network}",
            extra=extra,
        )
    else:
        logger.info(
            f"{ICON} dash watch-only fingerprint={fp} network={conn.network}",
            extra=extra,
        )


async def run_dashd_with_reconnect(
    *,
    db: Any,
    conn: DashConnectionConfig,
    material: XpubMaterial | None,
    watcher: WatcherState,
    stop: asyncio.Event,
) -> None:
    """Retry dashd until it answers, then poll invoices until `stop`."""
    delay = _RPC_RETRY_S
    dashd: Dashd | None = None
    try:
        while not stop.is_set():
            if dashd is None:
                try:
                    dashd = await connect_dashd(conn, material)
                    logger.info(f"{ICON} dashd rpc connected", extra={"rpc_url": conn.rpc_url})
                    delay = _RPC_RETRY_S
                except (httpx.TransportError, DashdError) as exc:
                    logger.warning(
                        f"{ICON} dashd rpc unavailable",
                        extra={"rpc_url": conn.rpc_url, "err": str(exc)},
                    )
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=delay)
                        return
                    except TimeoutError:
                        delay = min(60.0, delay * 2)
                        continue
            await run_watcher(db=db, dashd=dashd, conn=conn, state=watcher, stop=stop)
            return
    finally:
        if dashd is not None:
            await dashd.aclose()


def watcher_info_from_state(watcher: WatcherState, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    age = None
    if watcher.last_tick_at is not None:
        age = (now - watcher.last_tick_at).total_seconds()
    return {
        "last_tick_age_s": age,
        "open_invoices": watcher.open_invoices,
        "last_error": watcher.last_error,
    }


def watcher_info_from_doc(
    heartbeat: dict[str, Any] | None, now: datetime | None = None
) -> dict[str, Any] | None:
    if not heartbeat:
        return None
    now = now or datetime.now(UTC)
    last = heartbeat.get("last_tick_at")
    age = None
    if last is not None:
        if getattr(last, "tzinfo", None) is None:
            last = last.replace(tzinfo=UTC)
        age = (now - last).total_seconds()
    return {
        "last_tick_age_s": age,
        "open_invoices": heartbeat.get("open_invoices"),
        "last_error": heartbeat.get("last_error"),
    }


def assemble_dash_health(
    *,
    version: str,
    conn: DashConnectionConfig | None,
    mongo_ok: bool | None,
    wallet: dict[str, Any] | None = None,
    dashd_info: dict[str, Any] | None = None,
    watcher_info: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rpc_wanted = bool(conn is not None and conn.rpc_configured)
    poll_s = conn.poll_interval_s if conn is not None else 10
    ibd = bool(dashd_info is not None and dashd_info.get("initialblockdownload"))
    last_error = watcher_info.get("last_error") if watcher_info else None
    age = watcher_info.get("last_tick_age_s") if watcher_info else None
    stale = rpc_wanted and (age is None or age > max(30.0, float(poll_s) * 3))

    if mongo_ok is False or (dashd_info is not None and dashd_info.get("error")):
        status = "error"
    elif ibd or last_error or stale:
        status = "degraded"
    elif conn is None:
        status = "disabled"
    else:
        status = "ok"

    body: dict[str, Any] = {
        "status": status,
        "version": version,
        "network": conn.network if conn is not None else None,
        "mongo": mongo_ok,
    }
    if wallet is not None:
        body["wallet"] = wallet
    if dashd_info is not None:
        body["dashd"] = dashd_info
    if watcher_info is not None:
        body["watcher"] = watcher_info
    return body
