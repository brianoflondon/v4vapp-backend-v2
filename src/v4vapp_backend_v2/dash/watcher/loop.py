from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from bson import ObjectId

from v4vapp_backend_v2.config.setup import DashConnectionConfig, logger
from v4vapp_backend_v2.dash.amounts import rpc_dash_to_duffs, to_decimal
from v4vapp_backend_v2.dash.collections import COL_INVOICES
from v4vapp_backend_v2.dash.dashd.rpc import Dashd, DashdError
from v4vapp_backend_v2.dash.db.wallet_state import persist_watcher_heartbeat
from v4vapp_backend_v2.dash.models.invoice import DashInvoiceState
from v4vapp_backend_v2.dash.watcher.settlement import (
    WATCH_STATES,
    Decision,
    WatchedOutput,
    apply_settlement,
    merge_outputs,
)
from v4vapp_backend_v2.helpers.general_purpose_funcs import convert_decimals_for_mongodb

ICON = "💠"


@dataclass
class WatcherState:
    last_tick_at: datetime | None = None
    last_error: str | None = None
    open_invoices: int = 0
    last_id: ObjectId | None = None
    ticks: int = 0
    stuck: int = 0


def _invoice_extra(inv: dict[str, Any], decision: Decision) -> dict[str, Any]:
    return {
        "invoice_id": str(inv.get("_id")),
        "external_id": inv.get("external_id"),
        "address": inv.get("address"),
        "state": decision.state.value,
        "duffs_received": decision.duffs_received,
    }


async def run_watcher(
    *,
    db: Any,
    dashd: Dashd,
    conn: DashConnectionConfig,
    state: WatcherState,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        try:
            await tick(db=db, dashd=dashd, conn=conn, state=state)
            state.last_error = None
        except Exception as exc:
            state.last_error = str(exc)
            logger.warning(
                f"{ICON} watcher tick failed: {exc}",
                extra={"ticks": state.ticks, "notification": False},
            )
        await _persist_heartbeat(db, dashd, conn, state)
        try:
            await asyncio.wait_for(stop.wait(), timeout=conn.poll_interval_s)
        except TimeoutError:
            continue


async def _persist_heartbeat(
    db: Any, dashd: Dashd, conn: DashConnectionConfig, state: WatcherState
) -> None:
    try:
        dashd_info = await _snapshot_dashd(dashd)
        await persist_watcher_heartbeat(
            db,
            network=conn.network,
            watcher={
                "last_tick_at": state.last_tick_at,
                "last_error": state.last_error,
                "open_invoices": state.open_invoices,
                "ticks": state.ticks,
                "stuck": state.stuck,
            },
            dashd=dashd_info,
        )
    except Exception as exc:
        logger.warning(
            f"{ICON} watcher heartbeat persist failed: {exc}",
            extra={"notification": False},
        )


async def _snapshot_dashd(dashd: Dashd) -> dict[str, Any]:
    try:
        info = await dashd.getblockchaininfo()
        ibd = bool(info.get("initialblockdownload"))
        return {
            "chain": info.get("chain"),
            "blocks": info.get("blocks"),
            "headers": info.get("headers"),
            "verificationprogress": info.get("verificationprogress"),
            "initialblockdownload": ibd,
            "pruned": info.get("pruned"),
            "synced": not ibd,
        }
    except Exception:
        return {"error": True}


async def tick(
    *,
    db: Any,
    dashd: Dashd,
    conn: DashConnectionConfig,
    state: WatcherState,
) -> None:
    invoices = await _load_batch(db, conn.watch_batch, state.last_id)
    if len(invoices) < conn.watch_batch:
        state.last_id = None
    elif invoices:
        state.last_id = invoices[-1]["_id"]
    watching = {DashInvoiceState.OPEN, DashInvoiceState.DETECTED}
    state.open_invoices = sum(1 for inv in invoices if inv.get("state") in watching)

    addresses = [inv["address"] for inv in invoices if inv.get("address")]
    utxos_by_addr: dict[str, list[dict[str, Any]]] = {addr: [] for addr in addresses}
    if addresses:
        try:
            raw = await dashd.listunspent(0, 9999999, addresses, True)
        except DashdError as exc:
            state.last_error = str(exc)
            state.last_tick_at = datetime.now(UTC)
            state.ticks += 1
            logger.warning(
                f"{ICON} listunspent failed: {exc}", extra={"notification": False}
            )
            return
        for utxo in raw:
            addr = utxo.get("address")
            if addr in utxos_by_addr:
                utxos_by_addr[addr].append(utxo)

    now = datetime.now(UTC)
    for inv in invoices:
        utxos = utxos_by_addr.get(inv["address"], [])
        await _apply_invoice(db, dashd, conn, state, inv, utxos, now)

    state.last_tick_at = now
    state.ticks += 1


async def _load_batch(db: Any, limit: int, after: ObjectId | None) -> list[dict[str, Any]]:
    query: dict[str, Any] = {
        "swept_at": None,
        "state": {"$in": [s.value for s in WATCH_STATES]},
    }
    if after is not None:
        query["_id"] = {"$gt": after}
    cursor = db[COL_INVOICES].find(query).sort([("_id", 1)]).limit(limit)
    return [doc async for doc in cursor]


async def _apply_invoice(
    db: Any,
    dashd: Dashd,
    conn: DashConnectionConfig,
    watcher: WatcherState,
    inv: dict[str, Any],
    utxos: list[dict[str, Any]],
    now: datetime,
) -> None:
    fresh: list[WatchedOutput] = []
    for utxo in utxos:
        duffs = rpc_dash_to_duffs(utxo.get("amount", 0))
        if duffs < to_decimal(conn.dust_duffs):
            continue
        out = await _enrich(dashd, utxo, duffs, now)
        fresh.append(out)

    existing_txids = list(inv.get("txids") or [])
    outputs = merge_outputs(existing_txids, fresh)
    prev = DashInvoiceState(inv["state"])
    decision = apply_settlement(
        state=prev,
        now=now,
        expires_at=inv["expires_at"],
        settle_deadline_at=inv["settle_deadline_at"],
        duffs_quoted=to_decimal(inv["duffs_quoted"]),
        sats_requested=to_decimal(inv["sats_requested"]),
        policy=inv.get("policy") or {},
        outputs=outputs,
        canceled=prev == DashInvoiceState.CANCELED,
    )
    if decision.stuck:
        watcher.stuck += 1
        logger.error(
            f"{ICON} invoice {inv.get('_id')} stuck pending settlement past settle_deadline_at",
            extra=_invoice_extra(inv, decision),
        )

    prev_received = to_decimal(inv.get("duffs_received") or 0)
    late_states = {DashInvoiceState.EXPIRED, DashInvoiceState.CANCELED}
    if prev in late_states and decision.duffs_received > prev_received:
        logger.error(
            f"{ICON} late payment on {prev.value} {inv.get('address')} "
            f"duffs_received={decision.duffs_received}",
            extra=_invoice_extra(inv, decision),
        )

    await _persist(db, inv, prev, decision, now)
    if decision.state in {DashInvoiceState.SETTLED, DashInvoiceState.OVERPAID}:
        extra = _invoice_extra(inv, decision)
        if decision.txids:
            extra["txid"] = decision.txids[-1].get("txid")
        logger.info(
            f"{ICON} Dash invoice settled {inv.get('_id')} duffs: {decision.duffs_received:,.0f} sats: {decision.sats_credited:,.0f}",
            extra=extra,
        )


async def _enrich(
    dashd: Dashd, utxo: dict[str, Any], duffs: Decimal, now: datetime
) -> WatchedOutput:
    instantlock = bool(utxo.get("instantlock"))
    chainlock = bool(utxo.get("chainlock"))
    confirmations = int(utxo.get("confirmations") or 0)
    try:
        tx = await dashd.gettransaction(str(utxo["txid"]))
        instantlock = bool(tx.get("instantlock"))
        chainlock = bool(tx.get("chainlock"))
        confirmations = int(tx.get("confirmations") or confirmations)
    except DashdError:
        pass
    return WatchedOutput(
        txid=str(utxo["txid"]),
        vout=int(utxo["vout"]),
        duffs=duffs,
        confirmations=confirmations,
        instantlock=instantlock,
        chainlock=chainlock,
        detected_at=now,
    )


async def _persist(
    db: Any,
    inv: dict[str, Any],
    prev: DashInvoiceState,
    decision: Decision,
    now: datetime,
) -> None:
    allowed = {prev.value}
    if decision.state in {
        DashInvoiceState.SETTLED,
        DashInvoiceState.OVERPAID,
        DashInvoiceState.UNDERPAID,
    }:
        allowed = {DashInvoiceState.OPEN.value, DashInvoiceState.DETECTED.value}
    elif decision.state == DashInvoiceState.EXPIRED:
        allowed = {DashInvoiceState.OPEN.value}
    elif decision.state == DashInvoiceState.DETECTED:
        allowed = {DashInvoiceState.OPEN.value, DashInvoiceState.DETECTED.value}

    update = {
        "state": decision.state.value,
        "first_seen_at": decision.first_seen_at,
        "detected_at": decision.detected_at,
        "settled_at": decision.settled_at,
        "duffs_received": decision.duffs_received,
        "sats_credited": decision.sats_credited,
        "late_payment": decision.late_payment,
        "txids": decision.txids,
        "updated_at": now,
    }
    filt: dict[str, Any] = {"_id": inv["_id"], "state": {"$in": list(allowed)}}
    if decision.state in {DashInvoiceState.EXPIRED, DashInvoiceState.CANCELED} and prev in {
        DashInvoiceState.EXPIRED,
        DashInvoiceState.CANCELED,
    }:
        filt = {"_id": inv["_id"], "state": prev.value}

    await db[COL_INVOICES].find_one_and_update(
        filt, {"$set": convert_decimals_for_mongodb(update)}
    )
