from datetime import UTC, datetime, timedelta
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Query, Request, Response
from pymongo.errors import DuplicateKeyError

from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.config.setup import DashConnectionConfig, logger
from v4vapp_backend_v2.dash.amounts import to_decimal
from v4vapp_backend_v2.dash.collections import COL_INVOICES
from v4vapp_backend_v2.dash.db.wallet_state import (
    WalletStateMismatch,
    allocate_receive_index,
    load_wallet_state,
)
from v4vapp_backend_v2.dash.errors import ApiError
from v4vapp_backend_v2.dash.keys import load_xpub_material
from v4vapp_backend_v2.dash.limits.check import check_cust_rate_limit
from v4vapp_backend_v2.dash.limits.hive_config import calculate_invoice_fees, fetch_hive_config
from v4vapp_backend_v2.dash.models.invoice import (
    DashInvoiceState,
    InvoiceCreate,
    InvoiceListOut,
    InvoiceOut,
    doc_to_out,
    payment_uri,
)
from v4vapp_backend_v2.dash.quotes.service import fetch_quote, quote_for_sats
from v4vapp_backend_v2.dash.runtime import (
    assemble_dash_health,
    watcher_info_from_doc,
    watcher_info_from_state,
)
from v4vapp_backend_v2.dash.settings import default_min_conf
from v4vapp_backend_v2.dash.wallet.hd import derive_receive
from v4vapp_backend_v2.dash.watcher.loop import WatcherState
from v4vapp_backend_v2.helpers.general_purpose_funcs import convert_decimals_for_mongodb

ICON = "💠"
router = APIRouter(prefix="/v2/dash", tags=["dash"])


def _db(request: Request) -> Any:
    db = getattr(request.app.state, "dash_db", None)
    if db is None:
        raise ApiError(503, "wallet_unavailable", "Mongo is not configured for Dash")
    return db


def _conn(request: Request) -> DashConnectionConfig:
    conn = getattr(request.app.state, "dash_conn", None)
    if conn is None:
        raise ApiError(503, "wallet_unavailable", "Dash is not configured")
    return conn


def _material_or_503(conn: DashConnectionConfig) -> Any:
    material = load_xpub_material(conn)
    if material is None:
        raise ApiError(503, "wallet_unavailable", "Dash xpub is not configured")
    return material


def _policy(conn: DashConnectionConfig, min_confirmations: int | None) -> dict[str, Any]:
    is_or_cl = conn.settle_policy == "instantsend_or_chainlock"
    if is_or_cl:
        min_conf = default_min_conf(conn.network)
    else:
        min_conf = min_confirmations if min_confirmations is not None else conn.min_conf
    return {
        "settle_policy": conn.settle_policy,
        "underpay_tolerance_duffs": conn.underpay_duffs,
        "underpay_tolerance_bps": conn.underpay_bps,
        "min_confirmations": min_conf,
        "accept_instantsend": is_or_cl,
        "accept_chainlock": is_or_cl,
    }


@router.get("/health")
async def dash_health(request: Request) -> dict[str, Any]:
    conn: DashConnectionConfig | None = getattr(request.app.state, "dash_conn", None)
    mongo_ok: bool | None = None
    wallet: dict[str, Any] | None = None
    dashd_info: dict[str, Any] | None = None
    watcher_info: dict[str, Any] | None = None
    db = getattr(request.app.state, "dash_db", None)
    in_proc: WatcherState | None = getattr(request.app.state, "watcher", None)

    if db is not None:
        try:
            client = getattr(db, "client", None)
            if client is not None:
                await client.admin.command("ping")
            mongo_ok = True
            if conn is not None:
                state = await load_wallet_state(db, conn.network)
                if state is not None:
                    wallet = {
                        "network": state.get("network"),
                        "next_receive_index": int(state.get("next_receive_index", 0)),
                        "descriptor_range_end": int(state.get("descriptor_range_end", 0)),
                    }
                    watcher_info = watcher_info_from_doc(state.get("watcher"))
                    dashd_info = state.get("dashd")
        except Exception:
            mongo_ok = False

    if in_proc is not None and in_proc.last_tick_at is not None:
        watcher_info = watcher_info_from_state(in_proc)

    return assemble_dash_health(
        version=__version__,
        conn=conn,
        mongo_ok=mongo_ok,
        wallet=wallet,
        dashd_info=dashd_info,
        watcher_info=watcher_info,
    )


@router.post("/invoices", status_code=201)
async def create_invoice(
    body: InvoiceCreate,
    request: Request,
    response: Response,
) -> InvoiceOut:
    request.state.external_id = body.external_id
    if body.cust_id is not None:
        request.state.cust_id = body.cust_id
    conn = _conn(request)
    db = _db(request)
    material = _material_or_503(conn)

    existing = await db[COL_INVOICES].find_one({"external_id": body.external_id})
    if existing is not None:
        same = (
            to_decimal(existing["sats_requested"]) == to_decimal(body.sats)
            and int(existing.get("expires_in_s", -1)) == body.expires_in_s
            and existing.get("lightning_invoice") == body.lightning_invoice
        )
        if same:
            response.status_code = 200
            request.state.invoice_id = str(existing["_id"])
            request.state.external_id = existing.get("external_id")
            if existing.get("cust_id") is not None:
                request.state.cust_id = existing["cust_id"]
            return doc_to_out(existing)
        raise ApiError(
            409, "duplicate_external_id", "external_id already used with different params"
        )

    hive = fetch_hive_config()
    if body.sats < hive.minimum_invoice_payment_sats:
        raise ApiError(
            422,
            "amount_too_small",
            f"Minimum invoice is {hive.minimum_invoice_payment_sats:,} sats",
        )
    if body.sats > hive.maximum_invoice_payment_sats:
        raise ApiError(
            422,
            "amount_too_large",
            f"Maximum invoice is {hive.maximum_invoice_payment_sats:,} sats",
        )

    fees = calculate_invoice_fees(body.sats, hive)
    try:
        raw_quote = await fetch_quote()
        priced = quote_for_sats(fees.sats_collect, raw_quote)
    except ApiError:
        raise
    except Exception as exc:
        raise ApiError(422, "quote_unavailable", str(exc)) from exc

    if body.cust_id:
        await check_cust_rate_limit(db, cust_id=body.cust_id, extra_sats=body.sats)

    try:
        index = await allocate_receive_index(db, conn.network)
    except WalletStateMismatch as exc:
        raise ApiError(503, "wallet_unavailable", str(exc)) from exc

    if index >= conn.descriptor_range_end:
        raise ApiError(503, "index_exhausted", "receive index is past the descriptor range")

    address, derivation = derive_receive(material.account_xpub, conn.network, index)
    now = datetime.now(UTC)
    expires_at = now + timedelta(seconds=body.expires_in_s)
    settle_deadline_at = expires_at + timedelta(seconds=conn.settle_grace_s)
    doc: dict[str, Any] = {
        "external_id": body.external_id,
        "cust_id": body.cust_id,
        "memo": body.memo,
        "lightning_invoice": body.lightning_invoice,
        "state": DashInvoiceState.OPEN.value,
        "address": address,
        "uri": payment_uri(address, priced.dash_quoted),
        "network": conn.network,
        "path": derivation.path,
        "account": derivation.account,
        "change": derivation.change,
        "index": index,
        "derivation": derivation.model_dump(),
        "sats_requested": body.sats,
        "sats_collect": fees.sats_collect,
        "expires_in_s": body.expires_in_s,
        "duffs_quoted": priced.duffs_quoted,
        "dash_quoted": priced.dash_quoted,
        "fees": {
            "conv_fee_percent": fees.conv_fee_percent,
            "conv_fee_base_sats": fees.conv_fee_base_sats,
            "conv_fee_sats": fees.conv_fee_sats,
            "routing_fee_sats": fees.routing_fee_sats,
            "total_fee_sats": fees.total_fee_sats,
            "sats_collect": fees.sats_collect,
        },
        "duffs_received": to_decimal(0),
        "sats_credited": None,
        "quote": {
            "source": priced.source,
            "fetched_at": priced.fetched_at,
            "btc_usd": str(priced.btc_usd),
            "dash_usd": str(priced.dash_usd),
            "dash_btc": str(priced.dash_btc),
            "sats_per_dash": str(priced.sats_per_dash),
            "ttl_s": priced.ttl_s,
        },
        "policy": _policy(conn, body.min_confirmations),
        "txids": [],
        "created_at": now,
        "expires_at": expires_at,
        "settle_deadline_at": settle_deadline_at,
        "first_seen_at": None,
        "detected_at": None,
        "settled_at": None,
        "canceled_at": None,
        "late_payment": False,
        "swept_at": None,
        "updated_at": now,
    }
    try:
        result = await db[COL_INVOICES].insert_one(convert_decimals_for_mongodb(doc))
    except DuplicateKeyError as exc:
        if body.lightning_invoice:
            clash = await db[COL_INVOICES].find_one({"lightning_invoice": body.lightning_invoice})
            if clash is not None and clash.get("external_id") != body.external_id:
                raise ApiError(
                    409,
                    "duplicate_lightning_invoice",
                    "lightning_invoice already used on another invoice",
                ) from exc
        raise ApiError(409, "duplicate_external_id", "external_id already exists") from exc
    doc["_id"] = result.inserted_id
    invoice_id = str(result.inserted_id)
    request.state.invoice_id = invoice_id
    request.state.external_id = body.external_id
    if body.cust_id is not None:
        request.state.cust_id = body.cust_id
    logger.info(
        f"{ICON} invoice created",
        extra={
            "invoice_id": invoice_id,
            "external_id": body.external_id,
            "cust_id": body.cust_id,
            "sats": body.sats,
            "state": DashInvoiceState.OPEN.value,
            "address": address,
        },
    )
    return doc_to_out(doc)


def _as_object_id(invoice_id: str) -> ObjectId:
    try:
        return ObjectId(invoice_id)
    except InvalidId as exc:
        raise ApiError(404, "not_found", "invoice not found") from exc


@router.get("/invoices/by-external/{external_id:path}")
async def get_by_external(external_id: str, request: Request) -> InvoiceOut:
    request.state.external_id = external_id
    db = _db(request)
    doc = await db[COL_INVOICES].find_one({"external_id": external_id})
    if doc is None:
        raise ApiError(404, "not_found", "invoice not found")
    request.state.invoice_id = str(doc["_id"])
    if doc.get("cust_id") is not None:
        request.state.cust_id = doc["cust_id"]
    return doc_to_out(doc)


@router.get("/invoices/{invoice_id}")
async def get_invoice(invoice_id: str, request: Request) -> InvoiceOut:
    request.state.invoice_id = invoice_id
    db = _db(request)
    doc = await db[COL_INVOICES].find_one({"_id": _as_object_id(invoice_id)})
    if doc is None:
        raise ApiError(404, "not_found", "invoice not found")
    if doc.get("external_id") is not None:
        request.state.external_id = doc["external_id"]
    if doc.get("cust_id") is not None:
        request.state.cust_id = doc["cust_id"]
    return doc_to_out(doc)


@router.get("/invoices")
async def list_invoices(
    request: Request,
    state: DashInvoiceState | None = None,
    cust_id: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = None,
) -> InvoiceListOut:
    db = _db(request)
    query: dict[str, Any] = {}
    if state is not None:
        query["state"] = state.value
    if cust_id is not None:
        query["cust_id"] = cust_id
    if cursor:
        created_raw, id_raw = _split_cursor(cursor)
        oid = _as_object_id(id_raw)
        query["$or"] = [
            {"created_at": {"$gt": created_raw}},
            {"created_at": created_raw, "_id": {"$gt": oid}},
        ]
    cursor_docs = (
        db[COL_INVOICES].find(query).sort([("created_at", 1), ("_id", 1)]).limit(limit + 1)
    )
    rows = [doc async for doc in cursor_docs]
    next_cursor = None
    if len(rows) > limit:
        last = rows[limit - 1]
        rows = rows[:limit]
        next_cursor = f"{last['created_at'].isoformat()}|{last['_id']}"
    return InvoiceListOut(items=[doc_to_out(doc) for doc in rows], next_cursor=next_cursor)


@router.post("/invoices/{invoice_id}/cancel")
async def cancel_invoice(invoice_id: str, request: Request) -> InvoiceOut:
    request.state.invoice_id = invoice_id
    db = _db(request)
    now = datetime.now(UTC)
    doc = await db[COL_INVOICES].find_one_and_update(
        {
            "_id": _as_object_id(invoice_id),
            "state": DashInvoiceState.OPEN.value,
            "first_seen_at": None,
        },
        {
            "$set": {
                "state": DashInvoiceState.CANCELED.value,
                "canceled_at": now,
                "updated_at": now,
            }
        },
        return_document=True,
    )
    if doc is None:
        existing = await db[COL_INVOICES].find_one({"_id": _as_object_id(invoice_id)})
        if existing is None:
            raise ApiError(404, "not_found", "invoice not found")
        if existing.get("external_id") is not None:
            request.state.external_id = existing["external_id"]
        raise ApiError(409, "invoice_not_cancelable", "invoice is not OPEN or already has funds")
    if doc.get("external_id") is not None:
        request.state.external_id = doc["external_id"]
    logger.info(
        f"{ICON} invoice canceled",
        extra={"invoice_id": str(doc["_id"]), "external_id": doc.get("external_id")},
    )
    return doc_to_out(doc)


@router.post("/payouts")
async def create_payout() -> None:
    raise ApiError(501, "not_implemented", "Outgoing payouts are not enabled")


def _split_cursor(cursor: str) -> tuple[datetime, str]:
    if "|" not in cursor:
        raise ApiError(400, "invalid_request", "cursor must be created_at|_id")
    created_raw, id_raw = cursor.split("|", 1)
    try:
        created = datetime.fromisoformat(created_raw)
    except ValueError as exc:
        raise ApiError(400, "invalid_request", "cursor timestamp is invalid") from exc
    return created, id_raw
