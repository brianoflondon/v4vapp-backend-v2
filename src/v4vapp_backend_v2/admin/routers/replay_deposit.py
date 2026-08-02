"""
Replay Hive Deposit admin router.

Recovers stuck HBD/HIVE deposits where cust_h_in succeeded but follow-on LN pay
failed (e.g. LND GetInfo timeout). Prefers re-running follow_on_transfer while
keeping the deposit ledger entry, after clearing replies that block retry.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

from fastapi import APIRouter, Body, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.accounting.sanity_checks import run_all_sanity_checks
from v4vapp_backend_v2.actions.lnurl_decode import decode_any_lightning_string
from v4vapp_backend_v2.actions.tracked_models import ReplyType
from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive_models.op_all import OP_MAP
from v4vapp_backend_v2.hive_models.op_transfer import Transfer, TransferBase
from v4vapp_backend_v2.hive_models.pending_transaction_class import PendingTransaction
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.process.process_transfer import HiveTransferError, follow_on_transfer

router = APIRouter()

templates: Jinja2Templates | None = None
nav_manager: NavigationManager | None = None

# Replies that do NOT block follow_on_transfer (see process_transfer.follow_on_transfer).
_NON_BLOCKING_REPLY_TYPES = {ReplyType.LEDGER_ERROR, ReplyType.CUSTOM_JSON}

# Failure / return noise we strip on "retry pay" (never strip successful payments).
_CLEARABLE_ON_RETRY = {
    ReplyType.TRANSFER,
    ReplyType.HIVE_ERROR,
    ReplyType.UNKNOWN,
    ReplyType.INVOICE,
    ReplyType.MAGI_TRANSFER,
}


def set_templates_and_nav(tmpl: Jinja2Templates, nav: NavigationManager) -> None:
    global templates, nav_manager
    templates = tmpl
    nav_manager = nav


def _serialize_reply(reply: Any) -> dict[str, Any]:
    if hasattr(reply, "model_dump"):
        data = reply.model_dump(mode="json")
    elif isinstance(reply, dict):
        data = dict(reply)
    else:
        data = {
            "reply_id": getattr(reply, "reply_id", None),
            "reply_type": getattr(reply, "reply_type", None),
            "reply_message": getattr(reply, "reply_message", None),
            "reply_error": getattr(reply, "reply_error", None),
            "reply_msat": getattr(reply, "reply_msat", 0),
        }
    rtype = data.get("reply_type")
    if hasattr(rtype, "value"):
        rtype = rtype.value
    data["reply_type"] = rtype
    data["blocks_follow_on"] = rtype not in {
        ReplyType.LEDGER_ERROR.value,
        ReplyType.CUSTOM_JSON.value,
        None,
        "",
    }
    # Empty reply_type is not in the allowlist of follow_on — treat as blocking if present.
    if not rtype:
        data["blocks_follow_on"] = True
    return data


def _serialize_ledger(entry: LedgerEntry) -> dict[str, Any]:
    return {
        "group_id": entry.group_id,
        "short_id": entry.short_id,
        "ledger_type": entry.ledger_type.value if entry.ledger_type else None,
        "description": entry.description,
        "user_memo": entry.user_memo or "",
        "cust_id": entry.cust_id or "",
        "timestamp": entry.timestamp.isoformat() if entry.timestamp else None,
        "debit_amount": str(entry.debit_amount),
        "debit_unit": entry.debit_unit.value if entry.debit_unit else None,
        "credit_amount": str(entry.credit_amount),
        "credit_unit": entry.credit_unit.value if entry.credit_unit else None,
        "reversed": entry.reversed.isoformat() if entry.reversed else None,
        "op_type": entry.op_type or "",
        "link": getattr(entry, "link", "") or "",
    }


async def _find_ledger_deposits(query: str, limit: int = 25) -> list[LedgerEntry]:
    """Find customer Hive/HBD deposit ledger rows by short_id, group_id, or cust_id."""
    q = (query or "").strip()
    if not q:
        return []

    coll = LedgerEntry.collection()
    deposit_type = LedgerType.CUSTOMER_HIVE_IN.value
    filters: list[dict[str, Any]] = []

    # Exact short_id / group_id
    filters.append({"short_id": q, "ledger_type": deposit_type})
    filters.append({"group_id": q, "ledger_type": deposit_type})
    # Prefix / contains for short_id fragments
    filters.append({
        "short_id": {"$regex": re.escape(q), "$options": "i"},
        "ledger_type": deposit_type,
    })
    # cust_id exact (Hive account name)
    if re.fullmatch(r"[a-z0-9\-\.]{3,16}", q.lower()):
        filters.append({
            "cust_id": q.lower(),
            "ledger_type": deposit_type,
        })

    seen: set[str] = set()
    results: list[LedgerEntry] = []
    for filt in filters:
        cursor = coll.find(filt).sort("timestamp", -1).limit(limit)
        async for doc in cursor:
            try:
                entry = LedgerEntry.model_validate(doc)
            except Exception:
                continue
            if entry.group_id in seen:
                continue
            seen.add(entry.group_id)
            results.append(entry)
            if len(results) >= limit:
                return results
    return results


async def _load_hive_op_doc(short_id: str | None, group_id: str | None) -> dict[str, Any] | None:
    db = InternalConfig.db
    hive_ops = db["hive_ops"]
    if group_id:
        doc = await hive_ops.find_one({"group_id": group_id})
        if doc:
            return doc
    if short_id:
        doc = await hive_ops.find_one({"short_id": short_id})
        if doc:
            return doc
        # short_id is computed; fall back to regex on group_id tail patterns if needed
        cursor = hive_ops.find({"short_id": {"$regex": f"^{re.escape(short_id)}$"}}).limit(1)
        async for d in cursor:
            return d
    return None


def _op_from_doc(doc: dict[str, Any]) -> TransferBase | None:
    op_type = doc.get("type") or doc.get("op_type") or ""
    cls = OP_MAP.get(op_type)
    if cls is None:
        # Best effort as Transfer
        try:
            return Transfer.model_validate(doc)
        except Exception:
            return None
    try:
        return cls.model_validate(doc)
    except Exception as e:
        logger.warning(f"Failed to validate hive op as {op_type}: {e}")
        try:
            return Transfer.model_validate(doc)
        except Exception:
            return None


async def _related_ledger(
    short_id: str, cust_id: str | None, limit: int = 40
) -> list[LedgerEntry]:
    """Same short_id first, then recent rows for the customer (dust outs often differ)."""
    coll = LedgerEntry.collection()
    seen: set[str] = set()
    out: list[LedgerEntry] = []

    async def _collect(filt: dict[str, Any], cap: int) -> None:
        cursor = coll.find(filt).sort("timestamp", -1).limit(cap)
        async for doc in cursor:
            try:
                entry = LedgerEntry.model_validate(doc)
            except Exception:
                continue
            if entry.group_id in seen:
                continue
            seen.add(entry.group_id)
            out.append(entry)

    if short_id:
        await _collect({"short_id": short_id}, limit)
    if cust_id and len(out) < limit:
        await _collect({"cust_id": cust_id}, min(20, limit - len(out)))
    # Newest first overall
    out.sort(key=lambda e: e.timestamp or e.group_id, reverse=True)
    return out[:limit]


async def _lnd_health() -> dict[str, Any]:
    lnd_config = InternalConfig().config.lnd_config
    if not lnd_config or not lnd_config.default:
        return {"ok": False, "error": "LND not configured"}
    client = LNDClient(connection_name=lnd_config.default)
    try:
        info = await client.node_get_info
        return {
            "ok": True,
            "alias": getattr(info, "alias", None),
            "identity_pubkey": getattr(info, "identity_pubkey", None),
            "num_active_channels": getattr(info, "num_active_channels", None),
            "synced_to_chain": getattr(info, "synced_to_chain", None),
            "block_height": getattr(info, "block_height", None),
            "connection": lnd_config.default,
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "connection": lnd_config.default}
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


async def _invoice_status(memo: str | None) -> dict[str, Any]:
    if not memo:
        return {"present": False, "message": "No memo on operation"}
    lnd_config = InternalConfig().config.lnd_config
    if not lnd_config or not lnd_config.default:
        return {"present": False, "error": "LND not configured"}
    client = LNDClient(connection_name=lnd_config.default)
    try:
        pay_req = await decode_any_lightning_string(memo, lnd_client=client)
        if not pay_req:
            return {"present": False, "message": "No lightning invoice found in memo"}
        expired = bool(getattr(pay_req, "is_expired", False))
        return {
            "present": True,
            "payment_hash": getattr(pay_req, "payment_hash", None),
            "value_msat": int(getattr(pay_req, "value_msat", 0) or 0),
            "value_sat": int(getattr(pay_req, "value", 0) or 0),
            "timestamp": (
                pay_req.timestamp.isoformat() if getattr(pay_req, "timestamp", None) else None
            ),
            "expiry_date": (
                pay_req.expiry_date.isoformat()
                if getattr(pay_req, "expiry_date", None)
                else None
            ),
            "expired": expired,
            "destination": getattr(pay_req, "dest_alias", None)
            or getattr(pay_req, "destination", None),
            "memo": getattr(pay_req, "memo", None),
        }
    except Exception as e:
        return {"present": False, "error": str(e)}
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


def _reply_type_value(reply: Any) -> str:
    rtype = getattr(reply, "reply_type", None)
    if rtype is None and isinstance(reply, dict):
        rtype = reply.get("reply_type")
    if hasattr(rtype, "value"):
        return str(rtype.value)
    return str(rtype or "")


def _is_blocking_reply(reply: Any) -> bool:
    rtype = _reply_type_value(reply)
    if rtype in (ReplyType.LEDGER_ERROR.value, ReplyType.CUSTOM_JSON.value):
        return False
    # Successful payment must block re-pay
    if rtype == ReplyType.PAYMENT.value:
        return True
    return True


def _has_payment_reply(replies: list[Any] | None) -> bool:
    if not replies:
        return False
    return any(_reply_type_value(r) == ReplyType.PAYMENT.value for r in replies)


def _filter_replies_for_retry(replies: list[Any] | None) -> list[dict[str, Any]]:
    """Keep non-clearable replies; drop failure/transfer noise. Never keep PAYMENT for retry."""
    if not replies:
        return []
    kept: list[dict[str, Any]] = []
    for r in replies:
        rtype = _reply_type_value(r)
        if rtype == ReplyType.PAYMENT.value:
            # Caller should refuse retry if payment exists; still keep it if we only clear.
            kept.append(_serialize_reply(r) if not isinstance(r, dict) else r)
            continue
        try:
            enum_t = ReplyType(rtype) if rtype else None
        except ValueError:
            enum_t = None
        if enum_t in _CLEARABLE_ON_RETRY or rtype in {t.value for t in _CLEARABLE_ON_RETRY}:
            continue
        if enum_t in _NON_BLOCKING_REPLY_TYPES:
            kept.append(r.model_dump(mode="json") if hasattr(r, "model_dump") else dict(r))
            continue
        # Unknown types: clear on retry (safer for stuck ops)
        continue
    return kept


async def _build_context(entry: LedgerEntry) -> dict[str, Any]:
    hive_doc = await _load_hive_op_doc(entry.short_id, entry.group_id)
    op = _op_from_doc(hive_doc) if hive_doc else None
    replies_raw = []
    if op and getattr(op, "replies", None):
        replies_raw = list(op.replies or [])
    elif hive_doc and hive_doc.get("replies"):
        replies_raw = list(hive_doc.get("replies") or [])

    replies = [_serialize_reply(r) for r in replies_raw]
    blocking = [r for r in replies if r.get("blocks_follow_on")]
    has_payment = _has_payment_reply(replies_raw)

    related = await _related_ledger(entry.short_id, entry.cust_id)
    related_ser = [_serialize_ledger(e) for e in related]

    dust_outs = [
        e
        for e in related
        if e.ledger_type == LedgerType.CUSTOMER_HIVE_OUT and e.group_id != entry.group_id
    ]

    memo = None
    if op is not None:
        memo = getattr(op, "d_memo", None) or getattr(op, "memo", None)
    elif hive_doc:
        memo = hive_doc.get("d_memo") or hive_doc.get("memo")

    lnd, invoice = await asyncio.gather(_lnd_health(), _invoice_status(memo))

    warnings: list[str] = []
    if has_payment:
        warnings.append(
            "A PAYMENT reply already exists — do not retry pay (risk of double payment)."
        )
    if not hive_doc:
        warnings.append("Hive op not found in hive_ops — cannot retry follow_on.")
    if entry.reversed:
        warnings.append("Deposit ledger entry is marked reversed.")
    if dust_outs:
        warnings.append(
            f"Found {len(dust_outs)} related cust_h_out entry(ies) "
            "(often dust/change on failure). Retry pay keeps the deposit; "
            "reverse dust separately only if needed."
        )
    if invoice.get("expired"):
        warnings.append("Lightning invoice appears EXPIRED — refund HBD instead of retry.")
    if not lnd.get("ok"):
        warnings.append(f"LND GetInfo failed: {lnd.get('error')} — fix Legion/LND before retry.")
    if blocking and not has_payment:
        warnings.append(
            f"{len(blocking)} reply(ies) currently block follow_on_transfer; "
            "use 'Clear blocking replies + retry pay'."
        )

    can_retry = bool(
        hive_doc
        and op is not None
        and not has_payment
        and not entry.reversed
        and lnd.get("ok")
        and not invoice.get("expired")
    )

    return {
        "deposit": _serialize_ledger(entry),
        "hive_op": {
            "found": hive_doc is not None,
            "group_id": (
                getattr(op, "group_id", None)
                if op is not None
                else (hive_doc or {}).get("group_id")
            ),
            "short_id": (
                getattr(op, "short_id", None)
                if op is not None
                else (hive_doc or {}).get("short_id")
            ),
            "from_account": getattr(op, "from_account", None)
            if op is not None
            else (hive_doc or {}).get("from"),
            "to_account": getattr(op, "to_account", None)
            if op is not None
            else (hive_doc or {}).get("to"),
            "amount": str(getattr(op, "amount", None) or (hive_doc or {}).get("amount") or ""),
            "memo": memo or "",
            "type": (hive_doc or {}).get("type") or (hive_doc or {}).get("op_type"),
            "replies": replies,
            "blocking_reply_count": len(blocking),
            "has_payment_reply": has_payment,
            "change_memo": getattr(op, "change_memo", None)
            if op is not None
            else (hive_doc or {}).get("change_memo"),
            "change_amount": str(getattr(op, "change_amount", "") or "")
            if op is not None
            else str((hive_doc or {}).get("change_amount") or ""),
        },
        "related_ledger": related_ser,
        "dust_outs": [_serialize_ledger(e) for e in dust_outs],
        "lnd": lnd,
        "invoice": invoice,
        "warnings": warnings,
        "can_retry_pay": can_retry,
        "recommendation": (
            "Retry follow-on pay only (keep deposit)"
            if can_retry
            else (
                "Invoice expired — refund HBD, do not retry"
                if invoice.get("expired")
                else (
                    "Already paid — do not retry"
                    if has_payment
                    else "Inspect warnings before any action"
                )
            )
        ),
    }


@router.get("/", response_class=HTMLResponse)
async def replay_deposit_page(request: Request, q: str | None = None):
    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    sanity_task = asyncio.create_task(run_all_sanity_checks())
    nav_items = nav_manager.get_navigation_items("/admin/replay-deposit")
    sanity_results = await sanity_task

    return templates.TemplateResponse(
        request,
        "replay_deposit/replay.html.jinja",
        {
            "request": request,
            "title": "Replay Hive Deposit",
            "nav_items": nav_items,
            "initial_query": q or "",
            "breadcrumbs": [
                {"name": "Admin", "url": "/admin"},
                {"name": "Replay Hive Deposit", "url": "/admin/replay-deposit"},
            ],
            "sanity_results": sanity_results,
            "pending_transactions": await PendingTransaction.list_all_str(),
        },
    )


@router.get("/api/search")
async def search_deposits(q: str = "", limit: int = 25) -> JSONResponse:
    try:
        limit = max(1, min(int(limit), 50))
    except Exception:
        limit = 25
    entries = await _find_ledger_deposits(q, limit=limit)
    return JSONResponse({
        "query": q,
        "count": len(entries),
        "results": [_serialize_ledger(e) for e in entries],
    })


@router.get("/api/inspect")
async def inspect_deposit(short_id: str = "", group_id: str = "") -> JSONResponse:
    entry: LedgerEntry | None = None
    if group_id:
        entry = await LedgerEntry.load(group_id)
    if entry is None and short_id:
        found = await _find_ledger_deposits(short_id, limit=5)
        entry = found[0] if found else None
    if entry is None:
        return JSONResponse({"error": "Deposit ledger entry not found"}, status_code=404)
    if entry.ledger_type != LedgerType.CUSTOMER_HIVE_IN:
        return JSONResponse(
            {
                "error": (
                    f"Entry is ledger_type={entry.ledger_type}, expected "
                    f"{LedgerType.CUSTOMER_HIVE_IN.value} (customer Hive/HBD deposit)"
                ),
                "entry": _serialize_ledger(entry),
            },
            status_code=400,
        )
    try:
        ctx = await _build_context(entry)
        return JSONResponse(ctx)
    except Exception as e:
        logger.exception("inspect_deposit failed: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/clear-replies")
async def clear_blocking_replies(payload: dict[str, Any] = Body(...)) -> JSONResponse:
    """Clear failure/transfer replies on the hive op so follow_on can run again."""
    short_id = (payload.get("short_id") or "").strip()
    group_id = (payload.get("group_id") or "").strip()
    dry_run = bool(payload.get("dry_run", False))

    hive_doc = await _load_hive_op_doc(short_id or None, group_id or None)
    if not hive_doc:
        return JSONResponse({"error": "Hive op not found"}, status_code=404)

    op = _op_from_doc(hive_doc)
    if op is None:
        return JSONResponse({"error": "Could not parse hive op"}, status_code=400)

    if _has_payment_reply(op.replies):
        return JSONResponse(
            {"error": "PAYMENT reply present — refusing to clear (already paid)"},
            status_code=409,
        )

    before = [_serialize_reply(r) for r in (op.replies or [])]
    kept = _filter_replies_for_retry(op.replies)
    if dry_run:
        return JSONResponse({
            "dry_run": True,
            "before": before,
            "after": kept,
            "removed": len(before) - len(kept),
        })

    # Direct Mongo update so empty replies list actually clears (TrackedBaseModel.save
    # pops replies=[] and would leave old values).
    gid = getattr(op, "group_id", None) or hive_doc.get("group_id")
    update: dict[str, Any] = {
        "$set": {"replies": kept},
        "$unset": {"change_memo": "", "change_amount": "", "change_conv": ""},
    }
    result = await InternalConfig.db["hive_ops"].update_one(
        {"group_id": gid} if gid else {"short_id": short_id},
        update,
    )
    logger.warning(
        f"Admin cleared blocking replies on hive op {short_id or gid}: "
        f"{len(before)} → {len(kept)} (matched={result.matched_count})",
        extra={"notification": True},
    )
    return JSONResponse({
        "ok": True,
        "matched": result.matched_count,
        "modified": result.modified_count,
        "before": before,
        "after": kept,
        "removed": len(before) - len(kept),
    })


@router.post("/api/retry-pay")
async def retry_follow_on_pay(payload: dict[str, Any] = Body(...)) -> JSONResponse:
    """
    Preferred recovery: keep cust_h_in, clear blocking replies, re-run follow_on_transfer.
    """
    short_id = (payload.get("short_id") or "").strip()
    group_id = (payload.get("group_id") or "").strip()
    clear_replies = payload.get("clear_replies", True)
    nobroadcast = bool(payload.get("nobroadcast", False))
    force = bool(payload.get("force", False))

    # Load deposit ledger
    entry: LedgerEntry | None = None
    if group_id:
        entry = await LedgerEntry.load(group_id)
    if entry is None and short_id:
        found = await _find_ledger_deposits(short_id, limit=5)
        entry = next(
            (e for e in found if e.ledger_type == LedgerType.CUSTOMER_HIVE_IN),
            found[0] if found else None,
        )
    if entry is None:
        return JSONResponse({"error": "Deposit ledger entry not found"}, status_code=404)
    if entry.ledger_type != LedgerType.CUSTOMER_HIVE_IN:
        return JSONResponse(
            {"error": f"Not a deposit (ledger_type={entry.ledger_type})"},
            status_code=400,
        )
    if entry.reversed and not force:
        return JSONResponse(
            {"error": "Deposit is reversed — refuse retry without force=true"},
            status_code=409,
        )

    hive_doc = await _load_hive_op_doc(entry.short_id, entry.group_id)
    if not hive_doc:
        return JSONResponse({"error": "Hive op not found in hive_ops"}, status_code=404)

    op = _op_from_doc(hive_doc)
    if op is None or not isinstance(op, TransferBase):
        return JSONResponse(
            {"error": "Hive op is not a transfer we can follow_on"},
            status_code=400,
        )

    if _has_payment_reply(op.replies):
        return JSONResponse(
            {"error": "PAYMENT reply present — already paid; do not retry"},
            status_code=409,
        )

    # Preflight
    lnd = await _lnd_health()
    if not lnd.get("ok") and not force:
        return JSONResponse(
            {"error": f"LND unhealthy: {lnd.get('error')}", "lnd": lnd},
            status_code=503,
        )

    memo = getattr(op, "d_memo", None) or getattr(op, "memo", None)
    invoice = await _invoice_status(memo)
    if invoice.get("expired") and not force:
        return JSONResponse(
            {
                "error": "Invoice expired — refund instead of retry (pass force=true to override)",
                "invoice": invoice,
            },
            status_code=409,
        )

    steps: list[str] = []

    if clear_replies and op.replies:
        before_n = len(op.replies or [])
        kept = _filter_replies_for_retry(op.replies)
        await InternalConfig.db["hive_ops"].update_one(
            {"group_id": op.group_id},
            {
                "$set": {"replies": kept},
                "$unset": {"change_memo": "", "change_amount": "", "change_conv": ""},
            },
        )
        steps.append(f"Cleared replies {before_n} → {len(kept)}")
        # Reload op so follow_on sees clean replies
        hive_doc = await _load_hive_op_doc(entry.short_id, entry.group_id)
        op = _op_from_doc(hive_doc)  # type: ignore[assignment]
        if op is None:
            return JSONResponse(
                {"error": "Failed to reload op after clearing replies"},
                status_code=500,
            )

    try:
        logger.warning(
            f"Admin retry follow_on_transfer for {entry.short_id} {entry.group_id} "
            f"cust={entry.cust_id} nobroadcast={nobroadcast}",
            extra={"notification": True},
        )
        await follow_on_transfer(tracked_op=op, nobroadcast=nobroadcast)
        steps.append("follow_on_transfer completed without raising")
        # Re-inspect
        ctx = await _build_context(entry)
        return JSONResponse({
            "ok": True,
            "steps": steps,
            "message": "follow_on_transfer finished — check related ledger / payment replies",
            "context": ctx,
        })
    except HiveTransferError as e:
        steps.append(f"HiveTransferError: {e}")
        logger.warning(f"Admin retry follow_on HiveTransferError: {e}")
        ctx = await _build_context(entry)
        return JSONResponse(
            {"ok": False, "error": str(e), "steps": steps, "context": ctx},
            status_code=400,
        )
    except Exception as e:
        steps.append(f"Exception: {e}")
        logger.exception(f"Admin retry follow_on failed: {e}")
        try:
            ctx = await _build_context(entry)
        except Exception:
            ctx = None
        return JSONResponse(
            {"ok": False, "error": str(e), "steps": steps, "context": ctx},
            status_code=500,
        )


@router.get("/api/lnd-health")
async def lnd_health() -> JSONResponse:
    return JSONResponse(await _lnd_health())
