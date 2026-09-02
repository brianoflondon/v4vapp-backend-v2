from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from v4vapp_backend_v2.dash.amounts import ZERO, json_amount, to_decimal
from v4vapp_backend_v2.dash.collections import COL_INVOICES
from v4vapp_backend_v2.dash.errors import ApiError
from v4vapp_backend_v2.dash.limits.hive_config import RateWindow, fetch_rate_windows
from v4vapp_backend_v2.dash.models.invoice import DashInvoiceState

PAID_STATES = (DashInvoiceState.SETTLED.value, DashInvoiceState.OVERPAID.value)


def format_time_delta(delta: timedelta) -> str:
    seconds = int(max(delta.total_seconds(), 0))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _sats_of(doc: dict[str, Any]) -> Decimal:
    credited = doc.get("sats_credited")
    if credited is not None:
        return to_decimal(credited)
    return to_decimal(doc.get("sats_requested") or 0)


async def _paid_since(db: Any, cust_id: str, since: datetime) -> list[dict[str, Any]]:
    cursor = db[COL_INVOICES].find(
        {
            "cust_id": cust_id,
            "state": {"$in": list(PAID_STATES)},
            "settled_at": {"$gte": since},
        }
    )
    if hasattr(cursor, "to_list"):
        return await cursor.to_list(length=None)
    return [doc async for doc in cursor]


def _period_result(
    *,
    window: RateWindow,
    used: Decimal,
    extra: Decimal,
    cust_id: str,
) -> dict[str, Any]:
    total = used + extra
    percent = int((total * Decimal(100) / window.sats).to_integral_value()) if window.sats else 0
    ok = total <= window.sats
    ok_str = "ok" if ok else "exceeded"
    text = (
        f"Lightning conversions for {cust_id} in the last {window.hours} hours: "
        f"{total:,.0f} sats (limit: {window.sats:,.0f} sats, {ok_str})"
    )
    return {
        "hours": window.hours,
        "limit_hours": window.hours,
        "limit_sats": json_amount(window.sats),
        "sats": json_amount(total),
        "sats_paid": json_amount(used),
        "sats_requested": json_amount(extra),
        "limit_ok": ok,
        "limit_percent": percent,
        "limit_text": text,
    }


async def check_cust_rate_limit(
    db: Any,
    *,
    cust_id: str,
    extra_sats: Decimal,
    now: datetime | None = None,
) -> None:
    """
    Raise 422 rate_limit_exceeded if paid SETTLED/OVERPAID sats for this cust_id
    plus extra_sats would exceed any Hive lightning_rate_limits window.
    """
    if not cust_id:
        return

    extra = to_decimal(extra_sats)
    now = now or datetime.now(UTC)
    windows = fetch_rate_windows()
    if not windows:
        return

    max_hours = max(w.hours for w in windows)
    paid = await _paid_since(db, cust_id, now - timedelta(hours=max_hours))

    periods: dict[str, dict[str, Any]] = {}
    over: list[tuple[RateWindow, list[dict[str, Any]]]] = []
    for window in windows:
        since = now - timedelta(hours=window.hours)
        in_window = [
            doc for doc in paid if doc.get("settled_at") is not None and doc["settled_at"] >= since
        ]
        used = sum((_sats_of(doc) for doc in in_window), ZERO)
        result = _period_result(window=window, used=used, extra=extra, cust_id=cust_id)
        periods[str(window.hours)] = result
        if not result["limit_ok"]:
            over.append((window, in_window))

    if not over:
        return

    soonest_expiry: datetime | None = None
    soonest_freed = ZERO
    for window, docs in over:
        dated = [doc for doc in docs if doc.get("settled_at") is not None]
        if not dated:
            continue
        oldest = min(dated, key=lambda d: d["settled_at"])
        expiry = oldest["settled_at"] + timedelta(hours=window.hours)
        if soonest_expiry is None or expiry < soonest_expiry:
            soonest_expiry = expiry
            soonest_freed = _sats_of(oldest)

    next_text = ""
    if soonest_expiry is not None:
        next_text = (
            f"Next limit expires in: {format_time_delta(soonest_expiry - now)}, "
            f"freeing {soonest_freed:,.0f} sats"
        )

    first_over = next(p for p in periods.values() if not p["limit_ok"])
    message = first_over["limit_text"]
    if next_text:
        message = f"{message}. {next_text}"

    raise ApiError(
        422,
        "rate_limit_exceeded",
        message,
        extra={
            "limit_check": {
                "cust_id": cust_id,
                "limit_ok": False,
                "next_limit_expiry": next_text,
                "expiry": soonest_expiry.isoformat() if soonest_expiry else None,
                "sats_freed": json_amount(soonest_freed),
                "periods": periods,
            }
        },
    )
