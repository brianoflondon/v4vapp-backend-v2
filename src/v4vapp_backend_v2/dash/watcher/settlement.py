from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from v4vapp_backend_v2.dash.amounts import BPS_DENOM, ZERO, json_amount, to_decimal
from v4vapp_backend_v2.dash.models.invoice import DashInvoiceState

WATCH_STATES = {
    DashInvoiceState.OPEN,
    DashInvoiceState.DETECTED,
    DashInvoiceState.EXPIRED,
    DashInvoiceState.CANCELED,
}


@dataclass
class WatchedOutput:
    txid: str
    vout: int
    duffs: Decimal
    confirmations: int
    instantlock: bool
    chainlock: bool
    detected_at: datetime

    def __post_init__(self) -> None:
        self.duffs = to_decimal(self.duffs)


@dataclass
class Decision:
    state: DashInvoiceState
    first_seen_at: datetime | None
    detected_at: datetime | None
    settled_at: datetime | None
    duffs_received: Decimal
    sats_credited: Decimal | None
    late_payment: bool
    txids: list[dict[str, Any]]
    stuck: bool = False


def underpay_tolerance(duffs_quoted: Decimal, bps: Decimal, floor_duffs: Decimal) -> Decimal:
    return max(floor_duffs, (duffs_quoted * bps) / BPS_DENOM)


def output_qualifies(output: WatchedOutput, policy: dict[str, Any]) -> bool:
    settle = policy.get("settle_policy", "instantsend_or_chainlock")
    if settle == "instantsend_or_chainlock":
        return bool(output.instantlock or output.chainlock)
    min_conf = int(policy.get("min_confirmations") or 1)
    return output.confirmations >= min_conf


def apply_settlement(
    *,
    state: DashInvoiceState,
    now: datetime,
    expires_at: datetime,
    settle_deadline_at: datetime,
    duffs_quoted: Decimal,
    sats_requested: Decimal,
    policy: dict[str, Any],
    outputs: list[WatchedOutput],
    canceled: bool = False,
) -> Decision:
    """Pure settlement. New outputs first seen after expires_at or cancel are late."""
    quoted = to_decimal(duffs_quoted)
    requested = to_decimal(sats_requested)
    if canceled or state == DashInvoiceState.CANCELED:
        late = list(outputs)
        pre: list[WatchedOutput] = []
    else:
        pre = [out for out in outputs if out.detected_at < expires_at]
        late = [out for out in outputs if out.detected_at >= expires_at]

    first_seen = min((out.detected_at for out in pre), default=None)
    qualifying = [out for out in pre if output_qualifies(out, policy)]
    qual_sum = sum((out.duffs for out in qualifying), ZERO)
    total = sum((out.duffs for out in outputs), ZERO)
    tolerance = underpay_tolerance(
        quoted,
        to_decimal(policy.get("underpay_tolerance_bps") or 0),
        to_decimal(policy.get("underpay_tolerance_duffs") or 0),
    )

    txids = [_tx_dict(out) for out in outputs]
    late_payment = bool(late)

    if canceled or state == DashInvoiceState.CANCELED:
        return Decision(
            state=DashInvoiceState.CANCELED,
            first_seen_at=None,
            detected_at=None,
            settled_at=None,
            duffs_received=total,
            sats_credited=None,
            late_payment=late_payment,
            txids=txids,
        )

    if state == DashInvoiceState.EXPIRED:
        return Decision(
            state=DashInvoiceState.EXPIRED,
            first_seen_at=None,
            detected_at=None,
            settled_at=None,
            duffs_received=total,
            sats_credited=None,
            late_payment=late_payment,
            txids=txids,
        )

    if not pre:
        if now >= expires_at:
            return Decision(
                state=DashInvoiceState.EXPIRED,
                first_seen_at=None,
                detected_at=None,
                settled_at=None,
                duffs_received=total,
                sats_credited=None,
                late_payment=late_payment,
                txids=txids,
            )
        return Decision(
            state=DashInvoiceState.OPEN,
            first_seen_at=None,
            detected_at=None,
            settled_at=None,
            duffs_received=total,
            sats_credited=None,
            late_payment=False,
            txids=txids,
        )

    if qual_sum >= quoted:
        new_state = DashInvoiceState.OVERPAID if qual_sum > quoted else DashInvoiceState.SETTLED
        return Decision(
            state=new_state,
            first_seen_at=first_seen,
            detected_at=first_seen,
            settled_at=now,
            duffs_received=total,
            sats_credited=requested,
            late_payment=late_payment,
            txids=txids,
        )

    if qual_sum >= quoted - tolerance:
        return Decision(
            state=DashInvoiceState.SETTLED,
            first_seen_at=first_seen,
            detected_at=first_seen,
            settled_at=now,
            duffs_received=total,
            sats_credited=requested,
            late_payment=late_payment,
            txids=txids,
        )

    waiting_on_policy = any(not output_qualifies(out, policy) for out in pre)
    if now < expires_at or waiting_on_policy:
        return Decision(
            state=DashInvoiceState.DETECTED,
            first_seen_at=first_seen,
            detected_at=first_seen,
            settled_at=None,
            duffs_received=total,
            sats_credited=None,
            late_payment=late_payment,
            txids=txids,
            stuck=waiting_on_policy and now >= settle_deadline_at,
        )

    return Decision(
        state=DashInvoiceState.UNDERPAID,
        first_seen_at=first_seen,
        detected_at=first_seen,
        settled_at=None,
        duffs_received=total,
        sats_credited=None,
        late_payment=late_payment,
        txids=txids,
    )


def _tx_dict(output: WatchedOutput) -> dict[str, Any]:
    return {
        "txid": output.txid,
        "vout": output.vout,
        "duffs": json_amount(output.duffs),
        "confirmations": output.confirmations,
        "instantlock": output.instantlock,
        "chainlock": output.chainlock,
        "detected_at": output.detected_at,
    }


def merge_outputs(
    existing: list[dict[str, Any]],
    fresh: list[WatchedOutput],
) -> list[WatchedOutput]:
    """Preserve first-seen times for (txid, vout) we already recorded."""
    prior = {(row["txid"], int(row["vout"])): row for row in existing}
    merged: list[WatchedOutput] = []
    seen: set[tuple[str, int]] = set()
    for out in fresh:
        key = (out.txid, out.vout)
        seen.add(key)
        old = prior.get(key)
        if old is not None:
            out.detected_at = old["detected_at"]
        merged.append(out)
    for key, old in prior.items():
        if key not in seen:
            merged.append(
                WatchedOutput(
                    txid=old["txid"],
                    vout=int(old["vout"]),
                    duffs=to_decimal(old["duffs"]),
                    confirmations=int(old.get("confirmations") or 0),
                    instantlock=bool(old.get("instantlock")),
                    chainlock=bool(old.get("chainlock")),
                    detected_at=old["detected_at"],
                )
            )
    return merged
