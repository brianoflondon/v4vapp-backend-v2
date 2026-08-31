"""Return Dash to the original payer when Lightning payout fails."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from v4vapp_backend_v2.dash.amounts import duffs_from_sats, rpc_dash_to_duffs, to_decimal
from v4vapp_backend_v2.dash.dashd.rpc import Dashd, DashdError

ICON = "💠"
REFUND_FEE_SATS = Decimal(100)


def refund_fee_duffs(invoice_doc: dict[str, Any]) -> Decimal:
    snapshot = invoice_doc.get("quote") or {}
    dash_btc = to_decimal(snapshot.get("dash_btc") or 0)
    return duffs_from_sats(REFUND_FEE_SATS, dash_btc)


def refund_duffs(invoice_doc: dict[str, Any]) -> Decimal:
    """Received Dash minus 100 sats of Dash (invoice quote)."""
    received = to_decimal(invoice_doc.get("duffs_received") or 0)
    leftover = received - refund_fee_duffs(invoice_doc)
    return leftover if leftover > 0 else to_decimal(0)


def refund_payout_external_id(invoice_id: str) -> str:
    return f"dash-refund:{invoice_id}"


def _script_address(vout: dict[str, Any]) -> str:
    script = vout.get("scriptPubKey") or {}
    if isinstance(script.get("address"), str) and script["address"]:
        return script["address"]
    addrs = script.get("addresses") or []
    if addrs and isinstance(addrs[0], str):
        return addrs[0]
    return ""


async def funding_sender_address(
    dashd: Dashd,
    *,
    txid: str,
    our_address: str,
) -> str | None:
    """Largest vin prevout address that is not our invoice receive address."""
    try:
        tx = await dashd.getrawtransaction(txid, True)
    except DashdError:
        return None
    if not isinstance(tx, dict):
        return None
    best_addr: str | None = None
    best_duffs = to_decimal(0)
    for vin in tx.get("vin") or []:
        prev_txid = vin.get("txid")
        if not prev_txid:
            continue
        try:
            prev_vout = int(vin.get("vout"))
        except (TypeError, ValueError):
            continue
        try:
            prev = await dashd.getrawtransaction(str(prev_txid), True)
        except DashdError:
            continue
        if not isinstance(prev, dict):
            continue
        vouts = prev.get("vout") or []
        if prev_vout < 0 or prev_vout >= len(vouts):
            continue
        addr = _script_address(vouts[prev_vout])
        if not addr or addr == our_address:
            continue
        duffs = rpc_dash_to_duffs(vouts[prev_vout].get("value") or 0)
        if best_addr is None or duffs > best_duffs:
            best_addr, best_duffs = addr, duffs
    return best_addr


def first_funding_txid(invoice_doc: dict[str, Any]) -> str:
    rows = list(invoice_doc.get("txids") or [])
    if not rows:
        return ""
    best = max(rows, key=lambda row: to_decimal(row.get("duffs") or 0))
    return str(best.get("txid") or "")
