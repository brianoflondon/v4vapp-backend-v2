"""Return Dash to the original payer when Lightning payout fails."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from bip_utils import CoinsConf, P2PKHAddr

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.dash.amounts import duffs_from_sats, to_decimal
from v4vapp_backend_v2.dash.dashd.rpc import Dashd, DashdError

ICON = "💠"
REFUND_FEE_SATS = Decimal(100)
_COMPRESSED_PUBKEY_LEN = 33
_UNCOMPRESSED_PUBKEY_LEN = 65


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


def _p2pkh_net_ver(our_address: str) -> bytes | None:
    if our_address.startswith("y"):
        return CoinsConf.DashTestNet.ParamByKey("p2pkh_net_ver")
    if our_address.startswith("X"):
        return CoinsConf.DashMainNet.ParamByKey("p2pkh_net_ver")
    return None


def _p2pkh_address_from_pubkey(pubkey: bytes, our_address: str) -> str | None:
    net_ver = _p2pkh_net_ver(our_address)
    if net_ver is None:
        return None
    try:
        addr = P2PKHAddr.EncodeKey(pubkey, net_ver=net_ver)
    except ValueError:
        return None
    return addr if isinstance(addr, str) and addr and addr != our_address else None


def _pubkey_from_scriptsig_asm(asm: str) -> bytes | None:
    """Last scriptSig push that looks like a compressed/uncompressed secp256k1 pubkey."""
    if not asm:
        return None
    last = asm.split()[-1]
    try:
        raw = bytes.fromhex(last)
    except ValueError:
        return None
    if len(raw) == _COMPRESSED_PUBKEY_LEN and raw[0] in (2, 3):
        return raw
    if len(raw) == _UNCOMPRESSED_PUBKEY_LEN and raw[0] == 4:
        return raw
    return None


def _unique_change_address(tx: dict[str, Any], our_address: str) -> str | None:
    """The other payable output, if there is exactly one (typical wallet change)."""
    found: list[str] = []
    seen: set[str] = set()
    for vout in tx.get("vout") or []:
        script = vout.get("scriptPubKey") or {}
        if script.get("type") == "nulldata":
            continue
        addr = _script_address(vout)
        if not addr or addr == our_address or addr in seen:
            continue
        seen.add(addr)
        found.append(addr)
    return found[0] if len(found) == 1 else None


def _p2pkh_from_scriptsigs(tx: dict[str, Any], our_address: str) -> str | None:
    """P2PKH address from vin pubkeys when every recoverable vin agrees."""
    found: list[str] = []
    seen: set[str] = set()
    vins = tx.get("vin") or []
    if not vins:
        return None
    for vin in vins:
        if vin.get("coinbase"):
            return None
        asm = str((vin.get("scriptSig") or {}).get("asm") or "")
        pubkey = _pubkey_from_scriptsig_asm(asm)
        if pubkey is None:
            return None
        addr = _p2pkh_address_from_pubkey(pubkey, our_address)
        if not addr:
            return None
        if addr not in seen:
            seen.add(addr)
            found.append(addr)
    return found[0] if len(found) == 1 else None


async def _decode_hex(dashd: Dashd, raw_hex: str, txid: str) -> dict[str, Any] | None:
    try:
        decoded = await dashd.decoderawtransaction(raw_hex)
    except DashdError as exc:
        logger.warning(
            f"{ICON} decoderawtransaction failed for {txid}: {exc}",
            extra={"txid": txid},
        )
        return None
    if isinstance(decoded, dict) and (decoded.get("vin") is not None or decoded.get("vout")):
        return decoded
    return None


async def _node_raw_tx(
    dashd: Dashd, txid: str, *, blockhash: str | None
) -> dict[str, Any] | None:
    try:
        raw = await dashd.getrawtransaction(txid, True, blockhash)
    except DashdError as exc:
        logger.debug(
            f"{ICON} getrawtransaction {txid[:12]} failed: {exc}",
            extra={"txid": txid, "blockhash": blockhash or ""},
        )
        return None
    if isinstance(raw, str) and raw:
        return await _decode_hex(dashd, raw, txid)
    if isinstance(raw, dict) and (raw.get("vin") is not None or raw.get("vout")):
        return raw
    return None


async def _decoded_funding_tx(dashd: Dashd, txid: str) -> dict[str, Any] | None:
    """Wallet hex first (no txindex). Node raw / blockhash only as fallback."""
    wallet_hex = ""
    blockhash = ""
    try:
        wtx = await dashd.gettransaction(txid)
    except DashdError as exc:
        logger.debug(
            f"{ICON} wallet gettransaction {txid[:12]} failed: {exc}",
            extra={"txid": txid},
        )
        wtx = None
    if isinstance(wtx, dict):
        wallet_hex = str(wtx.get("hex") or "")
        blockhash = str(wtx.get("blockhash") or "")
        if wallet_hex:
            decoded = await _decode_hex(dashd, wallet_hex, txid)
            if decoded is not None:
                return decoded
    decoded = await _node_raw_tx(dashd, txid, blockhash=None)
    if decoded is not None:
        return decoded
    if blockhash:
        decoded = await _node_raw_tx(dashd, txid, blockhash=blockhash)
        if decoded is not None:
            return decoded
    logger.warning(
        f"{ICON} could not load funding tx {txid} for Dash refund",
        extra={"txid": txid},
    )
    return None


async def _unique_prevout_address(
    dashd: Dashd, tx: dict[str, Any], our_address: str
) -> str | None:
    """Last resort: unique spent-output address. Needs txindex (or an in-wallet parent)."""
    found: list[str] = []
    seen: set[str] = set()
    for vin in tx.get("vin") or []:
        prev_txid = vin.get("txid")
        if not prev_txid:
            continue
        try:
            prev_vout = int(vin.get("vout"))
        except (TypeError, ValueError):
            continue
        prev = await _node_raw_tx(dashd, str(prev_txid), blockhash=None)
        if prev is None:
            continue
        vouts = prev.get("vout") or []
        if prev_vout < 0 or prev_vout >= len(vouts):
            continue
        addr = _script_address(vouts[prev_vout])
        if not addr or addr == our_address:
            continue
        if addr not in seen:
            seen.add(addr)
            found.append(addr)
    return found[0] if len(found) == 1 else None


async def funding_sender_address(
    dashd: Dashd,
    *,
    txid: str,
    our_address: str,
) -> str | None:
    """Refund destination from the inbound payment. Dash has no protocol refund address.

    Order: unique change vout, then P2PKH from vin scriptSig, then unique prevout.
    Change and scriptSig need only this transaction (wallet ``gettransaction`` hex).
    """
    tx = await _decoded_funding_tx(dashd, txid)
    if tx is None:
        return None
    change = _unique_change_address(tx, our_address)
    if change:
        logger.info(
            f"{ICON} Dash refund sender {change} from change output of {txid}",
            extra={"txid": txid, "refund_source": "change"},
        )
        return change
    p2pkh = _p2pkh_from_scriptsigs(tx, our_address)
    if p2pkh:
        logger.info(
            f"{ICON} Dash refund sender {p2pkh} from P2PKH scriptSig of {txid}",
            extra={"txid": txid, "refund_source": "scriptsig"},
        )
        return p2pkh
    prevout = await _unique_prevout_address(dashd, tx, our_address)
    if prevout:
        logger.info(
            f"{ICON} Dash refund sender {prevout} from vin prevout of {txid}",
            extra={"txid": txid, "refund_source": "prevout"},
        )
        return prevout
    logger.warning(
        f"{ICON} Dash refund: no change, P2PKH scriptSig, or unique prevout for {txid}",
        extra={"txid": txid},
    )
    return None


def first_funding_txid(invoice_doc: dict[str, Any]) -> str:
    rows = list(invoice_doc.get("txids") or [])
    if not rows:
        return ""
    best = max(rows, key=lambda row: to_decimal(row.get("duffs") or 0))
    return str(best.get("txid") or "")
