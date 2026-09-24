from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from v4vapp_backend_v2.config.setup import DashConnectionConfig, logger
from v4vapp_backend_v2.dash.amounts import dash_amount_string, rpc_dash_to_duffs, to_decimal
from v4vapp_backend_v2.dash.collections import COL_INVOICES, COL_PAYOUTS
from v4vapp_backend_v2.dash.dashd.rpc import Dashd, DashdError
from v4vapp_backend_v2.dash.db.wallet_state import allocate_change_index
from v4vapp_backend_v2.dash.errors import ApiError
from v4vapp_backend_v2.dash.keys import load_mnemonic, load_xpub_material, mnemonic_matches_xpub
from v4vapp_backend_v2.dash.models.invoice import DashInvoiceState
from v4vapp_backend_v2.dash.models.payout import DashPayoutState, PayoutCreate
from v4vapp_backend_v2.dash.models.wallet import Derivation
from v4vapp_backend_v2.dash.payouts.select import (
    SpendableUtxo,
    as_rpc_inputs,
    dash_outputs,
    parse_fee_rate,
    plan_payout,
    utxo_spendable,
)
from v4vapp_backend_v2.dash.settings import default_min_conf
from v4vapp_backend_v2.dash.wallet.hd import derive_change, wif_from_mnemonic
from v4vapp_backend_v2.helpers.general_purpose_funcs import convert_decimals_for_mongodb

ICON = "💠"
IN_FLIGHT = {DashInvoiceState.OPEN.value, DashInvoiceState.DETECTED.value}


async def lookup_derivation(db: Any, address: str) -> Derivation | None:
    inv = await db[COL_INVOICES].find_one({"address": address})
    if inv is not None:
        if inv.get("state") in IN_FLIGHT:
            return None
        der = inv.get("derivation")
        if der:
            return Derivation.model_validate(der)
    pay = await db[COL_PAYOUTS].find_one({"change_address": address})
    if pay is not None and pay.get("change_derivation"):
        return Derivation.model_validate(pay["change_derivation"])
    return None


async def collect_spendable(
    *,
    db: Any,
    dashd: Dashd,
    conn: DashConnectionConfig,
) -> list[SpendableUtxo]:
    min_conf = default_min_conf(conn.network)
    raw = await dashd.listunspent(0, 9999999, None, True)
    out: list[SpendableUtxo] = []
    for utxo in raw:
        address = str(utxo.get("address") or "")
        if not address:
            continue
        duffs = rpc_dash_to_duffs(utxo.get("amount", 0))
        if duffs < to_decimal(conn.dust_duffs):
            continue
        if not utxo_spendable(utxo, min_conf=min_conf):
            continue
        der = await lookup_derivation(db, address)
        if der is None:
            continue
        out.append(
            SpendableUtxo(
                txid=str(utxo["txid"]),
                vout=int(utxo["vout"]),
                duffs=duffs,
                address=address,
                derivation=der,
                confirmations=int(utxo.get("confirmations") or 0),
                instantlock=bool(utxo.get("instantlock")),
                chainlock=bool(utxo.get("chainlock")),
            )
        )
    return out


async def broadcast_payout(
    *,
    db: Any,
    dashd: Dashd,
    conn: DashConnectionConfig,
    body: PayoutCreate,
    dest_duffs: Decimal,
    address_valid: bool,
) -> dict[str, Any]:
    if not conn.payouts_enabled:
        raise ApiError(503, "payouts_disabled", "Outgoing Dash payouts are not enabled")
    mnemonic = load_mnemonic(conn)
    if not mnemonic:
        raise ApiError(503, "payouts_disabled", "Dash mnemonic_file is not configured")
    if not mnemonic_matches_xpub(mnemonic, conn):
        raise ApiError(503, "keys_mismatch", "mnemonic does not match configured xpub")
    if not address_valid:
        raise ApiError(422, "invalid_address", "address is not valid on this Dash network")

    try:
        smart = await dashd.estimatesmartfee(1)
    except DashdError:
        smart = {}
    fee_rate = parse_fee_rate(smart)
    spendable = await collect_spendable(db=db, dashd=dashd, conn=conn)
    plan = plan_payout(
        utxos=spendable,
        dest_duffs=dest_duffs,
        fee_duffs_per_kvb=fee_rate,
        dust_duffs=to_decimal(conn.dust_duffs),
        subtract_fee=body.subtract_fee,
    )
    if plan is None or plan.dest_duffs <= 0:
        raise ApiError(422, "insufficient_funds", "not enough spendable Dash for this payout")

    change_address = None
    change_der = None
    if plan.change_duffs > 0:
        material = load_xpub_material(conn)
        if material is None:
            raise ApiError(503, "wallet_unavailable", "Dash xpub is not configured")
        idx = await allocate_change_index(db, conn.network)
        change_address, change_der = derive_change(material.account_xpub, conn.network, idx)

    locks = as_rpc_inputs(plan)
    try:
        await dashd.lockunspent(False, locks)
    except DashdError as exc:
        logger.warning(f"{ICON} lockunspent failed: {exc}")

    now = datetime.now(UTC)
    wifs: list[str] = []
    try:
        outputs = dash_outputs(body.address, plan.dest_duffs, change_address, plan.change_duffs)
        raw = await dashd.createrawtransaction(locks, outputs)
        wifs = [
            wif_from_mnemonic(
                mnemonic, conn.network, u.derivation.index, change=u.derivation.change
            )
            for u in plan.inputs
        ]
        signed = await dashd.signrawtransactionwithkey(raw, wifs)
        if not signed.get("complete"):
            raise ApiError(503, "sign_failed", "dashd did not fully sign the payout")
        txid = await dashd.sendrawtransaction(str(signed["hex"]))
    except ApiError:
        await _unlock(dashd, locks)
        raise
    except DashdError as exc:
        await _unlock(dashd, locks)
        raise ApiError(503, "broadcast_failed", str(exc)) from exc
    finally:
        wifs.clear()

    try:
        await dashd.lockunspent(True, locks)
    except DashdError:
        pass

    return {
        "state": DashPayoutState.BROADCAST.value,
        "duffs": dest_duffs,
        "dash_amount": dash_amount_string(plan.dest_duffs),
        "fee_duffs": plan.fee_duffs,
        "change_duffs": plan.change_duffs,
        "change_address": change_address,
        "change_derivation": change_der.model_dump() if change_der else None,
        "txid": txid,
        "inputs": [
            {
                "txid": u.txid,
                "vout": u.vout,
                "duffs": u.duffs,
                "address": u.address,
                "path": u.derivation.path,
            }
            for u in plan.inputs
        ],
        "broadcast_at": now,
        "updated_at": now,
    }


async def _unlock(dashd: Dashd, locks: list[dict[str, Any]]) -> None:
    try:
        await dashd.lockunspent(True, locks)
    except DashdError:
        pass


def persistable_payout(
    *,
    body: PayoutCreate,
    network: str,
    dest_duffs: Decimal,
    sats: Decimal | None,
    extra: dict[str, Any],
) -> dict[str, Any]:
    now = datetime.now(UTC)
    doc: dict[str, Any] = {
        "external_id": body.external_id,
        "cust_id": body.cust_id,
        "memo": body.memo,
        "network": network,
        "address": body.address,
        "duffs": dest_duffs,
        "dash_amount": extra.get("dash_amount") or dash_amount_string(dest_duffs),
        "sats": sats,
        "subtract_fee": body.subtract_fee,
        "state": extra.get("state", DashPayoutState.BROADCAST.value),
        "fee_duffs": extra.get("fee_duffs"),
        "change_duffs": extra.get("change_duffs"),
        "change_address": extra.get("change_address"),
        "change_derivation": extra.get("change_derivation"),
        "txid": extra.get("txid"),
        "inputs": extra.get("inputs") or [],
        "error": extra.get("error"),
        "created_at": now,
        "broadcast_at": extra.get("broadcast_at"),
        "confirmed_at": None,
        "updated_at": now,
    }
    return convert_decimals_for_mongodb(doc)


async def refresh_payout_state(dashd: Dashd, doc: dict[str, Any]) -> dict[str, Any]:
    txid = doc.get("txid")
    if not txid or doc.get("state") not in {
        DashPayoutState.BROADCAST.value,
        DashPayoutState.LOCKED.value,
    }:
        return doc
    try:
        tx = await dashd.gettransaction(str(txid))
    except DashdError:
        return doc
    now = datetime.now(UTC)
    if tx.get("instantlock") or tx.get("chainlock"):
        doc["state"] = DashPayoutState.LOCKED.value
        if int(tx.get("confirmations") or 0) >= 1 and tx.get("chainlock"):
            doc["state"] = DashPayoutState.CONFIRMED.value
            doc["confirmed_at"] = now
    elif int(tx.get("confirmations") or 0) >= 6:
        doc["state"] = DashPayoutState.CONFIRMED.value
        doc["confirmed_at"] = now
    doc["updated_at"] = now
    return doc
