from __future__ import annotations

from typing import Any

from v4vapp_backend_v2.config.setup import DashConnectionConfig
from v4vapp_backend_v2.dash.amounts import ZERO, dash_amount_string, rpc_dash_to_duffs
from v4vapp_backend_v2.dash.collections import COL_INVOICES
from v4vapp_backend_v2.dash.dashd.rpc import Dashd, DashdError
from v4vapp_backend_v2.dash.db.wallet_state import load_wallet_state
from v4vapp_backend_v2.dash.keys import load_mnemonic, mnemonic_matches_xpub
from v4vapp_backend_v2.dash.models.invoice import DashInvoiceState
from v4vapp_backend_v2.dash.models.wallet_out import (
    WalletBalance,
    WalletHd,
    WalletOut,
    WalletUtxo,
)
from v4vapp_backend_v2.dash.payouts.select import utxo_spendable
from v4vapp_backend_v2.dash.settings import default_min_conf

IN_FLIGHT = {DashInvoiceState.OPEN.value, DashInvoiceState.DETECTED.value}


async def wallet_snapshot(
    *,
    db: Any,
    dashd: Dashd,
    conn: DashConnectionConfig,
    include_utxos: bool = False,
) -> WalletOut:
    min_conf = default_min_conf(conn.network)
    raw = await dashd.listunspent(0, 9999999, None, True)
    incoming_addrs: set[str] = set()
    cursor = db[COL_INVOICES].find({"state": {"$in": list(IN_FLIGHT)}})
    async for inv in cursor:
        if inv.get("address"):
            incoming_addrs.add(inv["address"])

    total = ZERO
    spendable = ZERO
    incoming = ZERO
    unconfirmed = ZERO
    spendable_n = 0
    rows: list[WalletUtxo] = []
    for utxo in raw:
        address = str(utxo.get("address") or "")
        duffs = rpc_dash_to_duffs(utxo.get("amount", 0))
        conf = int(utxo.get("confirmations") or 0)
        is_lock = bool(utxo.get("instantlock"))
        cl_lock = bool(utxo.get("chainlock"))
        is_incoming = address in incoming_addrs
        can_spend = (not is_incoming) and utxo_spendable(utxo, min_conf=min_conf)
        total += duffs
        if is_incoming:
            incoming += duffs
        elif can_spend:
            spendable += duffs
            spendable_n += 1
        if conf == 0 and not is_lock:
            unconfirmed += duffs
        if include_utxos:
            rows.append(
                WalletUtxo(
                    txid=str(utxo.get("txid") or ""),
                    vout=int(utxo.get("vout") or 0),
                    address=address,
                    duffs=duffs,
                    confirmations=conf,
                    instantlock=is_lock,
                    chainlock=cl_lock,
                    spendable=can_spend,
                    incoming=is_incoming,
                )
            )

    state = await load_wallet_state(db, conn.network)
    hd = WalletHd(
        fingerprint=(state or {}).get("fingerprint") or conn.master_fingerprint or None,
        next_receive_index=int((state or {}).get("next_receive_index") or 0),
        next_change_index=int((state or {}).get("next_change_index") or 0),
        descriptor_range_end=int(
            (state or {}).get("descriptor_range_end") or conn.descriptor_range_end
        ),
    )
    dashd_info = None
    try:
        info = await dashd.getblockchaininfo()
        ibd = bool(info.get("initialblockdownload"))
        dashd_info = {
            "chain": info.get("chain"),
            "blocks": info.get("blocks"),
            "headers": info.get("headers"),
            "initialblockdownload": ibd,
            "synced": not ibd,
            "pruned": info.get("pruned"),
        }
        winfo = await dashd.getwalletinfo()
        dashd_info["txcount"] = winfo.get("txcount")
        dashd_info["private_keys_enabled"] = winfo.get("private_keys_enabled")
        dashd_info["descriptors"] = winfo.get("descriptors")
    except DashdError:
        pass

    mnemonic = load_mnemonic(conn)
    can_sign = bool(
        conn.payouts_enabled and mnemonic and mnemonic_matches_xpub(mnemonic, conn)
    )
    return WalletOut(
        network=conn.network,
        payouts_enabled=conn.payouts_enabled,
        can_sign=can_sign,
        balance=WalletBalance(
            duffs_total=total,
            duffs_spendable=spendable,
            duffs_incoming=incoming,
            duffs_unconfirmed=unconfirmed,
            dash_total=dash_amount_string(total),
            dash_spendable=dash_amount_string(spendable),
        ),
        utxo_count=len(raw),
        spendable_utxo_count=spendable_n,
        hd=hd,
        dashd=dashd_info,
        utxos=rows if include_utxos else None,
    )
