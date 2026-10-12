from __future__ import annotations

from typing import Any

from v4vapp_backend_v2.config.setup import DashNetwork, logger
from v4vapp_backend_v2.dash.dashd.rpc import Dashd, WalletDisabled
from v4vapp_backend_v2.dash.wallet.descriptors import (
    checksummed_descriptor,
    import_request,
    raw_descriptor,
)

ICON = "💠"


async def bootstrap_watch_wallet(
    dashd: Dashd,
    *,
    network: DashNetwork,
    account_xpub: str,
    fingerprint: str,
    range_end: int,
) -> dict[str, Any]:
    """
    Ensure the named watch-only descriptor wallet exists and has receive+change
    ranged descriptors. Safe to call on every boot. Does not rescan pruned history.
    """
    try:
        wallets = await dashd.listwallets()
    except WalletDisabled:
        logger.warning(
            f"{ICON} dashd wallet RPC is disabled (-disablewallet=1); skip watch-wallet bootstrap"
        )
        return {"status": "wallet_disabled"}

    created = False
    if dashd.wallet_name not in wallets:
        await dashd.create_watch_wallet()
        created = True
        logger.info(f"{ICON} created watch-only descriptor wallet {dashd.wallet_name}")

    recv_raw = raw_descriptor(fingerprint, account_xpub, network, change=False)
    change_raw = raw_descriptor(fingerprint, account_xpub, network, change=True)
    recv_info = await dashd.getdescriptorinfo(recv_raw)
    change_info = await dashd.getdescriptorinfo(change_raw)
    recv_desc = checksummed_descriptor(recv_info, recv_raw)
    change_desc = checksummed_descriptor(change_info, change_raw)

    existing = await _existing_descs(dashd)
    need_import = recv_desc not in existing or change_desc not in existing
    imported = False
    if need_import:
        result = await dashd.importdescriptors(
            [
                import_request(recv_desc, internal=False, range_end=range_end),
                import_request(change_desc, internal=True, range_end=range_end),
            ]
        )
        _raise_if_import_failed(result)
        imported = True
        logger.info(f"{ICON} imported watch descriptors range [0, {range_end}]")

    return {
        "status": "ok",
        "wallet": dashd.wallet_name,
        "created": created,
        "imported": imported,
        "receive_desc": recv_desc,
        "change_desc": change_desc,
    }


async def _existing_descs(dashd: Dashd) -> set[str]:
    try:
        listed = await dashd.listdescriptors()
    except Exception:
        return set()
    descs: set[str] = set()
    for item in listed.get("descriptors", []):
        if isinstance(item, dict) and isinstance(item.get("desc"), str):
            descs.add(item["desc"])
    return descs


def _raise_if_import_failed(result: Any) -> None:
    if not isinstance(result, list):
        return
    for item in result:
        if isinstance(item, dict) and item.get("success") is False:
            error = item.get("error") or item
            raise RuntimeError(f"importdescriptors failed: {error}")
