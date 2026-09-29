"""Pick the next HD receive address that has never been used and has no UTXOs."""

from decimal import Decimal
from typing import Any

from v4vapp_backend_v2.config.setup import DashNetwork, logger
from v4vapp_backend_v2.dash.amounts import ZERO, rpc_dash_to_duffs, to_decimal
from v4vapp_backend_v2.dash.collections import COL_INVOICES
from v4vapp_backend_v2.dash.dashd.rpc import Dashd, DashdError
from v4vapp_backend_v2.dash.db.wallet_state import WalletStateMismatch, allocate_receive_index
from v4vapp_backend_v2.dash.errors import ApiError
from v4vapp_backend_v2.dash.models.wallet import Derivation
from v4vapp_backend_v2.dash.wallet.hd import derive_receive

ICON = "💠"
# Burn at most this many dirty indexes per invoice create, then fail closed.
MAX_EMPTY_RECEIVE_SKIPS = 64


async def allocate_empty_receive(
    db: Any,
    dashd: Dashd,
    *,
    network: DashNetwork,
    account_xpub: str,
    range_end: int,
) -> tuple[int, str, Derivation]:
    """
    Atomically take HD receive indexes until one is empty on-chain and unused in Mongo.

    Dirty indexes are never reused (the counter only moves forward). Fails if dashd
    cannot answer, the descriptor range is exhausted, or too many consecutive
    addresses already have coins.
    """
    skipped: list[dict[str, Any]] = []
    while True:
        try:
            index = await allocate_receive_index(db, network)
        except WalletStateMismatch as exc:
            raise ApiError(503, "wallet_unavailable", str(exc)) from exc
        if index >= range_end:
            raise ApiError(503, "index_exhausted", "receive index is past the descriptor range")

        address, derivation = derive_receive(account_xpub, network, index)
        reason = await receive_address_block_reason(db, dashd, address)
        if reason is None:
            if skipped:
                logger.info(
                    f"{ICON} skipped {len(skipped)} dirty receive index(es) before {address}",
                    extra={"index": index, "address": address, "skipped": skipped},
                )
            logger.info(f"{ICON} using receive index={index} {address}")
            return index, address, derivation

        logger.warning(
            f"{ICON} skip receive index={index} {address}: {reason}",
            extra={"index": index, "address": address, "reason": reason},
        )
        skipped.append({"index": index, "address": address, "reason": reason})
        if len(skipped) >= MAX_EMPTY_RECEIVE_SKIPS:
            raise ApiError(
                503,
                "index_exhausted",
                f"no empty receive address after skipping {len(skipped)} indexes",
            )


async def receive_address_block_reason(db: Any, dashd: Dashd, address: str) -> str | None:
    """Why this address must not be issued. None means it is empty and unused."""
    existing = await db[COL_INVOICES].find_one({"address": address})
    if existing is not None:
        return f"already on invoice {existing.get('_id')}"

    try:
        utxos = await dashd.listunspent(0, 9999999, [address], True)
    except DashdError as exc:
        raise ApiError(503, "wallet_unavailable", f"listunspent failed: {exc}") from exc
    hits = [
        utxo for utxo in utxos if not utxo.get("address") or str(utxo.get("address")) == address
    ]
    if hits:
        duffs = sum((rpc_dash_to_duffs(u.get("amount", 0)) for u in hits), start=to_decimal(0))
        return f"{len(hits)} unspent output(s) {duffs:,.0f} duffs"

    received = await _received_duffs(dashd, address)
    if received > 0:
        return f"previously received {received:,.0f} duffs"
    return None


async def _received_duffs(dashd: Dashd, address: str) -> Decimal:
    getter = getattr(dashd, "getreceivedbyaddress", None)
    if getter is None:
        return ZERO
    try:
        raw = await getter(address, 0)
    except DashdError:
        # Address not in the watch wallet yet — listunspent already said no UTXOs.
        return ZERO
    try:
        return rpc_dash_to_duffs(raw)
    except (TypeError, ValueError):
        return ZERO
