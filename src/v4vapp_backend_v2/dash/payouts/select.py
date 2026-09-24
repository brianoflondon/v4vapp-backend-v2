from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal
from typing import Any

from v4vapp_backend_v2.dash.amounts import ZERO, dash_amount_string, to_decimal
from v4vapp_backend_v2.dash.models.wallet import Derivation

P2PKH_INPUT_VBYTES = 148
P2PKH_OUTPUT_VBYTES = 34
TX_OVERHEAD_VBYTES = 10
FALLBACK_FEE_DUFFS_PER_KVB = Decimal(1000)  # 0.00001 DASH / kvB


@dataclass
class SpendableUtxo:
    txid: str
    vout: int
    duffs: Decimal
    address: str
    derivation: Derivation
    confirmations: int
    instantlock: bool
    chainlock: bool


@dataclass
class TxPlan:
    inputs: list[SpendableUtxo]
    dest_duffs: Decimal
    fee_duffs: Decimal
    change_duffs: Decimal
    n_out: int
    fee_duffs_per_kvb: Decimal

    @property
    def in_duffs(self) -> Decimal:
        return sum((u.duffs for u in self.inputs), ZERO)


def tx_vbytes(n_in: int, n_out: int) -> int:
    return TX_OVERHEAD_VBYTES + P2PKH_INPUT_VBYTES * n_in + P2PKH_OUTPUT_VBYTES * n_out


def estimate_fee_duffs(n_in: int, n_out: int, fee_duffs_per_kvb: Decimal) -> Decimal:
    raw = (fee_duffs_per_kvb * Decimal(tx_vbytes(n_in, n_out))) / Decimal(1000)
    fee = raw.to_integral_value(rounding=ROUND_CEILING)
    return fee if fee > ZERO else Decimal(1)


def utxo_spendable(utxo: dict[str, Any], *, min_conf: int) -> bool:
    if utxo.get("instantlock") or utxo.get("chainlock"):
        return True
    return int(utxo.get("confirmations") or 0) >= min_conf


def plan_payout(
    *,
    utxos: list[SpendableUtxo],
    dest_duffs: Decimal,
    fee_duffs_per_kvb: Decimal,
    dust_duffs: Decimal,
    subtract_fee: bool,
) -> TxPlan | None:
    """Largest-first coin select. Returns None if funds are insufficient."""
    ordered = sorted(utxos, key=lambda u: u.duffs, reverse=True)
    selected: list[SpendableUtxo] = []
    total = ZERO
    for utxo in ordered:
        selected.append(utxo)
        total += utxo.duffs
        for n_out in (2, 1):
            fee = estimate_fee_duffs(len(selected), n_out, fee_duffs_per_kvb)
            if subtract_fee:
                if dest_duffs <= fee or total < dest_duffs:
                    continue
                change = total - dest_duffs
                dest_send = dest_duffs - fee
                if n_out == 2 and change >= dust_duffs:
                    return TxPlan(selected, dest_send, fee, change, 2, fee_duffs_per_kvb)
                if n_out == 1 and change < dust_duffs:
                    extra = change
                    return TxPlan(
                        selected, dest_send - extra, fee + extra, ZERO, 1, fee_duffs_per_kvb
                    )
            else:
                need = dest_duffs + fee
                if total < need:
                    continue
                change = total - dest_duffs - fee
                if n_out == 2 and change >= dust_duffs:
                    return TxPlan(selected, dest_duffs, fee, change, 2, fee_duffs_per_kvb)
                if n_out == 1 and change < dust_duffs:
                    return TxPlan(selected, dest_duffs, fee + change, ZERO, 1, fee_duffs_per_kvb)
    return None


def dash_outputs(
    address: str,
    dest_duffs: Decimal,
    change_address: str | None,
    change_duffs: Decimal,
) -> dict[str, float]:
    outs: dict[str, float] = {address: float(dash_amount_string(dest_duffs))}
    if change_address and change_duffs > ZERO:
        outs[change_address] = float(dash_amount_string(change_duffs))
    return outs


def as_rpc_inputs(plan: TxPlan) -> list[dict[str, Any]]:
    return [{"txid": u.txid, "vout": u.vout} for u in plan.inputs]


def parse_fee_rate(smartfee: dict[str, Any] | None) -> Decimal:
    if not smartfee:
        return FALLBACK_FEE_DUFFS_PER_KVB
    if smartfee.get("errors"):
        return FALLBACK_FEE_DUFFS_PER_KVB
    feerate = smartfee.get("feerate")
    if feerate is None:
        return FALLBACK_FEE_DUFFS_PER_KVB
    duffs_per_kvb = (to_decimal(feerate) * Decimal(100_000_000)).to_integral_value(
        rounding=ROUND_CEILING
    )
    return duffs_per_kvb if duffs_per_kvb > ZERO else FALLBACK_FEE_DUFFS_PER_KVB
