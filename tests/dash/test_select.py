from decimal import Decimal

from v4vapp_backend_v2.dash.models.wallet import Derivation
from v4vapp_backend_v2.dash.payouts.select import (
    FALLBACK_FEE_DUFFS_PER_KVB,
    SpendableUtxo,
    estimate_fee_duffs,
    parse_fee_rate,
    plan_payout,
    tx_vbytes,
)


def _u(duffs: int, index: int = 0) -> SpendableUtxo:
    return SpendableUtxo(
        txid="a" * 64,
        vout=index,
        duffs=Decimal(duffs),
        address="yRd4FhXfVGHXpsuZXPNkMrfD9GVj46pnjt",
        derivation=Derivation(account=0, change=0, index=index, path=f"m/44'/1'/0'/0/{index}"),
        confirmations=6,
        instantlock=True,
        chainlock=False,
    )


def test_fee_grows_with_inputs() -> None:
    one = estimate_fee_duffs(1, 2, FALLBACK_FEE_DUFFS_PER_KVB)
    two = estimate_fee_duffs(2, 2, FALLBACK_FEE_DUFFS_PER_KVB)
    assert two > one
    assert tx_vbytes(1, 2) == 10 + 148 + 68


def test_plan_selects_largest_first_with_change() -> None:
    plan = plan_payout(
        utxos=[_u(5_000_000, 0), _u(50_000_000, 1)],
        dest_duffs=Decimal(10_000_000),
        fee_duffs_per_kvb=FALLBACK_FEE_DUFFS_PER_KVB,
        dust_duffs=Decimal(5460),
        subtract_fee=False,
    )
    assert plan is not None
    assert len(plan.inputs) == 1
    assert plan.inputs[0].duffs == Decimal(50_000_000)
    assert plan.dest_duffs == Decimal(10_000_000)
    assert plan.change_duffs > 0
    assert plan.n_out == 2


def test_plan_insufficient() -> None:
    plan = plan_payout(
        utxos=[_u(1000)],
        dest_duffs=Decimal(10_000_000),
        fee_duffs_per_kvb=FALLBACK_FEE_DUFFS_PER_KVB,
        dust_duffs=Decimal(5460),
        subtract_fee=False,
    )
    assert plan is None


def test_subtract_fee_reduces_destination() -> None:
    plan = plan_payout(
        utxos=[_u(10_000_000)],
        dest_duffs=Decimal(10_000_000),
        fee_duffs_per_kvb=FALLBACK_FEE_DUFFS_PER_KVB,
        dust_duffs=Decimal(5460),
        subtract_fee=True,
    )
    assert plan is not None
    assert plan.n_out == 1
    assert plan.dest_duffs + plan.fee_duffs == Decimal(10_000_000)
    assert plan.dest_duffs < Decimal(10_000_000)


def test_parse_fee_rate_fallback_on_errors() -> None:
    assert parse_fee_rate({"errors": ["Insufficient data"]}) == FALLBACK_FEE_DUFFS_PER_KVB
    rate = parse_fee_rate({"feerate": 0.00001})
    assert rate == Decimal(1000)
