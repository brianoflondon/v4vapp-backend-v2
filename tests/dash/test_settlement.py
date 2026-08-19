from datetime import UTC, datetime, timedelta

from v4vapp_backend_v2.dash.models.invoice import DashInvoiceState
from v4vapp_backend_v2.dash.watcher.settlement import WatchedOutput, apply_settlement

T0 = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)
EXPIRES = T0 + timedelta(minutes=15)
DEADLINE = EXPIRES + timedelta(hours=1)
IS_POLICY = {
    "settle_policy": "instantsend_or_chainlock",
    "underpay_tolerance_bps": 100,
    "underpay_tolerance_duffs": 50_000,
    "min_confirmations": 6,
}
CONF_POLICY = {
    "settle_policy": "conf_n",
    "underpay_tolerance_bps": 100,
    "underpay_tolerance_duffs": 50_000,
    "min_confirmations": 1,
}


def _out(
    *,
    duffs: int = 50_000_000,
    detected: datetime = T0 + timedelta(minutes=1),
    instantlock: bool = False,
    chainlock: bool = False,
    confirmations: int = 0,
    txid: str = "aa",
    vout: int = 0,
) -> WatchedOutput:
    return WatchedOutput(
        txid=txid,
        vout=vout,
        duffs=duffs,
        confirmations=confirmations,
        instantlock=instantlock,
        chainlock=chainlock,
        detected_at=detected,
    )


def _apply(state=DashInvoiceState.OPEN, now=None, outputs=None, policy=None, canceled=False):
    return apply_settlement(
        state=state,
        now=now or T0 + timedelta(minutes=2),
        expires_at=EXPIRES,
        settle_deadline_at=DEADLINE,
        duffs_quoted=50_000_000,
        sats_requested=25_000,
        policy=policy or IS_POLICY,
        outputs=outputs or [],
        canceled=canceled,
    )


def test_instantsend_settles() -> None:
    d = _apply(outputs=[_out(instantlock=True)])
    assert d.state == DashInvoiceState.SETTLED
    assert d.sats_credited == 25_000


def test_chainlock_settles() -> None:
    d = _apply(outputs=[_out(chainlock=True, confirmations=1)])
    assert d.state == DashInvoiceState.SETTLED


def test_one_conf_without_cl_does_not_settle_under_is_policy() -> None:
    d = _apply(outputs=[_out(confirmations=1, instantlock=False, chainlock=False)])
    assert d.state == DashInvoiceState.DETECTED
    assert d.sats_credited is None


def test_first_seen_before_expiry_settles_after_expiry() -> None:
    seen = T0 + timedelta(minutes=14)
    now = EXPIRES + timedelta(minutes=1)
    d = _apply(now=now, outputs=[_out(detected=seen, instantlock=True)])
    assert d.state == DashInvoiceState.SETTLED
    assert d.first_seen_at == seen


def test_first_seen_after_expiry_is_late() -> None:
    late = EXPIRES + timedelta(seconds=1)
    d = _apply(now=late + timedelta(seconds=1), outputs=[_out(detected=late, instantlock=True)])
    assert d.state == DashInvoiceState.EXPIRED
    assert d.late_payment is True
    assert d.sats_credited is None


def test_cancel_then_pay_is_late() -> None:
    d = _apply(
        state=DashInvoiceState.CANCELED,
        canceled=True,
        outputs=[_out(instantlock=True, detected=T0 + timedelta(minutes=1))],
    )
    assert d.state == DashInvoiceState.CANCELED
    assert d.late_payment is True
    assert d.sats_credited is None


def test_underpay_at_expiry() -> None:
    d = _apply(
        now=EXPIRES + timedelta(seconds=1),
        outputs=[_out(duffs=1_000_000, instantlock=True)],
    )
    assert d.state == DashInvoiceState.UNDERPAID
    assert d.sats_credited is None


def test_overpay() -> None:
    d = _apply(outputs=[_out(duffs=60_000_000, instantlock=True)])
    assert d.state == DashInvoiceState.OVERPAID
    assert d.sats_credited == 25_000


def test_tolerance_counts_as_settled() -> None:
    d = _apply(outputs=[_out(duffs=49_500_000, instantlock=True)])
    assert d.state == DashInvoiceState.SETTLED


def test_conf_n_settles_at_minconf() -> None:
    d = _apply(outputs=[_out(confirmations=1)], policy=CONF_POLICY)
    assert d.state == DashInvoiceState.SETTLED


def test_no_outputs_after_expiry_expires() -> None:
    d = _apply(now=EXPIRES + timedelta(seconds=1), outputs=[])
    assert d.state == DashInvoiceState.EXPIRED


def test_unconfirmed_after_expiry_stays_detected() -> None:
    seen = T0 + timedelta(minutes=1)
    d = _apply(
        now=EXPIRES + timedelta(minutes=2),
        outputs=[_out(detected=seen, confirmations=1, instantlock=False, chainlock=False)],
    )
    assert d.state == DashInvoiceState.DETECTED
    assert d.stuck is False


def test_stuck_after_settle_deadline() -> None:
    seen = T0 + timedelta(minutes=1)
    d = _apply(
        now=DEADLINE + timedelta(seconds=1),
        outputs=[_out(detected=seen, confirmations=1)],
    )
    assert d.state == DashInvoiceState.DETECTED
    assert d.stuck is True
