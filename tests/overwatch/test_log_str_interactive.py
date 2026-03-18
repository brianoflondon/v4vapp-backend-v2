"""
Interactive test script for building FlowInstance.log_str.

Run with:
    uv run pytest tests/overwatch/test_log_str_interactive.py -s

Prints log_str for each flow type at various lifecycle stages so you
can see the current output and iterate on the format.
"""

from datetime import datetime, timedelta, timezone

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.process.overwatch_flows import (
    EXTERNAL_TO_KEEPSATS_FLOW,
    HIVE_TO_KEEPSATS_EXTERNAL_FLOW,
    HIVE_TO_KEEPSATS_FLOW,
    KEEPSATS_INTERNAL_TRANSFER_FLOW,
    KEEPSATS_TO_EXTERNAL_FLOW,
    KEEPSATS_TO_HIVE_FLOW,
)
from v4vapp_backend_v2.process.process_overwatch import FlowEvent, FlowInstance, FlowStatus

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime.now(tz=timezone.utc)


def _op(
    op_type: str,
    group_id: str = "gid_primary",
    short_id: str = "3410_47a073_1",
    ts: datetime | None = None,
) -> FlowEvent:
    return FlowEvent(
        event_type="op",
        timestamp=ts or _NOW,
        group_id=group_id,
        short_id=short_id,
        op_type=op_type,
        group="primary",
    )


def _ledger(
    ledger_type: LedgerType,
    group_id: str = "gid_primary",
    short_id: str = "3410_47a073_1",
    ts: datetime | None = None,
) -> FlowEvent:
    return FlowEvent(
        event_type="ledger",
        timestamp=ts or _NOW,
        group_id=group_id,
        short_id=short_id,
        ledger_type=ledger_type,
        group="primary",
    )


def _make_flow(defn, short_id="3410_47a073_1", cust_id="v4vapp-test") -> FlowInstance:
    return FlowInstance(
        flow_definition=defn,
        trigger_group_id="gid_trigger",
        trigger_short_id=short_id,
        cust_id=cust_id,
        status=FlowStatus.PENDING,
        started_at=_NOW,
    )


def _banner(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


def _stage(label: str, flow: FlowInstance) -> None:
    print(f"\n  [{label}]")
    print(f"    log_str:  {flow.log_str}")
    print(f"    progress: {flow.progress}")
    print(f"    status:   {flow.status.value}")
    print(f"    matched:  {sorted(flow.matched_stage_names)}")
    print(f"    missing:  {[s.name for s in flow.missing_stages]}")
    if flow.duration is not None:
        print(f"    duration: {flow.duration:.1f}s")


# ---------------------------------------------------------------------------
# 1. hive_to_keepsats — transfer trigger, 14 required stages
# ---------------------------------------------------------------------------


def test_hive_to_keepsats_log_str():
    _banner("hive_to_keepsats (transfer → 14 required stages)")
    f = _make_flow(HIVE_TO_KEEPSATS_FLOW)

    # Trigger
    f.add_event(_op("transfer"))
    _stage("after trigger", f)

    # Some primary ledgers
    for lt in [
        LedgerType.CUSTOMER_HIVE_IN,
        LedgerType.HOLD_KEEPSATS,
        LedgerType.CONV_HIVE_TO_KEEPSATS,
        LedgerType.CONTRA_HIVE_TO_KEEPSATS,
        LedgerType.CONV_CUSTOMER,
        LedgerType.RELEASE_KEEPSATS,
    ]:
        f.add_event(_ledger(lt))
    _stage("after primary ledgers (7/14)", f)

    # Fee notification
    f.add_event(_op("custom_json", group_id="gid_fee", short_id="sid_fee"))
    f.add_event(_ledger(LedgerType.CUSTOM_JSON_FEE, group_id="gid_fee", short_id="sid_fee"))
    f.add_event(_ledger(LedgerType.FEE_INCOME, group_id="gid_fee", short_id="sid_fee"))
    _stage("after fee stages (10/14)", f)

    # Keepsats notification
    f.add_event(_op("custom_json", group_id="gid_notif", short_id="sid_notif"))
    f.add_event(_ledger(LedgerType.RECEIVE_LIGHTNING, group_id="gid_notif", short_id="sid_notif"))
    _stage("after keepsats notification (12/14)", f)

    # Change return
    f.add_event(_op("transfer", group_id="gid_change", short_id="sid_change"))
    f.add_event(
        _ledger(LedgerType.CUSTOMER_HIVE_OUT, group_id="gid_change", short_id="sid_change")
    )
    _stage("COMPLETED (14/14)", f)


# ---------------------------------------------------------------------------
# 2. hive_to_keepsats_external — transfer trigger, 17 required stages
# ---------------------------------------------------------------------------


def test_hive_to_keepsats_external_log_str():
    _banner("hive_to_keepsats_external (transfer → 17 required stages)")
    f = _make_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)

    # Trigger
    f.add_event(_op("transfer"))
    _stage("after trigger", f)

    # Primary ledgers
    for lt in [
        LedgerType.CUSTOMER_HIVE_IN,
        LedgerType.HOLD_KEEPSATS,
        LedgerType.CONV_HIVE_TO_KEEPSATS,
        LedgerType.CONTRA_HIVE_TO_KEEPSATS,
        LedgerType.CONV_CUSTOMER,
        LedgerType.RELEASE_KEEPSATS,
    ]:
        f.add_event(_ledger(lt))
    _stage("after primary ledgers (7/17)", f)

    # Payment
    f.add_event(_op("payment", group_id="gid_pay", short_id="sid_pay"))
    f.add_event(_ledger(LedgerType.WITHDRAW_LIGHTNING, group_id="gid_pay", short_id="sid_pay"))
    f.add_event(_ledger(LedgerType.FEE_EXPENSE, group_id="gid_pay", short_id="sid_pay"))
    _stage("after payment stages (10/17)", f)

    # Fee notification
    f.add_event(_op("custom_json", group_id="gid_fee", short_id="sid_fee"))
    f.add_event(_ledger(LedgerType.CUSTOM_JSON_FEE, group_id="gid_fee", short_id="sid_fee"))
    f.add_event(_ledger(LedgerType.FEE_INCOME, group_id="gid_fee", short_id="sid_fee"))
    # Keepsats notification
    f.add_event(_op("custom_json", group_id="gid_notif", short_id="sid_notif"))
    f.add_event(_ledger(LedgerType.RECEIVE_LIGHTNING, group_id="gid_notif", short_id="sid_notif"))
    # Change return
    f.add_event(_op("transfer", group_id="gid_change", short_id="sid_change"))
    f.add_event(
        _ledger(LedgerType.CUSTOMER_HIVE_OUT, group_id="gid_change", short_id="sid_change")
    )
    _stage("COMPLETED (17/17)", f)


# ---------------------------------------------------------------------------
# 3. keepsats_to_hive — custom_json trigger, 12 required + 5 optional
# ---------------------------------------------------------------------------


def test_keepsats_to_hive_log_str():
    _banner("keepsats_to_hive (custom_json → 12 required, 5 optional)")
    f = _make_flow(KEEPSATS_TO_HIVE_FLOW)

    # Trigger
    f.add_event(_op("custom_json"))
    _stage("after trigger", f)

    # Primary ledgers
    for lt in [
        LedgerType.CUSTOM_JSON_TRANSFER,
        LedgerType.CONTRA_KEEPSATS_TO_HIVE,
        LedgerType.CUSTOM_JSON_FEE_REFUND,
        LedgerType.FEE_INCOME,
        LedgerType.CONV_CUSTOMER,
        LedgerType.RECLASSIFY_VSC_SATS,
        LedgerType.RECLASSIFY_VSC_HIVE,
        LedgerType.EXCHANGE_CONVERSION,
        LedgerType.EXCHANGE_FEES,
    ]:
        f.add_event(_ledger(lt))
    _stage("after primary ledgers (10/12)", f)

    # HBD transfer
    f.add_event(_op("transfer", group_id="gid_hbd", short_id="sid_hbd"))
    f.add_event(_ledger(LedgerType.CUSTOMER_HIVE_OUT, group_id="gid_hbd", short_id="sid_hbd"))
    _stage("COMPLETED (12/12 required)", f)

    # Optional: notification
    f.add_event(_op("custom_json", group_id="gid_notif", short_id="sid_notif"))
    _stage("after optional notification", f)


# ---------------------------------------------------------------------------
# 4. keepsats_to_external — custom_json trigger, 6 required + 1 optional
# ---------------------------------------------------------------------------


def test_keepsats_to_external_log_str():
    _banner("keepsats_to_external (custom_json → 6 required, 1 optional)")
    f = _make_flow(KEEPSATS_TO_EXTERNAL_FLOW)

    # Trigger
    f.add_event(_op("custom_json"))
    _stage("after trigger", f)

    # Primary ledgers
    f.add_event(_ledger(LedgerType.HOLD_KEEPSATS))
    f.add_event(_ledger(LedgerType.RELEASE_KEEPSATS))
    _stage("after primary ledgers (3/6)", f)

    # Payment
    f.add_event(_op("payment", group_id="gid_pay", short_id="sid_pay"))
    f.add_event(_ledger(LedgerType.WITHDRAW_LIGHTNING, group_id="gid_pay", short_id="sid_pay"))
    f.add_event(_ledger(LedgerType.FEE_EXPENSE, group_id="gid_pay", short_id="sid_pay"))
    _stage("COMPLETED (6/6 required)", f)

    # Optional: notification
    f.add_event(_op("custom_json", group_id="gid_notif", short_id="sid_notif"))
    _stage("after optional notification", f)


# ---------------------------------------------------------------------------
# 5. external_to_keepsats — invoice trigger, 4 required + 3 optional
# ---------------------------------------------------------------------------


def test_external_to_keepsats_log_str():
    _banner("external_to_keepsats (invoice → 4 required, 3 optional)")
    f = _make_flow(EXTERNAL_TO_KEEPSATS_FLOW, short_id="inv_abc123")

    # Trigger
    f.add_event(_op("invoice", short_id="inv_abc123"))
    _stage("after trigger", f)

    # Deposit lightning
    f.add_event(_ledger(LedgerType.DEPOSIT_LIGHTNING))
    _stage("after deposit (2/4)", f)

    # Keepsats notification
    f.add_event(_op("custom_json", group_id="gid_notif", short_id="sid_notif"))
    f.add_event(_ledger(LedgerType.RECEIVE_LIGHTNING, group_id="gid_notif", short_id="sid_notif"))
    _stage("COMPLETED (4/4 required)", f)

    # Optional: hive notification transfer
    f.add_event(_op("transfer", group_id="gid_hive", short_id="sid_hive"))
    _stage("after optional hive transfer", f)

    # Optional: customer hive out
    f.add_event(_ledger(LedgerType.CUSTOMER_HIVE_OUT, group_id="gid_hive", short_id="sid_hive"))
    _stage("after optional cust_h_out", f)

    # Optional: notification custom_json
    f.add_event(_op("custom_json", group_id="gid_notif2", short_id="sid_notif2"))
    _stage("after optional notification custom_json", f)


# ---------------------------------------------------------------------------
# 6. keepsats_internal_transfer — custom_json trigger, 2 required + 1 optional
# ---------------------------------------------------------------------------


def test_keepsats_internal_transfer_log_str():
    _banner("keepsats_internal_transfer (custom_json → 2 required, 1 optional)")
    f = _make_flow(KEEPSATS_INTERNAL_TRANSFER_FLOW)

    # Trigger
    f.add_event(_op("custom_json"))
    _stage("after trigger", f)

    # Custom json transfer ledger
    f.add_event(_ledger(LedgerType.CUSTOM_JSON_TRANSFER))
    _stage("COMPLETED (2/2 required)", f)

    # Optional: notification
    f.add_event(_op("custom_json", group_id="gid_notif", short_id="sid_notif"))
    _stage("after optional notification", f)


# ---------------------------------------------------------------------------
# 7. Stalled flow example
# ---------------------------------------------------------------------------


def test_stalled_flow_log_str():
    _banner("STALLED flow example")
    f = _make_flow(HIVE_TO_KEEPSATS_FLOW)
    f.add_event(_op("transfer", ts=_NOW - timedelta(minutes=10)))
    f.add_event(_ledger(LedgerType.CUSTOMER_HIVE_IN, ts=_NOW - timedelta(minutes=10)))
    f.status = FlowStatus.STALLED
    _stage("STALLED (only 2/14)", f)


# ---------------------------------------------------------------------------
# 8. Summary of all flow definitions
# ---------------------------------------------------------------------------


def test_all_definitions_summary():
    _banner("All flow definitions summary")
    for defn in [
        HIVE_TO_KEEPSATS_FLOW,
        HIVE_TO_KEEPSATS_EXTERNAL_FLOW,
        KEEPSATS_TO_HIVE_FLOW,
        KEEPSATS_TO_EXTERNAL_FLOW,
        EXTERNAL_TO_KEEPSATS_FLOW,
        KEEPSATS_INTERNAL_TRANSFER_FLOW,
    ]:
        req = len(defn.required_stages)
        opt = len(defn.stages) - req
        print(f"\n  {defn.name}")
        print(f"    trigger: {defn.trigger_op_type}")
        print(f"    stages:  {req} required, {opt} optional ({len(defn.stages)} total)")
        print(f"    names:   {defn.stage_names}")
