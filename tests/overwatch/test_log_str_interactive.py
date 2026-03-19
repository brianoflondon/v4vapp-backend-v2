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
    EXTERNAL_TO_HIVE_FLOW,
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
    event_value: str = "",
) -> FlowEvent:
    return FlowEvent(
        event_type="op",
        timestamp=ts or _NOW,
        group_id=group_id,
        short_id=short_id,
        op_type=op_type,
        group="primary",
        event_value=event_value,
    )


def _ledger(
    ledger_type: LedgerType,
    group_id: str = "gid_primary",
    short_id: str = "3410_47a073_1",
    ts: datetime | None = None,
    event_value: str = "",
) -> FlowEvent:
    return FlowEvent(
        event_type="ledger",
        timestamp=ts or _NOW,
        group_id=group_id,
        short_id=short_id,
        ledger_type=ledger_type,
        group="primary",
        event_value=event_value,
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
    print(f"    log_str:    {flow.log_str}")
    print(f"    flow_value: {flow.flow_value!r}")
    print(f"    progress:   {flow.progress}")
    print(f"    status:     {flow.status.value}")
    print(f"    matched:    {sorted(flow.matched_stage_names)}")
    print(f"    missing:    {[s.name for s in flow.missing_stages]}")
    if flow.duration is not None:
        print(f"    duration:   {flow.duration:.1f}s")
    # Show per-event values for the most recent batch
    vals = [
        (e.event_type, e.op_type or str(e.ledger_type), e.event_value)
        for e in flow.events
        if e.event_value
    ]
    if vals:
        print(f"    values:     {vals[-3:]}")  # last 3 events with values


# ---------------------------------------------------------------------------
# 1. hive_to_keepsats — transfer trigger, 14 required stages
# ---------------------------------------------------------------------------


def test_hive_to_keepsats_log_str():
    _banner("hive_to_keepsats (transfer → 14 required stages)")
    f = _make_flow(HIVE_TO_KEEPSATS_FLOW, short_id="6921_b9c54f_1")

    # Trigger — 10 HIVE deposit → 987 sats gross
    f.add_event(_op("transfer", short_id="6921_b9c54f_1", event_value="987 sats"))
    _stage("after trigger", f)

    # Primary ledgers
    for lt, val in [
        (LedgerType.CUSTOMER_HIVE_IN, "10.000 HIVE"),
        (LedgerType.HOLD_KEEPSATS, "987 sats"),
        (LedgerType.CONV_HIVE_TO_KEEPSATS, "10.000 HIVE"),
        (LedgerType.CONTRA_HIVE_TO_KEEPSATS, "987 sats"),
        (LedgerType.CONV_CUSTOMER, "987 sats"),
        (LedgerType.RELEASE_KEEPSATS, "918 sats"),
    ]:
        f.add_event(_ledger(lt, event_value=val))
    _stage("after primary ledgers (7/14)", f)

    # Fee notification — 69 sat fee
    f.add_event(_op("custom_json", group_id="gid_fee", short_id="sid_fee", event_value="69 sats"))
    f.add_event(
        _ledger(
            LedgerType.CUSTOM_JSON_FEE,
            group_id="gid_fee",
            short_id="sid_fee",
            event_value="69 sats",
        )
    )
    f.add_event(
        _ledger(
            LedgerType.FEE_INCOME, group_id="gid_fee", short_id="sid_fee", event_value="69 sats"
        )
    )
    _stage("after fee stages (10/14)", f)

    # Keepsats notification — 918 sats net deposited
    f.add_event(
        _op("custom_json", group_id="gid_notif", short_id="sid_notif", event_value="918 sats")
    )
    f.add_event(
        _ledger(
            LedgerType.RECEIVE_LIGHTNING,
            group_id="gid_notif",
            short_id="sid_notif",
            event_value="918 sats",
        )
    )
    _stage("after keepsats notification (12/14)", f)

    # Change return — 0.001 HIVE dust
    f.add_event(
        _op("transfer", group_id="gid_change", short_id="sid_change", event_value="0 sats")
    )
    f.add_event(
        _ledger(
            LedgerType.CUSTOMER_HIVE_OUT,
            group_id="gid_change",
            short_id="sid_change",
            event_value="0.001 HIVE",
        )
    )
    _stage("COMPLETED (14/14)", f)


# ---------------------------------------------------------------------------
# 2. hive_to_keepsats_external — transfer trigger, 17 required stages
# ---------------------------------------------------------------------------


def test_hive_to_keepsats_external_log_str():
    _banner("hive_to_keepsats_external (transfer → 17 required stages)")
    f = _make_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW, short_id="6921_b9c54f_1")

    # Trigger — 10 HIVE → external lightning payment
    f.add_event(_op("transfer", short_id="6921_b9c54f_1", event_value="987 sats"))
    _stage("after trigger", f)

    # Primary ledgers
    for lt, val in [
        (LedgerType.CUSTOMER_HIVE_IN, "10.000 HIVE"),
        (LedgerType.HOLD_KEEPSATS, "987 sats"),
        (LedgerType.CONV_HIVE_TO_KEEPSATS, "10.000 HIVE"),
        (LedgerType.CONTRA_HIVE_TO_KEEPSATS, "987 sats"),
        (LedgerType.CONV_CUSTOMER, "987 sats"),
        (LedgerType.RELEASE_KEEPSATS, "918 sats"),
    ]:
        f.add_event(_ledger(lt, event_value=val))
    _stage("after primary ledgers (7/17)", f)

    # External payment — 918 sats sent, 3 sats routing fee
    f.add_event(_op("payment", group_id="gid_pay", short_id="sid_pay", event_value="918 sats"))
    f.add_event(
        _ledger(
            LedgerType.WITHDRAW_LIGHTNING,
            group_id="gid_pay",
            short_id="sid_pay",
            event_value="918 sats",
        )
    )
    f.add_event(
        _ledger(
            LedgerType.FEE_EXPENSE, group_id="gid_pay", short_id="sid_pay", event_value="3 sats"
        )
    )
    _stage("after payment stages (10/17)", f)

    # Fee notification — 69 sat fee
    f.add_event(_op("custom_json", group_id="gid_fee", short_id="sid_fee", event_value="69 sats"))
    f.add_event(
        _ledger(
            LedgerType.CUSTOM_JSON_FEE,
            group_id="gid_fee",
            short_id="sid_fee",
            event_value="69 sats",
        )
    )
    f.add_event(
        _ledger(
            LedgerType.FEE_INCOME, group_id="gid_fee", short_id="sid_fee", event_value="69 sats"
        )
    )
    # Keepsats notification
    f.add_event(
        _op("custom_json", group_id="gid_notif", short_id="sid_notif", event_value="918 sats")
    )
    f.add_event(
        _ledger(
            LedgerType.RECEIVE_LIGHTNING,
            group_id="gid_notif",
            short_id="sid_notif",
            event_value="918 sats",
        )
    )
    # Change return — 0.001 HIVE dust
    f.add_event(
        _op("transfer", group_id="gid_change", short_id="sid_change", event_value="0 sats")
    )
    f.add_event(
        _ledger(
            LedgerType.CUSTOMER_HIVE_OUT,
            group_id="gid_change",
            short_id="sid_change",
            event_value="0.001 HIVE",
        )
    )
    _stage("COMPLETED (17/17)", f)


# ---------------------------------------------------------------------------
# 3. keepsats_to_hive — custom_json trigger, 12 required + 5 optional
# ---------------------------------------------------------------------------


def test_keepsats_to_hive_log_str():
    _banner("keepsats_to_hive (custom_json → 12 required, 5 optional)")
    f = _make_flow(KEEPSATS_TO_HIVE_FLOW, short_id="3991_317497_1")

    # Trigger — 2,000 sats → HBD
    f.add_event(_op("custom_json", short_id="3991_317497_1", event_value="2000 sats"))
    _stage("after trigger", f)

    # Primary ledgers
    for lt, val in [
        (LedgerType.CUSTOM_JSON_TRANSFER, "2000 sats"),
        (LedgerType.CONTRA_KEEPSATS_TO_HIVE, "2000 sats"),
        (LedgerType.CUSTOM_JSON_FEE_REFUND, "88 sats"),
        (LedgerType.FEE_INCOME, "88 sats"),
        (LedgerType.CONV_CUSTOMER, "1912 sats"),
        (LedgerType.RECLASSIFY_VSC_SATS, "1912 sats"),
        (LedgerType.RECLASSIFY_VSC_HIVE, "19.896 HIVE"),
        (LedgerType.EXCHANGE_CONVERSION, "19.896 HIVE"),
        (LedgerType.EXCHANGE_FEES, "0.020 HIVE"),
    ]:
        f.add_event(_ledger(lt, event_value=val))
    _stage("after primary ledgers (10/12)", f)

    # HBD transfer — 1.379 HBD sent to customer
    f.add_event(_op("transfer", group_id="gid_hbd", short_id="sid_hbd", event_value="1911 sats"))
    f.add_event(
        _ledger(
            LedgerType.CUSTOMER_HIVE_OUT,
            group_id="gid_hbd",
            short_id="sid_hbd",
            event_value="1.379 HBD",
        )
    )
    _stage("COMPLETED (12/12 required)", f)

    # Optional: notification
    f.add_event(
        _op("custom_json", group_id="gid_notif", short_id="sid_notif", event_value="1911 sats")
    )
    _stage("after optional notification", f)


# ---------------------------------------------------------------------------
# 4. keepsats_to_external — custom_json trigger, 6 required + 1 optional
# ---------------------------------------------------------------------------


def test_keepsats_to_external_log_str():
    _banner("keepsats_to_external (custom_json → 6 required, 1 optional)")
    f = _make_flow(KEEPSATS_TO_EXTERNAL_FLOW, short_id="6454_ba3351_1")

    # Trigger — 1,285 sats external lightning payment
    f.add_event(_op("custom_json", short_id="6454_ba3351_1", event_value="1285 sats"))
    _stage("after trigger", f)

    # Primary ledgers — hold and release
    f.add_event(_ledger(LedgerType.HOLD_KEEPSATS, event_value="1285 sats"))
    f.add_event(_ledger(LedgerType.RELEASE_KEEPSATS, event_value="1285 sats"))
    _stage("after primary ledgers (3/6)", f)

    # Payment — sent externally, 5 sat routing fee
    f.add_event(_op("payment", group_id="gid_pay", short_id="sid_pay", event_value="1285 sats"))
    f.add_event(
        _ledger(
            LedgerType.WITHDRAW_LIGHTNING,
            group_id="gid_pay",
            short_id="sid_pay",
            event_value="1285 sats",
        )
    )
    f.add_event(
        _ledger(
            LedgerType.FEE_EXPENSE, group_id="gid_pay", short_id="sid_pay", event_value="5 sats"
        )
    )
    _stage("COMPLETED (6/6 required)", f)

    # Optional: notification
    f.add_event(
        _op("custom_json", group_id="gid_notif", short_id="sid_notif", event_value="1285 sats")
    )
    _stage("after optional notification", f)


# ---------------------------------------------------------------------------
# 5. external_to_keepsats — invoice trigger, 4 required + 3 optional
# ---------------------------------------------------------------------------


def test_external_to_keepsats_log_str():
    _banner("external_to_keepsats (invoice → 4 required, 3 optional)")
    f = _make_flow(EXTERNAL_TO_KEEPSATS_FLOW, short_id="inv_abc123")

    # Trigger — 5,000 sat invoice received
    f.add_event(_op("invoice", short_id="inv_abc123", event_value="5000 sats"))
    _stage("after trigger", f)

    # Deposit lightning
    f.add_event(_ledger(LedgerType.DEPOSIT_LIGHTNING, event_value="5000 sats"))
    _stage("after deposit (2/4)", f)

    # Keepsats notification — credited to balance
    f.add_event(
        _op("custom_json", group_id="gid_notif", short_id="sid_notif", event_value="5000 sats")
    )
    f.add_event(
        _ledger(
            LedgerType.RECEIVE_LIGHTNING,
            group_id="gid_notif",
            short_id="sid_notif",
            event_value="5000 sats",
        )
    )
    _stage("COMPLETED (4/4 required)", f)

    # Optional: hive notification transfer
    f.add_event(_op("transfer", group_id="gid_hive", short_id="sid_hive", event_value="5000 sats"))
    _stage("after optional hive transfer", f)

    # Optional: customer hive out
    f.add_event(
        _ledger(
            LedgerType.CUSTOMER_HIVE_OUT,
            group_id="gid_hive",
            short_id="sid_hive",
            event_value="56.821 HIVE",
        )
    )
    _stage("after optional cust_h_out", f)

    # Optional: notification custom_json
    f.add_event(
        _op("custom_json", group_id="gid_notif2", short_id="sid_notif2", event_value="5000 sats")
    )
    _stage("after optional notification custom_json", f)


# ---------------------------------------------------------------------------
# 6. external_to_hive — invoice trigger, 6 required + 1 optional
# ---------------------------------------------------------------------------


def test_external_to_hive_log_str():
    _banner("external_to_hive (invoice → 6 required, 1 optional)")
    f = _make_flow(EXTERNAL_TO_HIVE_FLOW, short_id="B9XaNJm/x4")

    # Trigger — 5,815 sat invoice received, converting to HIVE
    f.add_event(_op("invoice", short_id="B9XaNJm/x4", event_value="5815 sats"))
    _stage("after trigger", f)

    # Deposit lightning
    f.add_event(_ledger(LedgerType.DEPOSIT_LIGHTNING, event_value="5815 sats"))
    _stage("after deposit (2/6)", f)

    # Keepsats notification
    f.add_event(
        _op("custom_json", group_id="gid_notif", short_id="sid_notif", event_value="5815 sats")
    )
    f.add_event(
        _ledger(
            LedgerType.RECEIVE_LIGHTNING,
            group_id="gid_notif",
            short_id="sid_notif",
            event_value="5815 sats",
        )
    )
    _stage("after keepsats notification (4/6)", f)

    # HIVE payout — 62.963 HIVE sent to customer
    f.add_event(_op("transfer", group_id="gid_hive", short_id="sid_hive", event_value="5815 sats"))
    f.add_event(
        _ledger(
            LedgerType.CUSTOMER_HIVE_OUT,
            group_id="gid_hive",
            short_id="sid_hive",
            event_value="62.963 HIVE",
        )
    )
    _stage("COMPLETED (6/6 required)", f)


# ---------------------------------------------------------------------------
# 7. keepsats_internal_transfer — custom_json trigger, 2 required + 1 optional
# ---------------------------------------------------------------------------


def test_keepsats_internal_transfer_log_str():
    _banner("keepsats_internal_transfer (custom_json → 2 required, 1 optional)")
    f = _make_flow(KEEPSATS_INTERNAL_TRANSFER_FLOW)

    # Trigger — 500 sat internal transfer between users
    f.add_event(_op("custom_json", event_value="500 sats"))
    _stage("after trigger", f)

    # Custom json transfer ledger
    f.add_event(_ledger(LedgerType.CUSTOM_JSON_TRANSFER, event_value="500 sats"))
    _stage("COMPLETED (2/2 required)", f)

    # Optional: notification
    f.add_event(
        _op("custom_json", group_id="gid_notif", short_id="sid_notif", event_value="500 sats")
    )
    _stage("after optional notification", f)


# ---------------------------------------------------------------------------
# 8. Stalled flow example
# ---------------------------------------------------------------------------


def test_stalled_flow_log_str():
    _banner("STALLED flow example")
    f = _make_flow(HIVE_TO_KEEPSATS_FLOW, short_id="6921_b9c54f_1")
    f.add_event(
        _op(
            "transfer",
            short_id="6921_b9c54f_1",
            ts=_NOW - timedelta(minutes=10),
            event_value="987 sats",
        )
    )
    f.add_event(
        _ledger(
            LedgerType.CUSTOMER_HIVE_IN, ts=_NOW - timedelta(minutes=10), event_value="10.000 HIVE"
        )
    )
    f.status = FlowStatus.STALLED
    _stage("STALLED (only 2/14)", f)


# ---------------------------------------------------------------------------
# 9. Summary of all flow definitions
# ---------------------------------------------------------------------------


def test_all_definitions_summary():
    _banner("All flow definitions summary")
    for defn in [
        HIVE_TO_KEEPSATS_FLOW,
        HIVE_TO_KEEPSATS_EXTERNAL_FLOW,
        KEEPSATS_TO_HIVE_FLOW,
        KEEPSATS_TO_EXTERNAL_FLOW,
        EXTERNAL_TO_KEEPSATS_FLOW,
        EXTERNAL_TO_HIVE_FLOW,
        KEEPSATS_INTERNAL_TRANSFER_FLOW,
    ]:
        req = len(defn.required_stages)
        opt = len(defn.stages) - req
        print(f"\n  {defn.name}")
        print(f"    trigger: {defn.trigger_op_type}")
        print(f"    stages:  {req} required, {opt} optional ({len(defn.stages)} total)")
        print(f"    names:   {defn.stage_names}")
