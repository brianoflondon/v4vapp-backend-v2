"""
Tests for the Overwatch system: transaction flow tracking and completeness verification.

Uses real flow data extracted from db_monitor logs for a Hive-to-Keepsats conversion.
"""

import json
from datetime import timedelta
from pathlib import Path

import pytest

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.process.overwatch_flows import HIVE_TO_KEEPSATS_FLOW
from v4vapp_backend_v2.process.process_overwatch import (
    FlowDefinition,
    FlowEvent,
    FlowInstance,
    FlowStage,
    FlowStatus,
    Overwatch,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FLOW_DATA_PATH = Path("tests/data/overwatch/hive_to_keepsats_flow.json")


@pytest.fixture
def flow_data() -> dict:
    """Load the Hive-to-Keepsats flow test data."""
    with open(FLOW_DATA_PATH) as f:
        return json.load(f)


@pytest.fixture
def primary_ledger_entries(flow_data: dict) -> dict[str, LedgerEntry]:
    """Parse primary ledger entries from the test data."""
    entries = {}
    for key, data in flow_data["primary_ledger_entries"].items():
        entries[key] = LedgerEntry(**data)
    return entries


@pytest.fixture
def fee_ledger_entries(flow_data: dict) -> dict[str, LedgerEntry]:
    """Parse fee notification ledger entries from the test data."""
    entries = {}
    for key, data in flow_data["fee_ledger_entries"].items():
        entries[key] = LedgerEntry(**data)
    return entries


@pytest.fixture
def keepsats_notification_ledger_entries(flow_data: dict) -> dict[str, LedgerEntry]:
    """Parse keepsats notification ledger entries from the test data."""
    entries = {}
    for key, data in flow_data["keepsats_notification_ledger_entries"].items():
        entries[key] = LedgerEntry(**data)
    return entries


@pytest.fixture
def change_ledger_entries(flow_data: dict) -> dict[str, LedgerEntry]:
    """Parse change return ledger entries from the test data."""
    entries = {}
    for key, data in flow_data["change_ledger_entries"].items():
        entries[key] = LedgerEntry(**data)
    return entries


@pytest.fixture
def all_flow_events(
    flow_data: dict,
    primary_ledger_entries: dict[str, LedgerEntry],
    fee_ledger_entries: dict[str, LedgerEntry],
    keepsats_notification_ledger_entries: dict[str, LedgerEntry],
    change_ledger_entries: dict[str, LedgerEntry],
) -> list[FlowEvent]:
    """Build a list of all FlowEvents for the complete Hive-to-Keepsats flow.

    All events use the default ``group="primary"`` to simulate what
    ``db_monitor`` actually sends — the Overwatch system must be able to
    match stages without the caller specifying the correct group.
    """
    events: list[FlowEvent] = []

    # 1. Trigger transfer op
    trigger = flow_data["trigger_transfer"]
    events.append(
        FlowEvent(
            event_type="op",
            timestamp=trigger["timestamp"],
            group_id=trigger["group_id"],
            short_id=trigger["short_id"],
            op_type=trigger["type"],
            group="primary",
            ledger_entry=None,
            op=None,
            ledger_type=None,
        )
    )

    # 2. Primary ledger entries
    for le in primary_ledger_entries.values():
        events.append(FlowEvent.from_ledger_entry(le, group="primary"))

    # 3. Fee notification custom_json op
    fee_op = flow_data["fee_notification_op"]
    events.append(
        FlowEvent(
            event_type="op",
            timestamp=fee_op["timestamp"],
            group_id=fee_op["group_id"],
            short_id=fee_op["short_id"],
            op_type=fee_op["type"],
            group="primary",
            ledger_entry=None,
            op=None,
            ledger_type=None,
        )
    )

    # 4. Fee ledger entries
    for le in fee_ledger_entries.values():
        events.append(FlowEvent.from_ledger_entry(le, group="primary"))

    # 5. Keepsats notification custom_json op
    ks_op = flow_data["keepsats_notification_op"]
    events.append(
        FlowEvent(
            event_type="op",
            timestamp=ks_op["timestamp"],
            group_id=ks_op["group_id"],
            short_id=ks_op["short_id"],
            op_type=ks_op["type"],
            group="primary",
            ledger_entry=None,
            op=None,
            ledger_type=None,
        )
    )

    # 6. Keepsats notification ledger entries
    for le in keepsats_notification_ledger_entries.values():
        events.append(FlowEvent.from_ledger_entry(le, group="primary"))

    # 7. Change transfer op
    change_op = flow_data["change_transfer"]
    events.append(
        FlowEvent(
            event_type="op",
            timestamp=change_op["timestamp"],
            group_id=change_op["group_id"],
            short_id=change_op["short_id"],
            op_type=change_op["type"],
            group="primary",
            ledger_entry=None,
            op=None,
            ledger_type=None,
        )
    )

    # 8. Change ledger entries
    for le in change_ledger_entries.values():
        events.append(FlowEvent.from_ledger_entry(le, group="primary"))

    return events


@pytest.fixture
def flow_instance(flow_data: dict) -> FlowInstance:
    """Create an empty FlowInstance for the Hive-to-Keepsats flow."""
    trigger = flow_data["trigger_transfer"]
    return FlowInstance(
        flow_definition=HIVE_TO_KEEPSATS_FLOW,
        trigger_group_id=trigger["group_id"],
        trigger_short_id=trigger["short_id"],
        cust_id=trigger["cust_id"],
    )


# ---------------------------------------------------------------------------
# Tests: FlowStage
# ---------------------------------------------------------------------------


class TestFlowStage:
    """Tests for the FlowStage model."""

    def test_ledger_stage_matches_correct_event(self):
        stage = FlowStage(
            name="customer_hive_in",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOMER_HIVE_IN,
        )
        event = FlowEvent(
            event_type="ledger",
            ledger_type=LedgerType.CUSTOMER_HIVE_IN,
        )
        assert stage.matches(event)

    def test_ledger_stage_rejects_wrong_type(self):
        stage = FlowStage(
            name="customer_hive_in",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOMER_HIVE_IN,
        )
        event = FlowEvent(
            event_type="ledger",
            ledger_type=LedgerType.HOLD_KEEPSATS,
        )
        assert not stage.matches(event)

    def test_op_stage_matches_correct_event(self):
        stage = FlowStage(
            name="trigger_transfer",
            event_type="op",
            op_type="transfer",
        )
        event = FlowEvent(
            event_type="op",
            op_type="transfer",
        )
        assert stage.matches(event)

    def test_op_stage_rejects_wrong_op_type(self):
        stage = FlowStage(
            name="trigger_transfer",
            event_type="op",
            op_type="transfer",
        )
        event = FlowEvent(
            event_type="op",
            op_type="custom_json",
        )
        assert not stage.matches(event)

    def test_stage_rejects_event_type_mismatch(self):
        stage = FlowStage(
            name="customer_hive_in",
            event_type="ledger",
            ledger_type=LedgerType.CUSTOMER_HIVE_IN,
        )
        event = FlowEvent(
            event_type="op",
            op_type="transfer",
        )
        assert not stage.matches(event)


# ---------------------------------------------------------------------------
# Tests: FlowDefinition
# ---------------------------------------------------------------------------


class TestFlowDefinition:
    """Tests for the FlowDefinition model."""

    def test_hive_to_keepsats_definition_exists(self):
        assert HIVE_TO_KEEPSATS_FLOW.name == "hive_to_keepsats"
        assert HIVE_TO_KEEPSATS_FLOW.trigger_op_type == "transfer"

    def test_hive_to_keepsats_has_expected_stages(self):
        stage_names = HIVE_TO_KEEPSATS_FLOW.stage_names
        expected = [
            "trigger_transfer",
            "customer_hive_in",
            "hold_keepsats",
            "conv_hive_to_keepsats",
            "contra_hive_to_keepsats",
            "conv_customer",
            "release_keepsats",
            "fee_custom_json_op",
            "custom_json_fee",
            "fee_income",
            "keepsats_notification_op",
            "receive_lightning",
            "change_transfer_op",
            "customer_hive_out",
        ]
        assert stage_names == expected

    def test_required_stages_count(self):
        # All stages in the HIVE_TO_KEEPSATS flow are required by default
        assert len(HIVE_TO_KEEPSATS_FLOW.required_stages) == 14

    def test_custom_definition_with_optional_stage(self):
        defn = FlowDefinition(
            name="test_flow",
            trigger_op_type="transfer",
            stages=[
                FlowStage(name="required_stage", event_type="op", op_type="transfer"),
                FlowStage(
                    name="optional_stage",
                    event_type="ledger",
                    ledger_type=LedgerType.CUSTOMER_HIVE_OUT,
                    required=False,
                ),
            ],
        )
        assert len(defn.required_stages) == 1
        assert len(defn.stages) == 2


# ---------------------------------------------------------------------------
# Tests: FlowEvent
# ---------------------------------------------------------------------------


class TestFlowEvent:
    """Tests for FlowEvent creation."""

    def test_from_ledger_entry(self, primary_ledger_entries: dict[str, LedgerEntry]):
        le = primary_ledger_entries["cust_h_in"]
        event = FlowEvent.from_ledger_entry(le)
        assert event.event_type == "ledger"
        assert event.ledger_type == LedgerType.CUSTOMER_HIVE_IN
        assert event.short_id == "6921_b9c54f_1"
        assert event.group == "primary"

    def test_from_ledger_entry_with_group(self, fee_ledger_entries: dict[str, LedgerEntry]):
        le = fee_ledger_entries["c_j_fee"]
        event = FlowEvent.from_ledger_entry(le, group="fee_notification")
        assert event.group == "fee_notification"
        assert event.ledger_type == LedgerType.CUSTOM_JSON_FEE

    def test_from_op_dict(self, flow_data: dict):
        trigger = flow_data["trigger_transfer"]
        event = FlowEvent(
            event_type="op",
            timestamp=trigger["timestamp"],
            group_id=trigger["group_id"],
            short_id=trigger["short_id"],
            op_type=trigger["type"],
        )
        assert event.event_type == "op"
        assert event.op_type == "transfer"
        assert event.short_id == "6921_b9c54f_1"


# ---------------------------------------------------------------------------
# Tests: LedgerEntry deserialization from test data
# ---------------------------------------------------------------------------


class TestLedgerEntryDeserialization:
    """Verify test data can be deserialized into proper LedgerEntry objects."""

    def test_primary_entries_deserialize(self, primary_ledger_entries: dict[str, LedgerEntry]):
        assert len(primary_ledger_entries) == 6
        for key, le in primary_ledger_entries.items():
            assert isinstance(le, LedgerEntry)
            assert le.short_id == "6921_b9c54f_1"

    def test_primary_entry_types(self, primary_ledger_entries: dict[str, LedgerEntry]):
        expected_types = {
            "cust_h_in": LedgerType.CUSTOMER_HIVE_IN,
            "hold_k": LedgerType.HOLD_KEEPSATS,
            "h_conv_k": LedgerType.CONV_HIVE_TO_KEEPSATS,
            "h_contra_k": LedgerType.CONTRA_HIVE_TO_KEEPSATS,
            "cust_conv": LedgerType.CONV_CUSTOMER,
            "release_k": LedgerType.RELEASE_KEEPSATS,
        }
        for key, expected_ledger_type in expected_types.items():
            assert primary_ledger_entries[key].ledger_type == expected_ledger_type

    def test_fee_entries_deserialize(self, fee_ledger_entries: dict[str, LedgerEntry]):
        assert len(fee_ledger_entries) == 2
        assert fee_ledger_entries["c_j_fee"].ledger_type == LedgerType.CUSTOM_JSON_FEE
        assert fee_ledger_entries["fee_inc"].ledger_type == LedgerType.FEE_INCOME

    def test_change_entry_deserializes(self, change_ledger_entries: dict[str, LedgerEntry]):
        assert len(change_ledger_entries) == 1
        le = change_ledger_entries["cust_h_out"]
        assert le.ledger_type == LedgerType.CUSTOMER_HIVE_OUT
        assert le.short_id == "6926_94400f_1"


# ---------------------------------------------------------------------------
# Tests: FlowInstance - complete flow
# ---------------------------------------------------------------------------


class TestFlowInstanceComplete:
    """Tests for FlowInstance with a complete set of events."""

    def test_complete_flow_is_marked_complete(
        self,
        flow_instance: FlowInstance,
        all_flow_events: list[FlowEvent],
    ):
        for event in all_flow_events:
            flow_instance.add_event(event)
        assert flow_instance.is_complete
        assert flow_instance.status == FlowStatus.COMPLETED
        assert len(flow_instance.missing_stages) == 0

    def test_all_stages_matched(
        self,
        flow_instance: FlowInstance,
        all_flow_events: list[FlowEvent],
    ):
        for event in all_flow_events:
            flow_instance.add_event(event)
        assert flow_instance.matched_stage_names == set(HIVE_TO_KEEPSATS_FLOW.stage_names)

    def test_progress_shows_all_done(
        self,
        flow_instance: FlowInstance,
        all_flow_events: list[FlowEvent],
    ):
        for event in all_flow_events:
            flow_instance.add_event(event)
        assert flow_instance.progress == "14/14 required stages complete"

    def test_event_count(
        self,
        flow_instance: FlowInstance,
        all_flow_events: list[FlowEvent],
    ):
        for event in all_flow_events:
            flow_instance.add_event(event)
        # 1 trigger op + 6 primary ledger + 1 fee op + 2 fee ledger
        # + 1 keepsats op + 1 keepsats ledger + 1 change op + 1 change ledger = 14
        assert len(flow_instance.events) == 14

    def test_summary_dict(
        self,
        flow_instance: FlowInstance,
        all_flow_events: list[FlowEvent],
    ):
        for event in all_flow_events:
            flow_instance.add_event(event)
        summary = flow_instance.summary()
        assert summary["flow_type"] == "hive_to_keepsats"
        assert summary["status"] == "completed"
        assert summary["cust_id"] == "v4vapp-test"
        assert summary["trigger_short_id"] == "6921_b9c54f_1"
        assert len(summary["missing_stages"]) == 0
        assert summary["event_count"] == 14

    def test_customer_id_preserved(
        self,
        flow_instance: FlowInstance,
    ):
        assert flow_instance.cust_id == "v4vapp-test"


# ---------------------------------------------------------------------------
# Tests: FlowInstance - incomplete flow
# ---------------------------------------------------------------------------


class TestFlowInstanceIncomplete:
    """Tests for FlowInstance with missing events."""

    def test_empty_instance_not_complete(
        self,
        flow_instance: FlowInstance,
    ):
        assert not flow_instance.is_complete
        assert flow_instance.status == FlowStatus.PENDING
        assert len(flow_instance.missing_stages) == 14

    def test_partial_primary_events_not_complete(
        self,
        flow_instance: FlowInstance,
        primary_ledger_entries: dict[str, LedgerEntry],
        flow_data: dict,
    ):
        # Add trigger op and a few primary ledger entries
        trigger = flow_data["trigger_transfer"]
        flow_instance.add_event(
            FlowEvent(
                event_type="op",
                timestamp=trigger["timestamp"],
                group_id=trigger["group_id"],
                short_id=trigger["short_id"],
                op_type=trigger["type"],
                ledger_entry=None,
                op=None,
                ledger_type=None,
            )
        )
        flow_instance.add_event(FlowEvent.from_ledger_entry(primary_ledger_entries["cust_h_in"]))
        flow_instance.add_event(FlowEvent.from_ledger_entry(primary_ledger_entries["hold_k"]))

        assert not flow_instance.is_complete
        assert flow_instance.status == FlowStatus.IN_PROGRESS
        # 14 total - 3 added (trigger_transfer + customer_hive_in + hold_keepsats) = 11 missing
        assert len(flow_instance.missing_stages) == 11
        assert flow_instance.progress == "3/14 required stages complete"

    def test_missing_change_return_not_complete(
        self,
        flow_instance: FlowInstance,
        all_flow_events: list[FlowEvent],
    ):
        # Add all events EXCEPT the change return ones (last 2)
        # Events: trigger + 6 primary + fee_op + 2 fee_ledger + ks_op + ks_ledger = 12
        # Missing: change_transfer_op + customer_hive_out = 2
        for event in all_flow_events[:-2]:
            flow_instance.add_event(event)

        assert not flow_instance.is_complete
        missing_names = [s.name for s in flow_instance.missing_stages]
        assert "change_transfer_op" in missing_names
        assert "customer_hive_out" in missing_names

    def test_missing_fee_events_not_complete(
        self,
        flow_instance: FlowInstance,
        flow_data: dict,
        primary_ledger_entries: dict[str, LedgerEntry],
        keepsats_notification_ledger_entries: dict[str, LedgerEntry],
        change_ledger_entries: dict[str, LedgerEntry],
    ):
        # Add everything except fee-related events.
        # With group-agnostic op matching the keepsats custom_json op will
        # match the first unmatched custom_json stage (fee_custom_json_op)
        # because stages are tried in definition order.  That means
        # keepsats_notification_op ends up unmatched instead.
        trigger = flow_data["trigger_transfer"]
        flow_instance.add_event(
            FlowEvent(
                event_type="op",
                timestamp=trigger["timestamp"],
                group_id=trigger["group_id"],
                short_id=trigger["short_id"],
                op_type=trigger["type"],
                ledger_entry=None,
                op=None,
                ledger_type=None,
            )
        )
        for le in primary_ledger_entries.values():
            flow_instance.add_event(FlowEvent.from_ledger_entry(le))

        # Add keepsats notification (no explicit group — simulates db_monitor)
        ks_op = flow_data["keepsats_notification_op"]
        flow_instance.add_event(
            FlowEvent(
                event_type="op",
                timestamp=ks_op["timestamp"],
                group_id=ks_op["group_id"],
                short_id=ks_op["short_id"],
                op_type=ks_op["type"],
                ledger_entry=None,
                op=None,
                ledger_type=None,
            )
        )
        for le in keepsats_notification_ledger_entries.values():
            flow_instance.add_event(FlowEvent.from_ledger_entry(le))

        # Add change return (no explicit group — simulates db_monitor)
        change_op = flow_data["change_transfer"]
        flow_instance.add_event(
            FlowEvent(
                event_type="op",
                timestamp=change_op["timestamp"],
                group_id=change_op["group_id"],
                short_id=change_op["short_id"],
                op_type=change_op["type"],
                ledger_entry=None,
                op=None,
                ledger_type=None,
            )
        )
        for le in change_ledger_entries.values():
            flow_instance.add_event(FlowEvent.from_ledger_entry(le))

        assert not flow_instance.is_complete
        missing_names = [s.name for s in flow_instance.missing_stages]
        # Fee ledger stages are always missing (unique ledger_types)
        assert "custom_json_fee" in missing_names
        assert "fee_income" in missing_names
        # With group-agnostic matching the keepsats custom_json consumed
        # fee_custom_json_op, so keepsats_notification_op is the missing op stage
        assert "keepsats_notification_op" in missing_names
        assert len(missing_names) == 3


# ---------------------------------------------------------------------------
# Tests: FlowInstance - stage matching details
# ---------------------------------------------------------------------------


class TestFlowInstanceMatching:
    """Tests for the event-to-stage matching logic."""

    def test_add_event_returns_matched_stage_name(
        self,
        flow_instance: FlowInstance,
        primary_ledger_entries: dict[str, LedgerEntry],
    ):
        le = primary_ledger_entries["cust_h_in"]
        result = flow_instance.add_event(FlowEvent.from_ledger_entry(le))
        assert result == "customer_hive_in"

    def test_add_event_returns_none_for_duplicate(
        self,
        flow_instance: FlowInstance,
        primary_ledger_entries: dict[str, LedgerEntry],
    ):
        le = primary_ledger_entries["cust_h_in"]
        flow_instance.add_event(FlowEvent.from_ledger_entry(le))
        # Adding same type again should still match (stage already fulfilled)
        # but since the stage is already matched, it returns None
        result = flow_instance.add_event(FlowEvent.from_ledger_entry(le))
        assert result is None

    def test_add_event_returns_none_for_unmatched(
        self,
        flow_instance: FlowInstance,
    ):
        # An event type not in the flow definition
        event = FlowEvent(
            event_type="ledger",
            ledger_type=LedgerType.OPENING_BALANCE,
        )
        result = flow_instance.add_event(event)
        assert result is None

    def test_status_transitions(
        self,
        flow_instance: FlowInstance,
        all_flow_events: list[FlowEvent],
    ):
        assert flow_instance.status == FlowStatus.PENDING

        flow_instance.add_event(all_flow_events[0])
        assert flow_instance.status == FlowStatus.IN_PROGRESS

        for event in all_flow_events[1:]:
            flow_instance.add_event(event)
        assert flow_instance.status == FlowStatus.COMPLETED


# ---------------------------------------------------------------------------
# Tests: Overwatch singleton
# ---------------------------------------------------------------------------


class TestOverwatch:
    """Tests for the Overwatch singleton registry."""

    def test_add_and_retrieve_flow_instance(self, flow_instance: FlowInstance):
        Overwatch.reset()
        ow = Overwatch()
        ow.flow_instances.append(flow_instance)
        assert len(ow.active_flows) == 1
        assert len(ow.completed_flows) == 0

    def test_completed_flow_moves_to_completed(
        self,
        flow_instance: FlowInstance,
        all_flow_events: list[FlowEvent],
    ):
        Overwatch.reset()
        for event in all_flow_events:
            flow_instance.add_event(event)
        ow = Overwatch()
        ow.flow_instances.append(flow_instance)
        assert len(ow.active_flows) == 0
        assert len(ow.completed_flows) == 1

    def test_reset_clears_everything(self, flow_instance: FlowInstance):
        Overwatch.reset()
        ow = Overwatch()
        ow.flow_instances.append(flow_instance)
        Overwatch.reset()
        ow2 = Overwatch()
        assert len(ow2.flow_instances) == 0

    def test_singleton_returns_same_instance(self):
        Overwatch.reset()
        a = Overwatch()
        b = Overwatch()
        assert a is b

    def test_register_and_retrieve_flow_definition(self):
        Overwatch.reset()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        flows = Overwatch.registered_flows()
        assert "hive_to_keepsats" in flows
        assert flows["hive_to_keepsats"] is HIVE_TO_KEEPSATS_FLOW

    async def test_check_stalls_marks_old_flows(
        self,
        flow_instance: FlowInstance,
        primary_ledger_entries: dict[str, LedgerEntry],
    ):
        Overwatch.reset()
        ow = Overwatch()
        # Add one event so the flow is IN_PROGRESS
        flow_instance.add_event(FlowEvent.from_ledger_entry(primary_ledger_entries["cust_h_in"]))
        ow.flow_instances.append(flow_instance)
        # Simulate time passing
        far_future = flow_instance.started_at + Overwatch.stall_timeout + timedelta(seconds=1)
        stalled = await ow.check_stalls(now=far_future)
        assert len(stalled) == 1
        assert flow_instance.status == FlowStatus.STALLED

    async def test_load_upgrades_stalled_flow_to_completed(self):
        """A flow persisted under an older definition should complete when
        the registered definition later makes some stages optional.
        """
        from v4vapp_backend_v2.process.overwatch_flows import KEEPSATS_TO_HBD_FLOW

        # start with an "old" copy where every stage is required
        old_def = FlowDefinition(**KEEPSATS_TO_HBD_FLOW.model_dump())
        for s in old_def.stages:
            s.required = True

        Overwatch.reset()
        ow = Overwatch()
        # ensure Redis is empty before we persist our test instance
        await ow.reset_redis()
        Overwatch.register_flow(old_def)

        # create a stalled instance that was already 'complete' under the
        # old definition by giving it a dummy event for each stage.
        flow = FlowInstance(
            flow_definition=old_def,
            trigger_group_id="old123",
            trigger_short_id="short456",
            cust_id="cust",
            status=FlowStatus.STALLED,
        )
        # add one event per stage so the instance satisfies the old definition
        from datetime import datetime, timezone
        for stage in old_def.stages:
            if stage.event_type == "op":
                evt = FlowEvent(
                    event_type="op",
                    timestamp=datetime.now(timezone.utc),
                    group_id="old123",
                    short_id="short456",
                    op_type=stage.op_type or "",
                )
            else:
                evt = FlowEvent(
                    event_type="ledger",
                    timestamp=datetime.now(timezone.utc),
                    group_id="old123",
                    short_id="short456",
                    ledger_type=stage.ledger_type,
                )
            flow.events.append(evt)
        ow.flow_instances.append(flow)
        await ow._persist_flow(flow)

        # reset and register the updated definition
        Overwatch.reset()
        ow2 = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_HBD_FLOW)

        loaded = await ow2.load_from_redis()
        # we expect at least one flow to be loaded and that one should
        # have been upgraded to COMPLETED; there may be other leftover
        # entries from earlier test runs.
        assert loaded >= 1
        assert any(f.status == FlowStatus.COMPLETED for f in ow2.flow_instances)

    def test_dedup_prevents_rematched_trigger(
        self,
        flow_instance: FlowInstance,
        flow_data: dict,
    ):
        """A re-arrived trigger (MongoDB update event) must NOT match
        change_transfer_op — dedup should skip it."""
        trigger = flow_data["trigger_transfer"]
        trigger_event = FlowEvent(
            event_type="op",
            timestamp=trigger["timestamp"],
            group_id=trigger["group_id"],
            short_id=trigger["short_id"],
            op_type=trigger["type"],
            ledger_entry=None,
            op=None,
            ledger_type=None,
        )
        # First insertion matches trigger_transfer
        result1 = flow_instance.add_event(trigger_event)
        assert result1 == "trigger_transfer"

        # Simulate the same trigger arriving again (e.g. with replies added)
        assert Overwatch._is_duplicate(flow_instance, trigger_event)

    async def test_dispatch_with_default_groups_completes_flow(
        self,
        flow_data: dict,
        all_flow_events: list[FlowEvent],
    ):
        """Simulate dbmonitor dispatching all events with group='primary'.

        The flow should still complete even though op stages have specific
        groups in the definition — matching is group-agnostic.
        """
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch._loaded_from_redis = True  # skip Redis

        trigger = flow_data["trigger_transfer"]
        # Simulate: ingest trigger op → creates flow
        trigger_event = all_flow_events[0]
        matched = await ow._try_create_flow(
            trigger_event,
            type(
                "FakeOp",
                (),
                {
                    "group_id": trigger["group_id"],
                    "short_id": trigger["short_id"],
                    "op_type": trigger["type"],
                    "from_account": trigger.get("from", ""),
                },
            )(),
        )
        assert matched == "trigger_transfer"
        assert len(ow.active_flows) == 1

        # Dispatch remaining events
        for event in all_flow_events[1:]:
            await ow._dispatch(event)

        assert len(ow.completed_flows) == 1
        assert ow.completed_flows[0].is_complete


# ---------------------------------------------------------------------------
# Tests: Flow data integrity checks
# ---------------------------------------------------------------------------


class TestFlowDataIntegrity:
    """Verify the extracted test data has expected properties."""

    def test_trigger_is_incoming_transfer(self, flow_data: dict):
        trigger = flow_data["trigger_transfer"]
        assert trigger["type"] == "transfer"
        assert trigger["from"] == "v4vapp-test"
        assert trigger["to"] == "devser.v4vapp"
        assert trigger["amount"]["amount"] == "10000"  # 10.000 HIVE
        assert "Deposit to #SATS" in trigger["memo"]

    def test_trigger_has_replies(self, flow_data: dict):
        replies = flow_data["trigger_transfer"]["replies"]
        assert len(replies) == 3
        reply_types = {r["reply_type"] for r in replies}
        assert "transfer" in reply_types  # change return
        assert "custom_json" in reply_types  # fee + keepsats notifications

    def test_fee_custom_json_has_parent_id(self, flow_data: dict):
        fee_op = flow_data["fee_notification_op"]
        parent_id = fee_op["json"]["parent_id"]
        assert parent_id == flow_data["trigger_transfer"]["group_id"]

    def test_keepsats_notification_has_parent_id(self, flow_data: dict):
        ks_op = flow_data["keepsats_notification_op"]
        parent_id = ks_op["json"]["parent_id"]
        assert parent_id == flow_data["trigger_transfer"]["group_id"]

    def test_change_transfer_references_trigger(self, flow_data: dict):
        change = flow_data["change_transfer"]
        trigger_short_id = flow_data["trigger_transfer"]["short_id"]
        # Change memo contains the § reference to the trigger short_id
        assert f"§ {trigger_short_id}" in change["memo"]

    def test_all_primary_entries_share_short_id(self, flow_data: dict):
        trigger_short_id = flow_data["trigger_transfer"]["short_id"]
        for key, entry in flow_data["primary_ledger_entries"].items():
            assert entry["short_id"] == trigger_short_id, (
                f"Primary ledger entry '{key}' has short_id '{entry['short_id']}' "
                f"but expected '{trigger_short_id}'"
            )

    def test_conversion_amounts_consistent(self, flow_data: dict):
        trigger = flow_data["trigger_transfer"]
        # 10 HIVE at ~98.7 sats/hive = ~987 sats
        assert trigger["conv"]["sats"] == pytest.approx(987.02246, rel=1e-3)
        # Fee is ~69 sats (7%)
        assert trigger["conv"]["msats_fee"] == 68753.0
        # Change is 0.001 HIVE
        assert trigger["change_amount"]["amount"] == "1"  # 0.001 HIVE
