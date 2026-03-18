"""
Tests for the Keepsats-to-External Lightning Overwatch flow.

Uses real flow data extracted from db_monitor logs for a Keepsats-to-External
Lightning payment (1,234 sats paid to WalletOfSatoshi.com via LND).
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.process.overwatch_flows import (
    KEEPSATS_TO_EXTERNAL_FLOW,
    KEEPSATS_TO_HBD_FLOW,
)
from v4vapp_backend_v2.process.process_overwatch import (
    FlowEvent,
    FlowInstance,
    FlowStatus,
    Overwatch,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

KE_FLOW_DATA_PATH = Path("tests/data/overwatch/keepsats_to_external_flow.json")


@pytest.fixture
def ke_flow_data() -> dict:
    """Load the Keepsats-to-External flow test data."""
    with open(KE_FLOW_DATA_PATH) as f:
        return json.load(f)


@pytest.fixture
def ke_primary_ledger_entries(ke_flow_data: dict) -> dict[str, LedgerEntry]:
    """Parse primary ledger entries (hold_k, release_k) from the test data."""
    entries = {}
    for key, data in ke_flow_data["primary_ledger_entries"].items():
        entries[key] = LedgerEntry(**data)
    return entries


@pytest.fixture
def ke_payment_ledger_entries(ke_flow_data: dict) -> dict[str, LedgerEntry]:
    """Parse payment ledger entries (withdraw_l, fee_exp) from the test data."""
    entries = {}
    for key, data in ke_flow_data["payment_ledger_entries"].items():
        entries[key] = LedgerEntry(**data)
    return entries


@pytest.fixture
def ke_all_flow_events(
    ke_flow_data: dict,
    ke_primary_ledger_entries: dict[str, LedgerEntry],
    ke_payment_ledger_entries: dict[str, LedgerEntry],
) -> list[FlowEvent]:
    """Build a list of all FlowEvents for the complete Keepsats-to-External flow.

    All events use the default ``group="primary"`` to simulate what
    ``db_monitor`` actually sends — the Overwatch system must be able to
    match stages without the caller specifying the correct group.
    """
    events: list[FlowEvent] = []

    # 1. Trigger custom_json op
    trigger = ke_flow_data["trigger_custom_json"]
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

    # 2. Primary ledger entries (hold_k, release_k - same short_id as trigger)
    for le in ke_primary_ledger_entries.values():
        events.append(FlowEvent.from_ledger_entry(le, group="primary"))

    # 3. Payment op (different short_id - payment hash)
    payment = ke_flow_data["payment_op"]
    events.append(
        FlowEvent(
            event_type="op",
            timestamp=payment["timestamp"],
            group_id=payment["group_id"],
            short_id=payment["short_id"],
            op_type=payment["type"],
            group="primary",
            ledger_entry=None,
            op=None,
            ledger_type=None,
        )
    )

    # 4. Payment ledger entries (withdraw_l, fee_exp)
    for le in ke_payment_ledger_entries.values():
        events.append(FlowEvent.from_ledger_entry(le, group="primary"))

    # 5. Notification custom_json op (different short_id - reply group)
    notif = ke_flow_data["notification_op"]
    events.append(
        FlowEvent(
            event_type="op",
            timestamp=notif["timestamp"],
            group_id=notif["group_id"],
            short_id=notif["short_id"],
            op_type=notif["type"],
            group="primary",
            ledger_entry=None,
            op=None,
            ledger_type=None,
        )
    )

    return events


@pytest.fixture
def ke_flow_instance(ke_flow_data: dict) -> FlowInstance:
    """Create an empty FlowInstance for the Keepsats-to-External flow."""
    trigger = ke_flow_data["trigger_custom_json"]
    return FlowInstance(
        flow_definition=KEEPSATS_TO_EXTERNAL_FLOW,
        trigger_group_id=trigger["group_id"],
        trigger_short_id=trigger["short_id"],
        cust_id=trigger["cust_id"],
    )


# ---------------------------------------------------------------------------
# Tests: FlowDefinition
# ---------------------------------------------------------------------------


class TestKeepsatsToExternalDefinition:
    """Tests for the KEEPSATS_TO_EXTERNAL_FLOW definition."""

    def test_definition_exists(self):
        assert KEEPSATS_TO_EXTERNAL_FLOW.name == "keepsats_to_external"
        assert KEEPSATS_TO_EXTERNAL_FLOW.trigger_op_type == "custom_json"

    def test_has_expected_stage_names(self):
        expected = [
            "trigger_custom_json",
            "hold_keepsats",
            "release_keepsats",
            "payment_op",
            "withdraw_lightning",
            "fee_expense",
            "notification_custom_json_op",
        ]
        assert KEEPSATS_TO_EXTERNAL_FLOW.stage_names == expected

    def test_required_stages_count(self):
        assert len(KEEPSATS_TO_EXTERNAL_FLOW.required_stages) == 6

    def test_optional_stages_listed(self):
        optional = [s.name for s in KEEPSATS_TO_EXTERNAL_FLOW.stages if not s.required]
        assert optional == ["notification_custom_json_op"]

    def test_stage_count(self):
        assert len(KEEPSATS_TO_EXTERNAL_FLOW.stages) == 7

    def test_groups_are_correct(self):
        groups = {s.group for s in KEEPSATS_TO_EXTERNAL_FLOW.stages}
        expected_groups = {"primary", "payment", "notification"}
        assert groups == expected_groups

    def test_primary_group_has_3_stages(self):
        primary_stages = [s for s in KEEPSATS_TO_EXTERNAL_FLOW.stages if s.group == "primary"]
        assert len(primary_stages) == 3

    def test_payment_group_has_3_stages(self):
        payment_stages = [s for s in KEEPSATS_TO_EXTERNAL_FLOW.stages if s.group == "payment"]
        assert len(payment_stages) == 3

    def test_ledger_types_in_definition(self):
        ledger_stages = [s for s in KEEPSATS_TO_EXTERNAL_FLOW.stages if s.event_type == "ledger"]
        expected_types = {
            LedgerType.HOLD_KEEPSATS,
            LedgerType.RELEASE_KEEPSATS,
            LedgerType.WITHDRAW_LIGHTNING,
            LedgerType.FEE_EXPENSE,
        }
        actual_types = {s.ledger_type for s in ledger_stages}
        assert actual_types == expected_types


# ---------------------------------------------------------------------------
# Tests: LedgerEntry deserialization from test data
# ---------------------------------------------------------------------------


class TestKeepsatsToExternalDeserialization:
    """Verify the extracted test data can be deserialized into LedgerEntry objects."""

    def test_primary_entries_deserialize(self, ke_primary_ledger_entries: dict[str, LedgerEntry]):
        assert len(ke_primary_ledger_entries) == 2
        for key, le in ke_primary_ledger_entries.items():
            assert isinstance(le, LedgerEntry)
            assert le.short_id == "6454_ba3351_1"

    def test_primary_entry_types(self, ke_primary_ledger_entries: dict[str, LedgerEntry]):
        expected_types = {
            "hold_k": LedgerType.HOLD_KEEPSATS,
            "release_k": LedgerType.RELEASE_KEEPSATS,
        }
        for key, expected_ledger_type in expected_types.items():
            assert ke_primary_ledger_entries[key].ledger_type == expected_ledger_type

    def test_payment_entries_deserialize(self, ke_payment_ledger_entries: dict[str, LedgerEntry]):
        assert len(ke_payment_ledger_entries) == 2
        for key, le in ke_payment_ledger_entries.items():
            assert isinstance(le, LedgerEntry)
            assert le.short_id == "cb437f7d1a"

    def test_payment_entry_types(self, ke_payment_ledger_entries: dict[str, LedgerEntry]):
        expected_types = {
            "withdraw_l": LedgerType.WITHDRAW_LIGHTNING,
            "fee_exp": LedgerType.FEE_EXPENSE,
        }
        for key, expected_ledger_type in expected_types.items():
            assert ke_payment_ledger_entries[key].ledger_type == expected_ledger_type


# ---------------------------------------------------------------------------
# Tests: FlowInstance - complete flow
# ---------------------------------------------------------------------------


class TestKeepsatsToExternalComplete:
    """Tests for FlowInstance with a complete set of Keepsats-to-External events."""

    def test_complete_flow_is_marked_complete(
        self,
        ke_flow_instance: FlowInstance,
        ke_all_flow_events: list[FlowEvent],
    ):
        for event in ke_all_flow_events:
            ke_flow_instance.add_event(event)
        assert ke_flow_instance.is_complete
        assert ke_flow_instance.status == FlowStatus.COMPLETED
        assert len(ke_flow_instance.missing_stages) == 0

    def test_all_stages_matched(
        self,
        ke_flow_instance: FlowInstance,
        ke_all_flow_events: list[FlowEvent],
    ):
        for event in ke_all_flow_events:
            ke_flow_instance.add_event(event)
        assert ke_flow_instance.matched_stage_names == set(KEEPSATS_TO_EXTERNAL_FLOW.stage_names)

    def test_progress_shows_all_done(
        self,
        ke_flow_instance: FlowInstance,
        ke_all_flow_events: list[FlowEvent],
    ):
        for event in ke_all_flow_events:
            ke_flow_instance.add_event(event)
        assert ke_flow_instance.progress == "6/6 required stages complete"

    def test_event_count(
        self,
        ke_flow_instance: FlowInstance,
        ke_all_flow_events: list[FlowEvent],
    ):
        for event in ke_all_flow_events:
            ke_flow_instance.add_event(event)
        # 1 trigger + 2 primary ledger + 1 payment op + 2 payment ledger
        # + 1 notification op = 7
        assert len(ke_flow_instance.events) == 7

    def test_summary_dict(
        self,
        ke_flow_instance: FlowInstance,
        ke_all_flow_events: list[FlowEvent],
    ):
        for event in ke_all_flow_events:
            ke_flow_instance.add_event(event)
        summary = ke_flow_instance.summary()
        assert summary["flow_type"] == "keepsats_to_external"
        assert summary["status"] == "completed"
        assert summary["cust_id"] == "v4vapp-test"
        assert summary["trigger_short_id"] == "6454_ba3351_1"
        assert len(summary["missing_stages"]) == 0
        assert summary["event_count"] == 7

    def test_customer_id_preserved(
        self,
        ke_flow_instance: FlowInstance,
    ):
        assert ke_flow_instance.cust_id == "v4vapp-test"


# ---------------------------------------------------------------------------
# Tests: FlowInstance - incomplete flow
# ---------------------------------------------------------------------------


class TestKeepsatsToExternalIncomplete:
    """Tests for FlowInstance with missing events."""

    def test_empty_instance_not_complete(
        self,
        ke_flow_instance: FlowInstance,
    ):
        assert not ke_flow_instance.is_complete
        assert ke_flow_instance.status == FlowStatus.PENDING
        assert len(ke_flow_instance.missing_stages) == 6

    def test_trigger_only_not_complete(
        self,
        ke_flow_instance: FlowInstance,
        ke_flow_data: dict,
    ):
        trigger = ke_flow_data["trigger_custom_json"]
        ke_flow_instance.add_event(
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
        assert not ke_flow_instance.is_complete
        assert ke_flow_instance.status == FlowStatus.IN_PROGRESS
        assert len(ke_flow_instance.missing_stages) == 5
        assert ke_flow_instance.progress == "1/6 required stages complete"

    def test_primary_events_only_not_complete(
        self,
        ke_flow_instance: FlowInstance,
        ke_flow_data: dict,
        ke_primary_ledger_entries: dict[str, LedgerEntry],
    ):
        # Add trigger + primary ledger entries, skip payment and notification
        trigger = ke_flow_data["trigger_custom_json"]
        ke_flow_instance.add_event(
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
        for le in ke_primary_ledger_entries.values():
            ke_flow_instance.add_event(FlowEvent.from_ledger_entry(le))

        assert not ke_flow_instance.is_complete
        assert ke_flow_instance.status == FlowStatus.IN_PROGRESS
        # 3 matched (trigger + hold_k + release_k), 3 remaining (payment_op, withdraw_l, fee_exp)
        assert len(ke_flow_instance.missing_stages) == 3
        assert ke_flow_instance.progress == "3/6 required stages complete"

    def test_without_notification_still_complete(
        self,
        ke_flow_instance: FlowInstance,
        ke_all_flow_events: list[FlowEvent],
    ):
        # Add all events EXCEPT the notification (last one).
        # Notification is optional, so flow should still be complete.
        for event in ke_all_flow_events[:-1]:
            ke_flow_instance.add_event(event)

        assert ke_flow_instance.is_complete
        missing_names = [s.name for s in ke_flow_instance.missing_stages]
        assert "notification_custom_json_op" not in missing_names


# ---------------------------------------------------------------------------
# Tests: FlowInstance - stage matching details
# ---------------------------------------------------------------------------


class TestKeepsatsToExternalMatching:
    """Tests for the event-to-stage matching logic with Keepsats-to-External data."""

    def test_add_event_returns_matched_stage_name(
        self,
        ke_flow_instance: FlowInstance,
        ke_primary_ledger_entries: dict[str, LedgerEntry],
    ):
        le = ke_primary_ledger_entries["hold_k"]
        result = ke_flow_instance.add_event(FlowEvent.from_ledger_entry(le))
        assert result == "hold_keepsats"

    def test_add_event_returns_none_for_duplicate(
        self,
        ke_flow_instance: FlowInstance,
        ke_primary_ledger_entries: dict[str, LedgerEntry],
    ):
        le = ke_primary_ledger_entries["hold_k"]
        ke_flow_instance.add_event(FlowEvent.from_ledger_entry(le))
        result = ke_flow_instance.add_event(FlowEvent.from_ledger_entry(le))
        assert result is None

    def test_add_event_returns_none_for_unmatched(
        self,
        ke_flow_instance: FlowInstance,
    ):
        event = FlowEvent(
            event_type="ledger",
            ledger_type=LedgerType.OPENING_BALANCE,
        )
        result = ke_flow_instance.add_event(event)
        assert result is None

    def test_payment_ledger_matches_correct_stage(
        self,
        ke_flow_instance: FlowInstance,
        ke_payment_ledger_entries: dict[str, LedgerEntry],
    ):
        le = ke_payment_ledger_entries["withdraw_l"]
        result = ke_flow_instance.add_event(FlowEvent.from_ledger_entry(le))
        assert result == "withdraw_lightning"

    def test_fee_expense_matches_correct_stage(
        self,
        ke_flow_instance: FlowInstance,
        ke_payment_ledger_entries: dict[str, LedgerEntry],
    ):
        le = ke_payment_ledger_entries["fee_exp"]
        result = ke_flow_instance.add_event(FlowEvent.from_ledger_entry(le))
        assert result == "fee_expense"


# ---------------------------------------------------------------------------
# Tests: Multi-candidate disambiguation
# ---------------------------------------------------------------------------


class TestMultiCandidateDisambiguation:
    """Verify that _try_create_flow creates candidates for all matching
    definitions and that _resolve_candidates removes losers when the
    correct flow completes."""

    @pytest.mark.asyncio
    async def test_try_create_creates_two_candidates(self, ke_flow_data: dict):
        """custom_json trigger should create both keepsats_to_external and
        keepsats_to_hbd as candidates."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_HBD_FLOW)
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = ke_flow_data["trigger_custom_json"]
        event = FlowEvent(
            event_type="op",
            timestamp=trigger["timestamp"],
            group_id=trigger["group_id"],
            short_id=trigger["short_id"],
            op_type=trigger["type"],
            group="primary",
        )
        fake_op = type(
            "FakeOp",
            (),
            {
                "group_id": trigger["group_id"],
                "short_id": trigger["short_id"],
                "op_type": trigger["type"],
                "from_account": trigger.get("cust_id", ""),
            },
        )()

        result = await ow._try_create_flow(event, fake_op)
        assert result == "trigger_custom_json"
        assert len(ow.active_flows) == 2
        flow_names = {f.flow_definition.name for f in ow.active_flows}
        assert flow_names == {"keepsats_to_hbd", "keepsats_to_external"}

    @pytest.mark.asyncio
    async def test_external_events_complete_external_and_remove_hbd(
        self,
        ke_flow_data: dict,
        ke_all_flow_events: list[FlowEvent],
    ):
        """Feeding keepsats_to_external events should complete that flow
        and resolve the keepsats_to_hbd candidate."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_HBD_FLOW)
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        # Create candidates from trigger
        trigger = ke_flow_data["trigger_custom_json"]
        trigger_event = ke_all_flow_events[0]
        fake_op = type(
            "FakeOp",
            (),
            {
                "group_id": trigger["group_id"],
                "short_id": trigger["short_id"],
                "op_type": trigger["type"],
                "from_account": trigger.get("cust_id", ""),
            },
        )()
        await ow._try_create_flow(trigger_event, fake_op)
        assert len(ow.active_flows) == 2

        # Dispatch remaining events (external flow events)
        for event in ke_all_flow_events[1:]:
            await ow._dispatch(event)

        # The external flow should complete and the HBD candidate should be removed
        assert len(ow.completed_flows) == 1
        assert ow.completed_flows[0].flow_definition.name == "keepsats_to_external"
        # HBD candidate was removed by _resolve_candidates
        hbd_flows = [f for f in ow.flow_instances if f.flow_definition.name == "keepsats_to_hbd"]
        assert len(hbd_flows) == 0

    @pytest.mark.asyncio
    async def test_dispatch_sends_events_to_all_candidates(
        self,
        ke_flow_data: dict,
        ke_primary_ledger_entries: dict[str, LedgerEntry],
    ):
        """Events should be dispatched to all active candidates, not just the first."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_HBD_FLOW)
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        # Create candidates
        trigger = ke_flow_data["trigger_custom_json"]
        trigger_event = FlowEvent(
            event_type="op",
            timestamp=trigger["timestamp"],
            group_id=trigger["group_id"],
            short_id=trigger["short_id"],
            op_type=trigger["type"],
            group="primary",
        )
        fake_op = type(
            "FakeOp",
            (),
            {
                "group_id": trigger["group_id"],
                "short_id": trigger["short_id"],
                "op_type": trigger["type"],
                "from_account": trigger.get("cust_id", ""),
            },
        )()
        await ow._try_create_flow(trigger_event, fake_op)

        # Dispatch hold_keepsats — both flows have this stage
        hold_le = ke_primary_ledger_entries["hold_k"]
        await ow._dispatch(FlowEvent.from_ledger_entry(hold_le, group="primary"))

        # Both candidates should now have matched the hold_keepsats stage
        for flow in ow.active_flows:
            if "hold_keepsats" in [s.name for s in flow.flow_definition.stages]:
                assert "hold_keepsats" in flow.matched_stage_names

    @pytest.mark.asyncio
    async def test_single_matching_definition_no_extra_candidate(self, ke_flow_data: dict):
        """When only one definition matches the trigger, no extra candidate is created."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)  # only one
        Overwatch._loaded_from_redis = True

        trigger = ke_flow_data["trigger_custom_json"]
        event = FlowEvent(
            event_type="op",
            timestamp=trigger["timestamp"],
            group_id=trigger["group_id"],
            short_id=trigger["short_id"],
            op_type=trigger["type"],
            group="primary",
        )
        fake_op = type(
            "FakeOp",
            (),
            {
                "group_id": trigger["group_id"],
                "short_id": trigger["short_id"],
                "op_type": trigger["type"],
                "from_account": trigger.get("cust_id", ""),
            },
        )()

        await ow._try_create_flow(event, fake_op)
        assert len(ow.active_flows) == 1
        assert ow.active_flows[0].flow_definition.name == "keepsats_to_external"

    @pytest.mark.asyncio
    async def test_resolve_candidates_removes_failed_from_instances(self, ke_flow_data: dict):
        """_resolve_candidates should remove losers from flow_instances."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_HBD_FLOW)
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = ke_flow_data["trigger_custom_json"]
        trigger_event = FlowEvent(
            event_type="op",
            timestamp=trigger["timestamp"],
            group_id=trigger["group_id"],
            short_id=trigger["short_id"],
            op_type=trigger["type"],
            group="primary",
        )
        fake_op = type(
            "FakeOp",
            (),
            {
                "group_id": trigger["group_id"],
                "short_id": trigger["short_id"],
                "op_type": trigger["type"],
                "from_account": trigger.get("cust_id", ""),
            },
        )()
        await ow._try_create_flow(trigger_event, fake_op)
        assert len(ow.flow_instances) == 2

        # Manually complete one candidate
        ext_flow = next(
            f for f in ow.flow_instances if f.flow_definition.name == "keepsats_to_external"
        )
        ext_flow.status = FlowStatus.COMPLETED
        await ow._resolve_candidates(ext_flow)

        # Only the winner should remain
        assert len(ow.flow_instances) == 1
        assert ow.flow_instances[0].flow_definition.name == "keepsats_to_external"

    @pytest.mark.asyncio
    async def test_late_notification_absorbed_by_completed_flow(
        self,
        ke_flow_data: dict,
        ke_all_flow_events: list[FlowEvent],
    ):
        """A notification custom_json arriving after the flow completes should
        be absorbed by the completed flow — not spawn new candidates."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_HBD_FLOW)
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        # Create candidates from trigger
        trigger = ke_flow_data["trigger_custom_json"]
        trigger_event = ke_all_flow_events[0]
        fake_op = type(
            "FakeOp",
            (),
            {
                "group_id": trigger["group_id"],
                "short_id": trigger["short_id"],
                "op_type": trigger["type"],
                "from_account": trigger.get("cust_id", ""),
            },
        )()
        await ow._try_create_flow(trigger_event, fake_op)

        # Dispatch all events EXCEPT the notification (last one) — completes the flow
        for event in ke_all_flow_events[1:-1]:
            await ow._dispatch(event)

        assert len(ow.completed_flows) == 1
        assert ow.completed_flows[0].flow_definition.name == "keepsats_to_external"
        assert len(ow.active_flows) == 0  # HBD candidate was resolved away

        # Simulate recent completion so the late-event time window applies
        ow.completed_flows[0].completed_at = datetime.now(tz=timezone.utc)

        # Now the late notification arrives
        notification_event = ke_all_flow_events[-1]
        result = await ow._dispatch(notification_event)

        # Should be absorbed by the completed flow, not return None
        assert result == "notification_custom_json_op"
        # No new active flows should have been created
        assert len(ow.active_flows) == 0
        # The completed flow absorbed the notification
        completed = ow.completed_flows[0]
        assert "notification_custom_json_op" in completed.matched_stage_names
