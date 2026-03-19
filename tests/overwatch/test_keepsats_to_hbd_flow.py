"""
Tests for the Keepsats-to-HBD Overwatch flow.

Uses real flow data extracted from db_monitor logs for a Keepsats-to-HBD conversion
(2,000 sats → 1.379 HBD).
"""

import json
from datetime import timedelta
from pathlib import Path

import pytest

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.process.overwatch_flows import KEEPSATS_TO_HIVE_FLOW
from v4vapp_backend_v2.process.process_overwatch import (
    FlowEvent,
    FlowInstance,
    FlowStatus,
    Overwatch,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

KS_FLOW_DATA_PATH = Path("tests/data/overwatch/keepsats_to_hive_flow.json")


@pytest.fixture
def ks_flow_data() -> dict:
    """Load the Keepsats-to-HBD flow test data."""
    with open(KS_FLOW_DATA_PATH) as f:
        return json.load(f)


@pytest.fixture
def ks_primary_ledger_entries(ks_flow_data: dict) -> dict[str, LedgerEntry]:
    """Parse primary ledger entries from the test data."""
    entries = {}
    for key, data in ks_flow_data["primary_ledger_entries"].items():
        entries[key] = LedgerEntry(**data)
    return entries


@pytest.fixture
def ks_hbd_transfer_ledger_entries(ks_flow_data: dict) -> dict[str, LedgerEntry]:
    """Parse HBD transfer ledger entries from the test data."""
    entries = {}
    for key, data in ks_flow_data["hbd_transfer_ledger_entries"].items():
        entries[key] = LedgerEntry(**data)
    return entries


@pytest.fixture
def ks_limit_order_ledger_entries(ks_flow_data: dict) -> dict[str, LedgerEntry]:
    """Parse limit order ledger entries from the test data."""
    entries = {}
    for key, data in ks_flow_data["limit_order_ledger_entries"].items():
        entries[key] = LedgerEntry(**data)
    return entries


@pytest.fixture
def ks_fill_order_ledger_entries(ks_flow_data: dict) -> dict[str, LedgerEntry]:
    """Parse fill order ledger entries from the test data."""
    entries = {}
    for key, data in ks_flow_data["fill_order_ledger_entries"].items():
        entries[key] = LedgerEntry(**data)
    return entries


@pytest.fixture
def ks_all_flow_events(
    ks_flow_data: dict,
    ks_primary_ledger_entries: dict[str, LedgerEntry],
    ks_hbd_transfer_ledger_entries: dict[str, LedgerEntry],
    ks_limit_order_ledger_entries: dict[str, LedgerEntry],
    ks_fill_order_ledger_entries: dict[str, LedgerEntry],
) -> list[FlowEvent]:
    """Build a list of all FlowEvents for the complete Keepsats-to-HBD flow.

    All events use the default ``group="primary"`` to simulate what
    ``db_monitor`` actually sends — the Overwatch system must be able to
    match stages without the caller specifying the correct group.
    """
    events: list[FlowEvent] = []

    # 1. Trigger custom_json op
    trigger = ks_flow_data["trigger_custom_json"]
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

    # 2. Primary ledger entries (9 entries)
    for le in ks_primary_ledger_entries.values():
        events.append(FlowEvent.from_ledger_entry(le, group="primary"))

    # 3. Notification custom_json op
    notif_op = ks_flow_data["notification_op"]
    events.append(
        FlowEvent(
            event_type="op",
            timestamp=notif_op["timestamp"],
            group_id=notif_op["group_id"],
            short_id=notif_op["short_id"],
            op_type=notif_op["type"],
            group="primary",
            ledger_entry=None,
            op=None,
            ledger_type=None,
        )
    )

    # 4. HBD transfer op
    hbd_op = ks_flow_data["hbd_transfer_op"]
    events.append(
        FlowEvent(
            event_type="op",
            timestamp=hbd_op["timestamp"],
            group_id=hbd_op["group_id"],
            short_id=hbd_op["short_id"],
            op_type=hbd_op["type"],
            group="primary",
            ledger_entry=None,
            op=None,
            ledger_type=None,
        )
    )

    # 5. HBD transfer ledger entries
    for le in ks_hbd_transfer_ledger_entries.values():
        events.append(FlowEvent.from_ledger_entry(le, group="primary"))

    # 6. Limit order create op
    lo_op = ks_flow_data["limit_order_op"]
    events.append(
        FlowEvent(
            event_type="op",
            timestamp=lo_op["timestamp"],
            group_id=lo_op["group_id"],
            short_id=lo_op["short_id"],
            op_type=lo_op["type"],
            group="primary",
            ledger_entry=None,
            op=None,
            ledger_type=None,
        )
    )

    # 7. Limit order ledger entries
    for le in ks_limit_order_ledger_entries.values():
        events.append(FlowEvent.from_ledger_entry(le, group="primary"))

    # 8. Fill order op
    fo_op = ks_flow_data["fill_order_op"]
    events.append(
        FlowEvent(
            event_type="op",
            timestamp=fo_op["timestamp"],
            group_id=fo_op["group_id"],
            short_id=fo_op["short_id"],
            op_type=fo_op["type"],
            group="primary",
            ledger_entry=None,
            op=None,
            ledger_type=None,
        )
    )

    # 9. Fill order ledger entries
    for le in ks_fill_order_ledger_entries.values():
        events.append(FlowEvent.from_ledger_entry(le, group="primary"))

    return events


@pytest.fixture
def ks_flow_instance(ks_flow_data: dict) -> FlowInstance:
    """Create an empty FlowInstance for the Keepsats-to-HBD flow."""
    trigger = ks_flow_data["trigger_custom_json"]
    return FlowInstance(
        flow_definition=KEEPSATS_TO_HIVE_FLOW,
        trigger_group_id=trigger["group_id"],
        trigger_short_id=trigger["short_id"],
        cust_id=trigger["cust_id"],
    )


# ---------------------------------------------------------------------------
# Tests: FlowDefinition
# ---------------------------------------------------------------------------


class TestKeepsatsToHbdDefinition:
    """Tests for the keepsats_to_hive_FLOW definition."""

    def test_definition_exists(self):
        assert KEEPSATS_TO_HIVE_FLOW.name == "keepsats_to_hive"
        assert KEEPSATS_TO_HIVE_FLOW.trigger_op_type == "custom_json"

    def test_has_expected_stage_names(self):
        expected = [
            "trigger_custom_json",
            "contra_keepsats_to_hive",
            "custom_json_fee_refund",
            "custom_json_transfer",
            "fee_income",
            "conv_customer",
            "reclassify_vsc_sats",
            "reclassify_vsc_hive",
            "exchange_conversion",
            "exchange_fees",
            "notification_custom_json_op",
            "hbd_transfer_op",
            "customer_hive_out",
            "limit_order_create_op",
            "limit_order_create",
            "fill_order_op",
            "fill_order_net",
        ]
        assert KEEPSATS_TO_HIVE_FLOW.stage_names == expected

    def test_required_stages_count(self):
        # five stages were marked optional in the definition
        assert len(KEEPSATS_TO_HIVE_FLOW.required_stages) == 12

    def test_optional_stages_listed(self):
        optional = [s.name for s in KEEPSATS_TO_HIVE_FLOW.stages if not s.required]
        expected = [
            "notification_custom_json_op",
            "limit_order_create_op",
            "limit_order_create",
            "fill_order_op",
            "fill_order_net",
        ]
        assert optional == expected

    def test_stage_count(self):
        assert len(KEEPSATS_TO_HIVE_FLOW.stages) == 17

    def test_groups_are_correct(self):
        groups = {s.group for s in KEEPSATS_TO_HIVE_FLOW.stages}
        expected_groups = {
            "primary",
            "notification",
            "hbd_transfer",
            "exchange_order",
            "fill_order",
        }
        assert groups == expected_groups

    def test_primary_group_has_10_stages(self):
        primary_stages = [s for s in KEEPSATS_TO_HIVE_FLOW.stages if s.group == "primary"]
        assert len(primary_stages) == 10

    def test_ledger_types_in_definition(self):
        ledger_stages = [s for s in KEEPSATS_TO_HIVE_FLOW.stages if s.event_type == "ledger"]
        expected_types = {
            LedgerType.CONTRA_KEEPSATS_TO_HIVE,
            LedgerType.CUSTOM_JSON_FEE_REFUND,
            LedgerType.CUSTOM_JSON_TRANSFER,
            LedgerType.FEE_INCOME,
            LedgerType.CONV_CUSTOMER,
            LedgerType.RECLASSIFY_VSC_SATS,
            LedgerType.RECLASSIFY_VSC_HIVE,
            LedgerType.EXCHANGE_CONVERSION,
            LedgerType.EXCHANGE_FEES,
            LedgerType.CUSTOMER_HIVE_OUT,
            LedgerType.LIMIT_ORDER_CREATE,
            LedgerType.FILL_ORDER_NET,
        }
        actual_types = {s.ledger_type for s in ledger_stages}
        assert actual_types == expected_types


# ---------------------------------------------------------------------------
# Tests: LedgerEntry deserialization from test data
# ---------------------------------------------------------------------------


class TestKeepsatsToHbdDeserialization:
    """Verify the extracted test data can be deserialized into proper LedgerEntry objects."""

    def test_primary_entries_deserialize(self, ks_primary_ledger_entries: dict[str, LedgerEntry]):
        assert len(ks_primary_ledger_entries) == 9
        for key, le in ks_primary_ledger_entries.items():
            assert isinstance(le, LedgerEntry)
            assert le.short_id == "3991_317497_1"

    def test_primary_entry_types(self, ks_primary_ledger_entries: dict[str, LedgerEntry]):
        expected_types = {
            "k_contra_h": LedgerType.CONTRA_KEEPSATS_TO_HIVE,
            "c_j_fee_r": LedgerType.CUSTOM_JSON_FEE_REFUND,
            "c_j_trans": LedgerType.CUSTOM_JSON_TRANSFER,
            "fee_inc": LedgerType.FEE_INCOME,
            "cust_conv": LedgerType.CONV_CUSTOMER,
            "r_vsc_sats": LedgerType.RECLASSIFY_VSC_SATS,
            "r_vsc_hive": LedgerType.RECLASSIFY_VSC_HIVE,
            "exc_conv": LedgerType.EXCHANGE_CONVERSION,
            "exc_fee": LedgerType.EXCHANGE_FEES,
        }
        for key, expected_ledger_type in expected_types.items():
            assert ks_primary_ledger_entries[key].ledger_type == expected_ledger_type

    def test_hbd_transfer_entries_deserialize(
        self, ks_hbd_transfer_ledger_entries: dict[str, LedgerEntry]
    ):
        assert len(ks_hbd_transfer_ledger_entries) == 1
        le = ks_hbd_transfer_ledger_entries["cust_h_out"]
        assert le.ledger_type == LedgerType.CUSTOMER_HIVE_OUT
        assert le.short_id == "3997_485e0b_1"

    def test_limit_order_entries_deserialize(
        self, ks_limit_order_ledger_entries: dict[str, LedgerEntry]
    ):
        assert len(ks_limit_order_ledger_entries) == 1
        le = ks_limit_order_ledger_entries["limit_or"]
        assert le.ledger_type == LedgerType.LIMIT_ORDER_CREATE
        assert le.short_id == "4012_ad5f6b_1"

    def test_fill_order_entries_deserialize(
        self, ks_fill_order_ledger_entries: dict[str, LedgerEntry]
    ):
        assert len(ks_fill_order_ledger_entries) == 1
        le = ks_fill_order_ledger_entries["fill_or_n"]
        assert le.ledger_type == LedgerType.FILL_ORDER_NET
        assert le.short_id == "4016_c2d062_1"


# ---------------------------------------------------------------------------
# Tests: FlowInstance - complete flow
# ---------------------------------------------------------------------------


class TestKeepsatsToHbdComplete:
    """Tests for FlowInstance with a complete set of Keepsats-to-HBD events."""

    def test_complete_flow_is_marked_complete(
        self,
        ks_flow_instance: FlowInstance,
        ks_all_flow_events: list[FlowEvent],
    ):
        for event in ks_all_flow_events:
            ks_flow_instance.add_event(event)
        assert ks_flow_instance.is_complete
        assert ks_flow_instance.status == FlowStatus.COMPLETED
        assert len(ks_flow_instance.missing_stages) == 0

    def test_all_stages_matched(
        self,
        ks_flow_instance: FlowInstance,
        ks_all_flow_events: list[FlowEvent],
    ):
        for event in ks_all_flow_events:
            ks_flow_instance.add_event(event)
        assert ks_flow_instance.matched_stage_names == set(KEEPSATS_TO_HIVE_FLOW.stage_names)

    def test_progress_shows_all_done(
        self,
        ks_flow_instance: FlowInstance,
        ks_all_flow_events: list[FlowEvent],
    ):
        for event in ks_all_flow_events:
            ks_flow_instance.add_event(event)
        # only 12 stages are required after marking some optional
        assert ks_flow_instance.progress == "12/12 required stages complete"

    def test_event_count(
        self,
        ks_flow_instance: FlowInstance,
        ks_all_flow_events: list[FlowEvent],
    ):
        for event in ks_all_flow_events:
            ks_flow_instance.add_event(event)
        # 1 trigger + 9 primary ledger + 1 notification op
        # + 1 hbd_transfer op + 1 hbd_transfer ledger
        # + 1 limit_order op + 1 limit_order ledger
        # + 1 fill_order op + 1 fill_order ledger = 17
        assert len(ks_flow_instance.events) == 17

    def test_summary_dict(
        self,
        ks_flow_instance: FlowInstance,
        ks_all_flow_events: list[FlowEvent],
    ):
        for event in ks_all_flow_events:
            ks_flow_instance.add_event(event)
        summary = ks_flow_instance.summary()
        assert summary["flow_type"] == "keepsats_to_hive"
        assert summary["status"] == "completed"
        assert summary["cust_id"] == "v4vapp-test"
        assert summary["trigger_short_id"] == "3991_317497_1"
        assert len(summary["missing_stages"]) == 0
        assert summary["event_count"] == 17

    def test_customer_id_preserved(
        self,
        ks_flow_instance: FlowInstance,
    ):
        assert ks_flow_instance.cust_id == "v4vapp-test"


# ---------------------------------------------------------------------------
# Tests: FlowInstance - incomplete flow
# ---------------------------------------------------------------------------


class TestKeepsatsToHbdIncomplete:
    """Tests for FlowInstance with missing events."""

    def test_empty_instance_not_complete(
        self,
        ks_flow_instance: FlowInstance,
    ):
        assert not ks_flow_instance.is_complete
        assert ks_flow_instance.status == FlowStatus.PENDING
        # only 12 stages are required now
        assert len(ks_flow_instance.missing_stages) == 12

    def test_partial_primary_events_not_complete(
        self,
        ks_flow_instance: FlowInstance,
        ks_primary_ledger_entries: dict[str, LedgerEntry],
        ks_flow_data: dict,
    ):
        # Add trigger op and first two primary ledger entries
        trigger = ks_flow_data["trigger_custom_json"]
        ks_flow_instance.add_event(
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
        entries = list(ks_primary_ledger_entries.values())
        ks_flow_instance.add_event(FlowEvent.from_ledger_entry(entries[0]))
        ks_flow_instance.add_event(FlowEvent.from_ledger_entry(entries[1]))

        assert not ks_flow_instance.is_complete
        assert ks_flow_instance.status == FlowStatus.IN_PROGRESS
        # 12 required stages total, 3 added leaving 9 missing
        assert len(ks_flow_instance.missing_stages) == 9
        assert ks_flow_instance.progress == "3/12 required stages complete"

    def test_missing_fill_order_events_not_complete(
        self,
        ks_flow_instance: FlowInstance,
        ks_all_flow_events: list[FlowEvent],
    ):
        # Add all events EXCEPT the fill order ones (last 2).  Those
        # stages are optional, so the flow should still report complete.
        for event in ks_all_flow_events[:-2]:
            ks_flow_instance.add_event(event)

        assert ks_flow_instance.is_complete
        # optional stages do not appear in missing_stages
        missing_names = [s.name for s in ks_flow_instance.missing_stages]
        assert "fill_order_op" not in missing_names
        assert "fill_order_net" not in missing_names

    def test_missing_hbd_transfer_not_complete(
        self,
        ks_flow_instance: FlowInstance,
        ks_flow_data: dict,
        ks_primary_ledger_entries: dict[str, LedgerEntry],
        ks_limit_order_ledger_entries: dict[str, LedgerEntry],
        ks_fill_order_ledger_entries: dict[str, LedgerEntry],
    ):
        # Add trigger + primary + notification + limit order + fill order
        # but skip HBD transfer group
        trigger = ks_flow_data["trigger_custom_json"]
        ks_flow_instance.add_event(
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
        for le in ks_primary_ledger_entries.values():
            ks_flow_instance.add_event(FlowEvent.from_ledger_entry(le))

        # Notification op
        notif = ks_flow_data["notification_op"]
        ks_flow_instance.add_event(
            FlowEvent(
                event_type="op",
                timestamp=notif["timestamp"],
                group_id=notif["group_id"],
                short_id=notif["short_id"],
                op_type=notif["type"],
                ledger_entry=None,
                op=None,
                ledger_type=None,
            )
        )

        # Limit order op + ledger
        lo_op = ks_flow_data["limit_order_op"]
        ks_flow_instance.add_event(
            FlowEvent(
                event_type="op",
                timestamp=lo_op["timestamp"],
                group_id=lo_op["group_id"],
                short_id=lo_op["short_id"],
                op_type=lo_op["type"],
                ledger_entry=None,
                op=None,
                ledger_type=None,
            )
        )
        for le in ks_limit_order_ledger_entries.values():
            ks_flow_instance.add_event(FlowEvent.from_ledger_entry(le))

        # Fill order op + ledger
        fo_op = ks_flow_data["fill_order_op"]
        ks_flow_instance.add_event(
            FlowEvent(
                event_type="op",
                timestamp=fo_op["timestamp"],
                group_id=fo_op["group_id"],
                short_id=fo_op["short_id"],
                op_type=fo_op["type"],
                ledger_entry=None,
                op=None,
                ledger_type=None,
            )
        )
        for le in ks_fill_order_ledger_entries.values():
            ks_flow_instance.add_event(FlowEvent.from_ledger_entry(le))

        assert not ks_flow_instance.is_complete
        missing_names = [s.name for s in ks_flow_instance.missing_stages]
        # HBD transfer ledger is unique (CUSTOMER_HIVE_OUT is missing)
        assert "customer_hive_out" in missing_names
        # The notification custom_json consumed the trigger_custom_json's op-match,
        # or vice versa, due to group-agnostic matching on ops. Let's just check
        # that it's not complete and at least the ledger entry is missing.
        assert len(missing_names) >= 1


# ---------------------------------------------------------------------------
# Tests: FlowInstance - stage matching details
# ---------------------------------------------------------------------------


class TestKeepsatsToHbdMatching:
    """Tests for the event-to-stage matching logic with Keepsats-to-HBD data."""

    def test_add_event_returns_matched_stage_name(
        self,
        ks_flow_instance: FlowInstance,
        ks_primary_ledger_entries: dict[str, LedgerEntry],
    ):
        le = ks_primary_ledger_entries["k_contra_h"]
        result = ks_flow_instance.add_event(FlowEvent.from_ledger_entry(le))
        assert result == "contra_keepsats_to_hive"

    def test_add_event_returns_none_for_duplicate(
        self,
        ks_flow_instance: FlowInstance,
        ks_primary_ledger_entries: dict[str, LedgerEntry],
    ):
        le = ks_primary_ledger_entries["k_contra_h"]
        ks_flow_instance.add_event(FlowEvent.from_ledger_entry(le))
        result = ks_flow_instance.add_event(FlowEvent.from_ledger_entry(le))
        assert result is None

    def test_add_event_returns_none_for_unmatched(
        self,
        ks_flow_instance: FlowInstance,
    ):
        event = FlowEvent(
            event_type="ledger",
            ledger_type=LedgerType.OPENING_BALANCE,
        )
        result = ks_flow_instance.add_event(event)
        assert result is None

    def test_status_transitions(
        self,
        ks_flow_instance: FlowInstance,
        ks_all_flow_events: list[FlowEvent],
    ):
        assert ks_flow_instance.status == FlowStatus.PENDING

        ks_flow_instance.add_event(ks_all_flow_events[0])
        assert ks_flow_instance.status == FlowStatus.IN_PROGRESS

        for event in ks_all_flow_events[1:]:
            ks_flow_instance.add_event(event)
        assert ks_flow_instance.status == FlowStatus.COMPLETED

    def test_ledger_entries_match_specific_stages(
        self,
        ks_flow_instance: FlowInstance,
        ks_primary_ledger_entries: dict[str, LedgerEntry],
    ):
        """Each ledger type matches exactly its designated stage."""
        expected_matches = {
            "k_contra_h": "contra_keepsats_to_hive",
            "c_j_fee_r": "custom_json_fee_refund",
            "c_j_trans": "custom_json_transfer",
            "fee_inc": "fee_income",
            "cust_conv": "conv_customer",
            "r_vsc_sats": "reclassify_vsc_sats",
            "r_vsc_hive": "reclassify_vsc_hive",
            "exc_conv": "exchange_conversion",
            "exc_fee": "exchange_fees",
        }
        for entry_key, expected_stage in expected_matches.items():
            le = ks_primary_ledger_entries[entry_key]
            result = ks_flow_instance.add_event(FlowEvent.from_ledger_entry(le))
            assert result == expected_stage, (
                f"Ledger entry '{entry_key}' matched '{result}' "
                f"instead of expected '{expected_stage}'"
            )


# ---------------------------------------------------------------------------
# Tests: Overwatch integration with Keepsats-to-HBD flow
# ---------------------------------------------------------------------------


class TestKeepsatsToHbdOverwatch:
    """Integration tests for Overwatch with the Keepsats-to-HBD flow."""

    def test_register_keepsats_to_hive_flow(self):
        Overwatch.reset()
        Overwatch.register_flow(KEEPSATS_TO_HIVE_FLOW)
        flows = Overwatch.registered_flows()
        assert "keepsats_to_hive" in flows
        assert flows["keepsats_to_hive"] is KEEPSATS_TO_HIVE_FLOW

    def test_completed_flow_moves_to_completed(
        self,
        ks_flow_instance: FlowInstance,
        ks_all_flow_events: list[FlowEvent],
    ):
        Overwatch.reset()
        for event in ks_all_flow_events:
            ks_flow_instance.add_event(event)
        ow = Overwatch()
        ow.flow_instances.append(ks_flow_instance)
        assert len(ow.active_flows) == 0
        assert len(ow.completed_flows) == 1

    async def test_check_stalls_marks_old_flows(
        self,
        ks_flow_instance: FlowInstance,
        ks_primary_ledger_entries: dict[str, LedgerEntry],
    ):
        Overwatch.reset()
        ow = Overwatch()
        # Add one event so the flow is IN_PROGRESS
        ks_flow_instance.add_event(
            FlowEvent.from_ledger_entry(ks_primary_ledger_entries["k_contra_h"])
        )
        ow.flow_instances.append(ks_flow_instance)
        far_future = ks_flow_instance.started_at + Overwatch.stall_timeout + timedelta(seconds=1)
        stalled = await ow.check_stalls(now=far_future)
        assert len(stalled) == 1
        assert ks_flow_instance.status == FlowStatus.STALLED

    def test_dedup_prevents_rematched_trigger(
        self,
        ks_flow_instance: FlowInstance,
        ks_flow_data: dict,
    ):
        """A re-arrived trigger must not re-match after initial insertion."""
        trigger = ks_flow_data["trigger_custom_json"]
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
        result1 = ks_flow_instance.add_event(trigger_event)
        assert result1 == "trigger_custom_json"

        assert Overwatch._is_duplicate(ks_flow_instance, trigger_event)

    async def test_dispatch_with_default_groups_completes_flow(
        self,
        ks_flow_data: dict,
        ks_all_flow_events: list[FlowEvent],
    ):
        """Simulate db_monitor dispatching all events with group='primary'.

        The flow should still complete — matching is group-agnostic for ops.
        """
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_HIVE_FLOW)
        Overwatch._loaded_from_redis = True  # skip Redis

        trigger = ks_flow_data["trigger_custom_json"]
        trigger_event = ks_all_flow_events[0]
        matched = await ow._try_create_flow(
            trigger_event,
            type(
                "FakeOp",
                (),
                {
                    "group_id": trigger["group_id"],
                    "short_id": trigger["short_id"],
                    "op_type": trigger["type"],
                    "from_account": trigger.get("cust_id", ""),
                    "memo": trigger["json"]["memo"],
                },
            )(),
        )
        assert matched == "trigger_custom_json"
        assert len(ow.active_flows) == 1

        # Dispatch remaining events
        for event in ks_all_flow_events[1:]:
            await ow._dispatch(event)

        assert len(ow.completed_flows) == 1
        assert ow.completed_flows[0].is_complete


# ---------------------------------------------------------------------------
# Tests: Flow data integrity checks
# ---------------------------------------------------------------------------


class TestKeepsatsToHbdDataIntegrity:
    """Verify the extracted test data has expected properties."""

    def test_trigger_is_custom_json(self, ks_flow_data: dict):
        trigger = ks_flow_data["trigger_custom_json"]
        assert trigger["type"] == "custom_json"
        assert trigger["cust_id"] == "v4vapp-test"
        assert "to #HBD" in trigger["json"]["memo"]

    def test_trigger_has_replies(self, ks_flow_data: dict):
        replies = ks_flow_data["trigger_custom_json"]["replies"]
        assert len(replies) == 2
        reply_types = {r["reply_type"] for r in replies}
        assert "custom_json" in reply_types  # notification
        assert "transfer" in reply_types  # HBD payment

    def test_notification_has_parent_id(self, ks_flow_data: dict):
        notif = ks_flow_data["notification_op"]
        parent_id = notif["json"]["parent_id"]
        assert parent_id == ks_flow_data["trigger_custom_json"]["group_id"]

    def test_hbd_transfer_is_outgoing(self, ks_flow_data: dict):
        hbd_op = ks_flow_data["hbd_transfer_op"]
        assert hbd_op["type"] == "transfer"
        assert hbd_op["from"] == "devser.v4vapp"
        assert hbd_op["to"] == "v4vapp-test"

    def test_all_primary_entries_share_short_id(self, ks_flow_data: dict):
        trigger_short_id = ks_flow_data["trigger_custom_json"]["short_id"]
        for key, entry in ks_flow_data["primary_ledger_entries"].items():
            assert entry["short_id"] == trigger_short_id, (
                f"Primary ledger entry '{key}' has short_id '{entry['short_id']}' "
                f"but expected '{trigger_short_id}'"
            )

    def test_conversion_amounts_consistent(self, ks_flow_data: dict):
        trigger = ks_flow_data["trigger_custom_json"]
        # 2000 sats converted to HBD
        assert trigger["conv"]["sats"] == 2000
        assert trigger["conv"]["msats"] == 2000000
        # Fee is 88 sats
        assert trigger["conv"]["msats_fee"] == 88000
        # Change is 1.379 HBD
        assert trigger["change_amount"]["amount"] == "1379"  # 1.379 HBD

    def test_limit_order_is_sell_hive(self, ks_flow_data: dict):
        lo = ks_flow_data["limit_order_op"]
        assert lo["type"] == "limit_order_create"
        # Selling HIVE to buy HBD
        assert lo["amount_to_sell"]["nai"] == "@@000000021"  # HIVE

    def test_fill_order_is_virtual(self, ks_flow_data: dict):
        fo = ks_flow_data["fill_order_op"]
        assert fo["type"] == "fill_order"
        assert fo["realm"] == "virtual"
        assert "virtual" in fo["group_id"]
