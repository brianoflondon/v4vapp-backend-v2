"""
Tests for the Hive-to-Keepsats-External flow and superset resolution.

The hive_to_keepsats_external flow is a superset of hive_to_keepsats — it
includes all the same stages plus external payment stages (payment, withdraw_l,
fee_exp).  Tests verify that:
  - Both candidates are created on a transfer trigger
  - The superset flow survives when hive_to_keepsats completes first
  - The superset flow is correctly removed for simple deposits (no payment)
  - Both flows can complete independently when payment events arrive
"""

from datetime import datetime, timezone

import pytest

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.process.overwatch_flows import (
    HIVE_TO_KEEPSATS_EXTERNAL_FLOW,
    HIVE_TO_KEEPSATS_FLOW,
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
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2026, 3, 16, 17, 32, 46, tzinfo=timezone.utc)


def _op_event(
    op_type: str,
    group_id: str = "gid_trigger",
    short_id: str = "sid_trigger",
) -> FlowEvent:
    return FlowEvent(
        event_type="op",
        timestamp=_TS,
        group_id=group_id,
        short_id=short_id,
        op_type=op_type,
        group="primary",
    )


def _ledger_event(
    ledger_type: LedgerType,
    group_id: str = "gid_trigger",
    short_id: str = "sid_trigger",
) -> FlowEvent:
    return FlowEvent(
        event_type="ledger",
        timestamp=_TS,
        group_id=group_id,
        short_id=short_id,
        ledger_type=ledger_type,
        group="primary",
    )


def _fake_op(group_id: str = "gid_trigger", short_id: str = "sid_trigger") -> object:
    return type(
        "FakeOp",
        (),
        {
            "group_id": group_id,
            "short_id": short_id,
            "op_type": "transfer",
            "from_account": "v4vapp-test",
        },
    )()


# Events shared by both hive_to_keepsats and hive_to_keepsats_external
_SHARED_EVENTS = [
    lambda: _op_event("transfer"),
    lambda: _ledger_event(LedgerType.CUSTOMER_HIVE_IN),
    lambda: _ledger_event(LedgerType.HOLD_KEEPSATS),
    lambda: _ledger_event(LedgerType.CONV_HIVE_TO_KEEPSATS),
    lambda: _ledger_event(LedgerType.CONTRA_HIVE_TO_KEEPSATS),
    lambda: _ledger_event(LedgerType.CONV_CUSTOMER),
    lambda: _ledger_event(LedgerType.RELEASE_KEEPSATS),
    lambda: _op_event("custom_json", group_id="gid_fee", short_id="sid_fee"),
    lambda: _ledger_event(LedgerType.CUSTOM_JSON_FEE, group_id="gid_fee", short_id="sid_fee"),
    lambda: _ledger_event(LedgerType.FEE_INCOME, group_id="gid_fee", short_id="sid_fee"),
    lambda: _op_event("custom_json", group_id="gid_notif", short_id="sid_notif"),
    lambda: _ledger_event(
        LedgerType.RECEIVE_LIGHTNING, group_id="gid_notif", short_id="sid_notif"
    ),
    lambda: _op_event("transfer", group_id="gid_change", short_id="sid_change"),
    lambda: _ledger_event(
        LedgerType.CUSTOMER_HIVE_OUT, group_id="gid_change", short_id="sid_change"
    ),
]

# External payment events (only in hive_to_keepsats_external)
_PAYMENT_EVENTS = [
    lambda: _op_event("payment", group_id="gid_payment", short_id="sid_payment"),
    lambda: _ledger_event(
        LedgerType.WITHDRAW_LIGHTNING, group_id="gid_payment", short_id="sid_payment"
    ),
    lambda: _ledger_event(
        LedgerType.FEE_EXPENSE, group_id="gid_payment", short_id="sid_payment"
    ),
]


# ---------------------------------------------------------------------------
# Tests: FlowDefinition
# ---------------------------------------------------------------------------


class TestHiveToKeepsatsExternalDefinition:
    """Tests for the HIVE_TO_KEEPSATS_EXTERNAL_FLOW definition."""

    def test_definition_exists(self):
        assert HIVE_TO_KEEPSATS_EXTERNAL_FLOW.name == "hive_to_keepsats_external"
        assert HIVE_TO_KEEPSATS_EXTERNAL_FLOW.trigger_op_type == "transfer"

    def test_has_17_required_stages(self):
        assert len(HIVE_TO_KEEPSATS_EXTERNAL_FLOW.required_stages) == 17

    def test_includes_all_hive_to_keepsats_stages(self):
        base_names = set(HIVE_TO_KEEPSATS_FLOW.stage_names)
        external_names = set(HIVE_TO_KEEPSATS_EXTERNAL_FLOW.stage_names)
        assert base_names.issubset(external_names)

    def test_has_payment_stages(self):
        names = HIVE_TO_KEEPSATS_EXTERNAL_FLOW.stage_names
        assert "payment_op" in names
        assert "withdraw_lightning" in names
        assert "fee_expense" in names

    def test_same_trigger_as_hive_to_keepsats(self):
        assert (
            HIVE_TO_KEEPSATS_EXTERNAL_FLOW.trigger_op_type
            == HIVE_TO_KEEPSATS_FLOW.trigger_op_type
        )


# ---------------------------------------------------------------------------
# Tests: Multi-candidate with superset flows
# ---------------------------------------------------------------------------


class TestSupersetCandidateResolution:
    """Verify that the resolve logic correctly handles superset flows."""

    @pytest.mark.asyncio
    async def test_transfer_creates_two_candidates(self):
        """A transfer trigger should create both hive_to_keepsats
        and hive_to_keepsats_external as candidates."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        event = _op_event("transfer")
        result = await ow._try_create_flow(event, _fake_op())
        assert result == "trigger_transfer"
        assert len(ow.active_flows) == 2
        flow_names = {f.flow_definition.name for f in ow.active_flows}
        assert flow_names == {"hive_to_keepsats", "hive_to_keepsats_external"}

    @pytest.mark.asyncio
    async def test_simple_deposit_removes_external_candidate(self):
        """For a simple deposit (no payment events), hive_to_keepsats completes
        and hive_to_keepsats_external is removed because all its events
        are explainable by the winner's definition."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        # Create candidates
        trigger = _SHARED_EVENTS[0]()
        await ow._try_create_flow(trigger, _fake_op())
        assert len(ow.active_flows) == 2

        # Dispatch all shared events (except trigger which was already consumed)
        for factory in _SHARED_EVENTS[1:]:
            await ow._dispatch(factory())

        # hive_to_keepsats should complete (14/14)
        assert len(ow.completed_flows) == 1
        assert ow.completed_flows[0].flow_definition.name == "hive_to_keepsats"

        # hive_to_keepsats_external should be removed (all its events
        # are also in hive_to_keepsats, so no unique events)
        ext_flows = [
            f
            for f in ow.flow_instances
            if f.flow_definition.name == "hive_to_keepsats_external"
        ]
        assert len(ext_flows) == 0

    @pytest.mark.asyncio
    async def test_back_to_back_keeps_external_after_base_completes(self):
        """When payment events arrive before completion, the external candidate
        has unique events and survives resolution."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        # Create candidates
        trigger = _SHARED_EVENTS[0]()
        await ow._try_create_flow(trigger, _fake_op())

        # Dispatch some shared events first
        await ow._dispatch(_SHARED_EVENTS[1]())  # cust_h_in
        await ow._dispatch(_SHARED_EVENTS[2]())  # hold_k

        # Then payment events arrive (only external matches these)
        for factory in _PAYMENT_EVENTS:
            await ow._dispatch(factory())

        # Dispatch remaining shared events to complete hive_to_keepsats
        for factory in _SHARED_EVENTS[3:]:
            await ow._dispatch(factory())

        # hive_to_keepsats completed
        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "hive_to_keepsats" in completed_names

        # hive_to_keepsats_external should still be active (has payment events
        # not coverable by hive_to_keepsats) — OR already completed too
        ext_flows = [
            f
            for f in ow.flow_instances
            if f.flow_definition.name == "hive_to_keepsats_external"
        ]
        assert len(ext_flows) == 1
        # It should have completed too (17/17 stages all dispatched)
        assert ext_flows[0].status == FlowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_both_flows_complete_when_all_events_dispatched(self):
        """Both hive_to_keepsats and hive_to_keepsats_external complete
        independently when all events (shared + payment) are dispatched."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        # Create candidates
        trigger = _SHARED_EVENTS[0]()
        await ow._try_create_flow(trigger, _fake_op())

        # Dispatch payment events early (before completion)
        for factory in _PAYMENT_EVENTS:
            await ow._dispatch(factory())

        # Dispatch all shared events (except trigger)
        for factory in _SHARED_EVENTS[1:]:
            await ow._dispatch(factory())

        # Both should be completed
        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert completed_names == {"hive_to_keepsats", "hive_to_keepsats_external"}

    @pytest.mark.asyncio
    async def test_external_candidate_not_counted_as_active_when_removed(self):
        """In a simple deposit, after resolution there should be no active flows."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = _SHARED_EVENTS[0]()
        await ow._try_create_flow(trigger, _fake_op())

        for factory in _SHARED_EVENTS[1:]:
            await ow._dispatch(factory())

        assert len(ow.active_flows) == 0
        assert len(ow.completed_flows) == 1


# ---------------------------------------------------------------------------
# Tests: Interaction with custom_json-triggered flows
# ---------------------------------------------------------------------------


class TestMixedTriggerTypes:
    """Verify that transfer-triggered and custom_json-triggered flows
    don't interfere with each other."""

    @pytest.mark.asyncio
    async def test_custom_json_does_not_create_transfer_candidates(self):
        """A custom_json event should not create hive_to_keepsats candidates."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch.register_flow(KEEPSATS_TO_HBD_FLOW)
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        event = _op_event("custom_json", group_id="gid_cj", short_id="sid_cj")
        fake_op = type(
            "FakeOp",
            (),
            {
                "group_id": "gid_cj",
                "short_id": "sid_cj",
                "op_type": "custom_json",
                "from_account": "test",
            },
        )()
        await ow._try_create_flow(event, fake_op)

        flow_names = {f.flow_definition.name for f in ow.active_flows}
        assert "hive_to_keepsats" not in flow_names
        assert "hive_to_keepsats_external" not in flow_names
        assert flow_names == {"keepsats_to_hbd", "keepsats_to_external"}
