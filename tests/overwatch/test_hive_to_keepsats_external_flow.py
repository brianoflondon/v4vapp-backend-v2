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

from datetime import datetime, timedelta, timezone

import pytest

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.process.overwatch_flows import (
    HIVE_TO_KEEPSATS_EXTERNAL_FLOW,
    HIVE_TO_KEEPSATS_FLOW,
    KEEPSATS_TO_EXTERNAL_FLOW,
    KEEPSATS_TO_HIVE_FLOW,
)
from v4vapp_backend_v2.process.process_overwatch import FlowEvent, FlowStatus, Overwatch

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
    lambda: _ledger_event(LedgerType.FEE_EXPENSE, group_id="gid_payment", short_id="sid_payment"),
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
            HIVE_TO_KEEPSATS_EXTERNAL_FLOW.trigger_op_type == HIVE_TO_KEEPSATS_FLOW.trigger_op_type
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
    async def test_simple_deposit_keeps_external_candidate(self):
        """For a simple deposit (no payment events), hive_to_keepsats completes
        but hive_to_keepsats_external is kept alive because it is a superset
        flow — its definition has stages the winner doesn't cover."""
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

        # hive_to_keepsats_external is a superset — kept alive so it can
        # continue accumulating payment events if they arrive.
        ext_flows = [
            f for f in ow.flow_instances if f.flow_definition.name == "hive_to_keepsats_external"
        ]
        assert len(ext_flows) == 1
        assert ext_flows[0].status not in (FlowStatus.COMPLETED, FlowStatus.FAILED)

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
            f for f in ow.flow_instances if f.flow_definition.name == "hive_to_keepsats_external"
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
    async def test_superset_candidate_stays_active_after_base_completes(self):
        """In a simple deposit, the superset external candidate stays active."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = _SHARED_EVENTS[0]()
        await ow._try_create_flow(trigger, _fake_op())

        for factory in _SHARED_EVENTS[1:]:
            await ow._dispatch(factory())

        assert len(ow.active_flows) == 1
        assert ow.active_flows[0].flow_definition.name == "hive_to_keepsats_external"
        assert len(ow.completed_flows) == 1


# ---------------------------------------------------------------------------
# Tests: Superset grace period (Rule 1)
# ---------------------------------------------------------------------------


class TestSupersetGracePeriod:
    """Superset candidates are cancelled after the grace period expires
    if no distinguishing events arrive."""

    @pytest.mark.asyncio
    async def test_grace_period_set_on_superset_candidate(self):
        """When hive_to_keepsats completes, the superset candidate gets a
        superset_grace_expires timestamp."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = _SHARED_EVENTS[0]()
        await ow._try_create_flow(trigger, _fake_op())

        for factory in _SHARED_EVENTS[1:]:
            await ow._dispatch(factory())

        ext = ow.active_flows[0]
        assert ext.flow_definition.name == "hive_to_keepsats_external"
        assert ext.superset_grace_expires is not None

    @pytest.mark.asyncio
    async def test_superset_cancelled_after_grace_expires(self):
        """check_stalls cancels the superset candidate once grace period is up."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = _SHARED_EVENTS[0]()
        await ow._try_create_flow(trigger, _fake_op())
        for factory in _SHARED_EVENTS[1:]:
            await ow._dispatch(factory())

        ext = ow.active_flows[0]
        # Jump past the grace period
        future = datetime.now(tz=timezone.utc) + timedelta(seconds=31)
        await ow.check_stalls(now=future)

        assert len(ow.active_flows) == 0
        # The candidate was removed, not just stalled
        ext_remaining = [
            f for f in ow.flow_instances if f.flow_definition.name == "hive_to_keepsats_external"
        ]
        assert len(ext_remaining) == 0

    @pytest.mark.asyncio
    async def test_superset_survives_before_grace_expires(self):
        """check_stalls does NOT cancel if grace hasn't expired yet."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = _SHARED_EVENTS[0]()
        await ow._try_create_flow(trigger, _fake_op())
        for factory in _SHARED_EVENTS[1:]:
            await ow._dispatch(factory())

        # Only 10s later — well within grace
        future = datetime.now(tz=timezone.utc) + timedelta(seconds=10)
        await ow.check_stalls(now=future)
        assert len(ow.active_flows) == 1

    @pytest.mark.asyncio
    async def test_distinguishing_event_clears_grace(self):
        """A payment event (only in external flow) clears the grace timer."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = _SHARED_EVENTS[0]()
        await ow._try_create_flow(trigger, _fake_op())

        # Dispatch some shared events first
        await ow._dispatch(_SHARED_EVENTS[1]())  # cust_h_in
        await ow._dispatch(_SHARED_EVENTS[2]())  # hold_k

        # Payment events (only external has these)
        for factory in _PAYMENT_EVENTS:
            await ow._dispatch(factory())

        # Remaining shared events complete hive_to_keepsats
        for factory in _SHARED_EVENTS[3:]:
            await ow._dispatch(factory())

        ext = [
            f for f in ow.flow_instances if f.flow_definition.name == "hive_to_keepsats_external"
        ]
        assert len(ext) == 1
        # Grace timer should be cleared because payment events arrived
        assert ext[0].superset_grace_expires is None

    @pytest.mark.asyncio
    async def test_custom_grace_period(self):
        """The grace period is configurable via Overwatch.superset_grace_period."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.superset_grace_period = timedelta(seconds=60)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = _SHARED_EVENTS[0]()
        await ow._try_create_flow(trigger, _fake_op())
        for factory in _SHARED_EVENTS[1:]:
            await ow._dispatch(factory())

        # 40s later — would be expired with default 30s, but not with 60s
        future = datetime.now(tz=timezone.utc) + timedelta(seconds=40)
        await ow.check_stalls(now=future)
        assert len(ow.active_flows) == 1

        # 70s later — now it's expired
        future = datetime.now(tz=timezone.utc) + timedelta(seconds=70)
        await ow.check_stalls(now=future)
        assert len(ow.active_flows) == 0


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
        Overwatch.register_flow(KEEPSATS_TO_HIVE_FLOW)
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
        assert flow_names == {"keepsats_to_hive", "keepsats_to_external"}


# ---------------------------------------------------------------------------
# Tests: Trigger-only timeout
# ---------------------------------------------------------------------------


class TestTriggerOnlyTimeout:
    """Flows that only have their trigger op and no subsequent matched events
    should be cancelled after trigger_only_timeout (default 60s).  This
    prevents false positives from irrelevant ops (e.g. server-to-exchange
    transfers) lingering as stalled flows."""

    @pytest.mark.asyncio
    async def test_trigger_only_flow_cancelled_after_timeout(self):
        """A flow with only the trigger op is cancelled after trigger_only_timeout."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())
        assert len(ow.active_flows) == 2  # both candidates created

        # 61 seconds later — past trigger_only_timeout (60s)
        future = datetime.now(tz=timezone.utc) + timedelta(seconds=61)
        await ow.check_stalls(now=future)

        # Both candidates should be cancelled (FAILED + removed)
        assert len(ow.active_flows) == 0
        assert all(
            f.status == FlowStatus.FAILED
            for f in ow.flow_instances
            if f.flow_definition.name in ("hive_to_keepsats", "hive_to_keepsats_external")
        )

    @pytest.mark.asyncio
    async def test_trigger_only_flow_survives_before_timeout(self):
        """A trigger-only flow is NOT cancelled before timeout expires."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())

        # 30 seconds later — before trigger_only_timeout
        future = datetime.now(tz=timezone.utc) + timedelta(seconds=30)
        await ow.check_stalls(now=future)

        assert len(ow.active_flows) == 2  # still alive

    @pytest.mark.asyncio
    async def test_flow_with_subsequent_event_uses_normal_stall(self):
        """A flow that received a post-trigger event uses the normal stall
        timeout, NOT the trigger-only timeout."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())

        # Dispatch a second event (ledger) so flows have 2 matched events
        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))

        # 90 seconds later — past trigger_only_timeout but before stall_timeout
        future = datetime.now(tz=timezone.utc) + timedelta(seconds=90)
        await ow.check_stalls(now=future)

        # Flows should still be active (not cancelled by trigger-only rule)
        assert len(ow.active_flows) == 2

    @pytest.mark.asyncio
    async def test_trigger_only_timeout_configurable(self):
        """trigger_only_timeout can be customised."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.trigger_only_timeout = timedelta(seconds=10)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())

        # 11 seconds — past custom timeout
        future = datetime.now(tz=timezone.utc) + timedelta(seconds=11)
        await ow.check_stalls(now=future)
        assert len(ow.active_flows) == 0

    @pytest.mark.asyncio
    async def test_superset_grace_flow_not_caught_by_trigger_only(self):
        """A flow in superset grace is handled by the grace rule, not trigger-only."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = _SHARED_EVENTS[0]()
        await ow._try_create_flow(trigger, _fake_op())
        # Complete hive_to_keepsats → ext enters superset grace
        for factory in _SHARED_EVENTS[1:]:
            await ow._dispatch(factory())

        ext = ow.active_flows[0]
        assert ext.superset_grace_expires is not None

        # 61s later — past trigger_only_timeout. But superset grace (30s)
        # should have already cancelled it via Rule 1 instead of Rule 2.
        future = datetime.now(tz=timezone.utc) + timedelta(seconds=61)
        await ow.check_stalls(now=future)
        assert len(ow.active_flows) == 0


# ---------------------------------------------------------------------------
# Tests: add_event only appends matched events
# ---------------------------------------------------------------------------


class TestAddEventSelectiveAppend:
    """Verify that add_event only appends events that match a stage."""

    def test_unmatched_event_not_appended(self):
        """An event that doesn't match any stage should not be in flow.events."""
        from v4vapp_backend_v2.process.process_overwatch import FlowInstance

        Overwatch.reset()
        flow = FlowInstance(
            flow_definition=HIVE_TO_KEEPSATS_FLOW,
            trigger_group_id="gid_test",
            trigger_short_id="sid_test",
        )
        # Add trigger event (matches)
        trigger = _op_event("transfer", group_id="gid_test", short_id="sid_test")
        result = flow.add_event(trigger)
        assert result is not None
        assert len(flow.events) == 1

        # Add an event with a ledger type that doesn't match any stage
        unmatched = _ledger_event(
            LedgerType.SERVER_TO_EXCHANGE,
            group_id="gid_test",
            short_id="sid_test",
        )
        result = flow.add_event(unmatched)
        assert result is None
        assert len(flow.events) == 1  # unchanged — not appended

    def test_matched_event_appended(self):
        """A matched event IS appended normally."""
        from v4vapp_backend_v2.process.process_overwatch import FlowInstance

        Overwatch.reset()
        flow = FlowInstance(
            flow_definition=HIVE_TO_KEEPSATS_FLOW,
            trigger_group_id="gid_test",
            trigger_short_id="sid_test",
        )
        trigger = _op_event("transfer", group_id="gid_test", short_id="sid_test")
        flow.add_event(trigger)

        matched = _ledger_event(
            LedgerType.CUSTOMER_HIVE_IN,
            group_id="gid_test",
            short_id="sid_test",
        )
        result = flow.add_event(matched)
        assert result is not None
        assert len(flow.events) == 2


# ---------------------------------------------------------------------------
# Tests: Internal account transfer filter
# ---------------------------------------------------------------------------


class TestInternalAccountFilter:
    """Transfers between known system accounts (server, treasury, exchange,
    funding) should NOT create flow candidates."""

    @staticmethod
    def _internal_op(
        from_account: str,
        to_account: str,
        group_id: str = "gid_internal",
        short_id: str = "sid_internal",
    ) -> object:
        return type(
            "FakeOp",
            (),
            {
                "group_id": group_id,
                "short_id": short_id,
                "op_type": "transfer",
                "from_account": from_account,
                "to_account": to_account,
            },
        )()

    @pytest.mark.asyncio
    async def test_server_to_exchange_skipped(self):
        """Server → exchange transfer must not create flow candidates."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        op = self._internal_op("someaccount", "fiction")
        trigger = _op_event("transfer", group_id="gid_internal", short_id="sid_internal")
        result = await ow._try_create_flow(trigger, op)

        assert result is None
        assert len(ow.active_flows) == 0

    @pytest.mark.asyncio
    async def test_server_to_treasury_skipped(self):
        """Server → treasury transfer must not create flow candidates."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch._loaded_from_redis = True

        op = self._internal_op("someaccount", "devtre.v4vapp")
        trigger = _op_event("transfer", group_id="gid_internal", short_id="sid_internal")
        result = await ow._try_create_flow(trigger, op)

        assert result is None
        assert len(ow.active_flows) == 0

    @pytest.mark.asyncio
    async def test_treasury_to_exchange_skipped(self):
        """Treasury → exchange transfer must not create flow candidates."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch._loaded_from_redis = True

        op = self._internal_op("devtre.v4vapp", "fiction")
        trigger = _op_event("transfer", group_id="gid_internal", short_id="sid_internal")
        result = await ow._try_create_flow(trigger, op)

        assert result is None
        assert len(ow.active_flows) == 0

    @pytest.mark.asyncio
    async def test_customer_to_server_not_skipped(self):
        """Customer → server transfer MUST create flow candidates (deposit)."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        op = self._internal_op("v4vapp-test", "someaccount")
        trigger = _op_event("transfer", group_id="gid_cust", short_id="sid_cust")
        result = await ow._try_create_flow(trigger, op)

        assert result is not None
        assert len(ow.active_flows) == 2  # both candidates created

    @pytest.mark.asyncio
    async def test_server_to_customer_not_skipped(self):
        """Server → customer transfer MUST create flow candidates (withdrawal)."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch._loaded_from_redis = True

        op = self._internal_op("someaccount", "v4vapp-test")
        trigger = _op_event("transfer", group_id="gid_cust", short_id="sid_cust")
        result = await ow._try_create_flow(trigger, op)

        assert result is not None
        assert len(ow.active_flows) == 1
