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
    KEEPSATS_INTERNAL_TRANSFER_FLOW,
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


# ---------------------------------------------------------------------------
# Tests: Reply op filter (parent_id check)
# ---------------------------------------------------------------------------


class TestReplyOpFilter:
    """Custom_json ops that carry a parent_id (fee, notification) are NOT
    independent customer triggers and must not create new flow candidates."""

    @staticmethod
    def _reply_op(
        from_account: str = "threespeakselfie",
        to_account: str = "v4vapp",
        parent_id: str | None = "parent_group_id_abc",
        group_id: str = "gid_reply",
        short_id: str = "sid_reply",
    ) -> object:
        """Fake custom_json op whose json_data carries an optional parent_id."""
        json_data = type("FakeJsonData", (), {"parent_id": parent_id})()
        return type(
            "FakeCustomJsonOp",
            (),
            {
                "group_id": group_id,
                "short_id": short_id,
                "op_type": "custom_json",
                "from_account": from_account,
                "to_account": to_account,
                "json_data": json_data,
            },
        )()

    @pytest.mark.asyncio
    async def test_reply_op_with_parent_id_skipped(self):
        """Custom_json with parent_id must NOT create flow candidates."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_HIVE_FLOW)
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        op = self._reply_op(parent_id="104796233_2453fa_1_real")
        trigger = _op_event("custom_json", group_id="gid_reply", short_id="sid_reply")
        result = await ow._try_create_flow(trigger, op)

        assert result is None
        assert len(ow.active_flows) == 0

    @pytest.mark.asyncio
    async def test_customer_op_without_parent_id_not_skipped(self):
        """Customer custom_json WITHOUT parent_id MUST create flow candidates."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_HIVE_FLOW)
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        op = self._reply_op(parent_id=None)
        trigger = _op_event("custom_json", group_id="gid_cust", short_id="sid_cust")
        result = await ow._try_create_flow(trigger, op)

        assert result is not None
        assert len(ow.active_flows) == 2  # both candidates created

    @pytest.mark.asyncio
    async def test_customer_op_with_empty_parent_id_not_skipped(self):
        """Custom_json with empty string parent_id is treated as no parent."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_HIVE_FLOW)
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        op = self._reply_op(parent_id="")
        trigger = _op_event("custom_json", group_id="gid_cust", short_id="sid_cust")
        result = await ow._try_create_flow(trigger, op)

        assert result is not None
        assert len(ow.active_flows) == 2

    @pytest.mark.asyncio
    async def test_op_without_json_data_not_skipped(self):
        """Ops without json_data (e.g. transfers) must not be affected."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch._loaded_from_redis = True

        # Transfer op — no json_data attribute
        op = type(
            "FakeTransferOp",
            (),
            {
                "group_id": "gid_transfer",
                "short_id": "sid_transfer",
                "op_type": "transfer",
                "from_account": "customer",
                "to_account": "someaccount",
            },
        )()
        trigger = _op_event("transfer", group_id="gid_transfer", short_id="sid_transfer")
        result = await ow._try_create_flow(trigger, op)

        assert result is not None
        assert len(ow.active_flows) == 1


# ---------------------------------------------------------------------------
# Tests: Notification reply completion
# ---------------------------------------------------------------------------


class TestNotificationReplyCompletion:
    """When a notification custom_json with parent_id arrives, active flows
    tied to that parent should be completed — this is the terminal signal
    that the transaction has been fully processed (success or failure)."""

    @staticmethod
    def _notification_op(
        parent_id: str = "gid_trigger",
        notification: bool = True,
        memo: str = "Lightning error: Payment failed: FAILURE_REASON_INCORRECT_PAYMENT_DETAILS",
        from_account: str = "v4vapp",
        to_account: str = "jannost",
        group_id: str = "gid_notif",
        short_id: str = "sid_notif",
    ) -> object:
        """Fake notification custom_json op with parent_id + notification flag."""
        json_data = type(
            "FakeJsonData",
            (),
            {"parent_id": parent_id, "notification": notification, "memo": memo},
        )()
        return type(
            "FakeNotifOp",
            (),
            {
                "group_id": group_id,
                "short_id": short_id,
                "op_type": "custom_json",
                "from_account": from_account,
                "to_account": to_account,
                "json_data": json_data,
                "conv": None,
                "timestamp": _TS,
                "log_str": f"notif {short_id}",
            },
        )()

    @staticmethod
    def _customer_op(
        group_id: str = "gid_trigger",
        short_id: str = "sid_trigger",
        from_account: str = "jannost",
        to_account: str = "v4vapp",
    ) -> object:
        """Fake customer custom_json trigger op (no parent_id)."""
        json_data = type("FD", (), {"parent_id": None, "notification": False})()
        return type(
            "FakeCustomerOp",
            (),
            {
                "group_id": group_id,
                "short_id": short_id,
                "op_type": "custom_json",
                "from_account": from_account,
                "to_account": to_account,
                "json_data": json_data,
                "conv": None,
                "timestamp": _TS,
                "log_str": f"cust {short_id}",
            },
        )()

    @pytest.mark.asyncio
    async def test_failed_payment_completed_by_notification(self):
        """Keepsats-to-external with failed payment: the notification reply
        should complete the most-progressed flow and cancel the others."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_HIVE_FLOW)
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch.register_flow(KEEPSATS_INTERNAL_TRANSFER_FLOW)
        Overwatch._loaded_from_redis = True

        # 1. Customer custom_json trigger creates 3 candidates
        trigger_gid = "104800922_53b2e5_1_real"
        customer_op = self._customer_op(group_id=trigger_gid, short_id="0922_53b2e5_1")
        await ow.ingest_op(customer_op)
        assert len(ow.active_flows) == 3

        # 2. Dispatch hold/release keepsats + failed payment (no withdraw/fee)
        for ev in [
            _ledger_event(
                LedgerType.HOLD_KEEPSATS, group_id=trigger_gid, short_id="0922_53b2e5_1"
            ),
            _ledger_event(
                LedgerType.RELEASE_KEEPSATS, group_id=trigger_gid, short_id="0922_53b2e5_1"
            ),
            _op_event("payment", group_id="gid_payment", short_id="sid_payment"),
        ]:
            await ow._dispatch(ev)

        # keepsats_to_external should be at 4/6 (trigger, hold, release, payment)
        ext_flow = next(
            f for f in ow.active_flows if f.flow_definition.name == "keepsats_to_external"
        )
        assert ext_flow.progress == "4/6 required stages complete"
        assert len(ow.active_flows) == 3  # all still active

        # 3. Notification reply arrives with parent_id
        notif_op = self._notification_op(parent_id=trigger_gid)
        await ow.ingest_op(notif_op)

        # The most progressed flow (keepsats_to_external) should be completed
        assert ext_flow.status == FlowStatus.COMPLETED
        # No active flows should remain
        assert len(ow.active_flows) == 0

    @pytest.mark.asyncio
    async def test_notification_without_notification_flag_does_not_complete(self):
        """A reply op with parent_id but notification=False (e.g. fee op)
        must NOT force-complete active flows."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch.register_flow(KEEPSATS_INTERNAL_TRANSFER_FLOW)
        Overwatch._loaded_from_redis = True

        trigger_gid = "gid_trigger_abc"
        customer_op = self._customer_op(group_id=trigger_gid, short_id="sid_trigger")
        await ow.ingest_op(customer_op)
        assert len(ow.active_flows) == 2

        # Fee op: has parent_id but notification=False
        fee_op = self._notification_op(
            parent_id=trigger_gid, notification=False, group_id="gid_fee", short_id="sid_fee"
        )
        await ow.ingest_op(fee_op)

        # Flows should still be active (fee doesn't complete them)
        assert len(ow.active_flows) == 2

    @pytest.mark.asyncio
    async def test_success_notification_does_not_force_complete(self):
        """A notification with parent_id but without 'Payment failed' in the
        memo (i.e. a success notification) must NOT force-complete flows —
        success flows complete naturally via their stage events."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch.register_flow(KEEPSATS_INTERNAL_TRANSFER_FLOW)
        Overwatch._loaded_from_redis = True

        trigger_gid = "gid_trigger_success"
        customer_op = self._customer_op(group_id=trigger_gid, short_id="sid_success")
        await ow.ingest_op(customer_op)
        assert len(ow.active_flows) == 2

        # Success notification: has parent_id and notification=True but no failure text
        success_notif = self._notification_op(
            parent_id=trigger_gid,
            memo="Sent 950 sats to Boltz | Thank you for using v4v.app",
            group_id="gid_snotif",
            short_id="sid_snotif",
        )
        await ow.ingest_op(success_notif)

        # Flows should still be active (success notifications don't force-complete)
        assert len(ow.active_flows) == 2

    @pytest.mark.asyncio
    async def test_notification_no_matching_flows_is_no_op(self):
        """Notification for a parent_id that has no active flows does nothing."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        # No flows created — just send a notification
        notif_op = self._notification_op(parent_id="nonexistent_parent")
        await ow.ingest_op(notif_op)

        assert len(ow.active_flows) == 0
        assert len(ow.completed_flows) == 0

    @pytest.mark.asyncio
    async def test_best_candidate_selected_by_progress(self):
        """When multiple candidates match, the one with the most matched
        required stages should be selected as the completed flow."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_HIVE_FLOW)
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        trigger_gid = "gid_trigger_best"
        customer_op = self._customer_op(group_id=trigger_gid, short_id="sid_best")
        await ow.ingest_op(customer_op)
        assert len(ow.active_flows) == 2

        # Give keepsats_to_external more matched stages
        for ev in [
            _ledger_event(LedgerType.HOLD_KEEPSATS, group_id=trigger_gid, short_id="sid_best"),
            _ledger_event(LedgerType.RELEASE_KEEPSATS, group_id=trigger_gid, short_id="sid_best"),
        ]:
            await ow._dispatch(ev)

        # keepsats_to_external: 3/6, keepsats_to_hive: 1/12
        ext = next(f for f in ow.active_flows if f.flow_definition.name == "keepsats_to_external")
        hive = next(f for f in ow.active_flows if f.flow_definition.name == "keepsats_to_hive")
        assert ext.progress == "3/6 required stages complete"
        assert hive.progress == "1/12 required stages complete"

        # Notification arrives
        notif = self._notification_op(parent_id=trigger_gid)
        await ow.ingest_op(notif)

        # keepsats_to_external should win (more progress)
        assert ext.status == FlowStatus.COMPLETED
        assert hive.status == FlowStatus.FAILED
        assert len(ow.active_flows) == 0

    @pytest.mark.asyncio
    async def test_already_completed_flow_not_affected(self):
        """If the flow already completed naturally before the notification,
        _complete_by_notification should be a no-op."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        trigger_gid = "gid_completed"
        customer_op = self._customer_op(group_id=trigger_gid, short_id="sid_completed")
        await ow.ingest_op(customer_op)
        assert len(ow.active_flows) == 1

        # Complete all required stages naturally
        for ev in [
            _ledger_event(
                LedgerType.HOLD_KEEPSATS, group_id=trigger_gid, short_id="sid_completed"
            ),
            _ledger_event(
                LedgerType.RELEASE_KEEPSATS, group_id=trigger_gid, short_id="sid_completed"
            ),
            _op_event("payment", group_id="gid_pay", short_id="sid_pay"),
            _ledger_event(LedgerType.WITHDRAW_LIGHTNING, group_id="gid_pay", short_id="sid_pay"),
            _ledger_event(LedgerType.FEE_EXPENSE, group_id="gid_pay", short_id="sid_pay"),
        ]:
            await ow._dispatch(ev)

        assert len(ow.active_flows) == 0
        assert len(ow.completed_flows) == 1

        # Now the notification arrives — should be a no-op
        notif = self._notification_op(parent_id=trigger_gid)
        await ow.ingest_op(notif)

        assert len(ow.completed_flows) == 1
        assert len(ow.active_flows) == 0

    @staticmethod
    def _transfer_refund_op(
        ref_short_id: str = "0922_53b2e5_1",
        memo: str = "Lightning error: 🆅 lnbc550u1p56t2 Payment failed: FAILURE_REASON_ERROR | § {ref} | Thank you for using v4v.app",
        group_id: str = "gid_refund",
        short_id: str = "sid_refund",
        from_account: str = "v4vapp",
        to_account: str = "bitcoinman",
    ) -> object:
        """Fake transfer op representing a Hive/HBD refund with § short_id."""
        final_memo = memo.replace("{ref}", ref_short_id)
        return type(
            "FakeTransferRefund",
            (),
            {
                "group_id": group_id,
                "short_id": short_id,
                "op_type": "transfer",
                "from_account": from_account,
                "to_account": to_account,
                "memo": final_memo,
                "json_data": None,
                "conv": None,
                "timestamp": _TS,
                "log_str": f"refund {short_id}",
            },
        )()

    @pytest.mark.asyncio
    async def test_transfer_refund_completes_flow_by_short_id(self):
        """A Hive/HBD transfer refund with 'Payment failed' and § short_id
        in the memo should complete active flows via trigger_short_id match."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_HIVE_FLOW)
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch.register_flow(KEEPSATS_INTERNAL_TRANSFER_FLOW)
        Overwatch._loaded_from_redis = True

        trigger_gid = "104281476_d9b6d8_1_real"
        trigger_sid = "1476_d9b6d8_1"
        customer_op = self._customer_op(group_id=trigger_gid, short_id=trigger_sid)
        await ow.ingest_op(customer_op)
        assert len(ow.active_flows) == 3

        # Dispatch some progress on keepsats_to_external
        for ev in [
            _ledger_event(
                LedgerType.HOLD_KEEPSATS,
                group_id=trigger_gid,
                short_id=trigger_sid,
            ),
            _ledger_event(
                LedgerType.RELEASE_KEEPSATS,
                group_id=trigger_gid,
                short_id=trigger_sid,
            ),
        ]:
            await ow._dispatch(ev)

        ext_flow = next(
            f for f in ow.active_flows if f.flow_definition.name == "keepsats_to_external"
        )
        assert ext_flow.progress == "3/6 required stages complete"

        # Transfer refund arrives — memo contains "Payment failed" + § short_id
        refund_op = self._transfer_refund_op(ref_short_id=trigger_sid)
        await ow.ingest_op(refund_op)

        # Best candidate completed, rest cancelled
        assert ext_flow.status == FlowStatus.COMPLETED
        assert len(ow.active_flows) == 0

    @pytest.mark.asyncio
    async def test_transfer_without_payment_failed_does_not_complete(self):
        """A transfer with § short_id but without 'Payment failed' in memo
        should NOT force-complete flows (e.g. a normal refund memo)."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch._loaded_from_redis = True

        trigger_gid = "gid_trigger_norefund"
        customer_op = self._customer_op(group_id=trigger_gid, short_id="sid_norefund")
        await ow.ingest_op(customer_op)
        assert len(ow.active_flows) == 1

        # Transfer with § short_id but no "Payment failed" in memo
        normal_transfer = self._transfer_refund_op(
            ref_short_id="sid_norefund",
            memo="Refund: § sid_norefund | Thank you",
        )
        await ow.ingest_op(normal_transfer)

        # Flow should still be active
        assert len(ow.active_flows) == 1
