"""
Tests for the Balance Request flow.

Flow: User sends a small HIVE transfer (0.001 HIVE) with memo
"balance_request" → system replies with a transfer containing the
balance JSON in an encrypted memo → cust_h_out ledger.

This is a simple 4-stage flow that is a subset of hive_to_keepsats
and hive_to_keepsats_external; the superset mechanism handles the
candidate elimination.
"""

from datetime import datetime, timezone

import pytest

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.process.overwatch_flows import (
    BALANCE_REQUEST_FLOW,
    HIVE_TO_KEEPSATS_EXTERNAL_FLOW,
    HIVE_TO_KEEPSATS_FLOW,
    HIVE_TRANSFER_FAILURE_FLOW,
    HIVE_TRANSFER_PAYWITHSATS_FLOW,
    check_balance_request,
)
from v4vapp_backend_v2.process.process_overwatch import FlowEvent, FlowStage, FlowStatus, Overwatch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2026, 4, 12, 10, 20, 23, tzinfo=timezone.utc)


def _op_event(
    op_type: str,
    group_id: str = "gid_trigger",
    short_id: str = "3306_06d160_1",
    op: object | None = None,
) -> FlowEvent:
    return FlowEvent(
        event_type="op",
        timestamp=_TS,
        group_id=group_id,
        short_id=short_id,
        op_type=op_type,
        op=op,
        group="primary",
    )


def _ledger_event(
    ledger_type: LedgerType,
    group_id: str = "gid_trigger",
    short_id: str = "3306_06d160_1",
) -> FlowEvent:
    return FlowEvent(
        event_type="ledger",
        timestamp=_TS,
        group_id=group_id,
        short_id=short_id,
        ledger_type=ledger_type,
        group="primary",
    )


def _fake_op(
    op_type: str = "transfer",
    group_id: str = "gid_trigger",
    short_id: str = "3306_06d160_1",
    balance_request: bool = False,
) -> object:
    return type(
        "FakeOp",
        (),
        {
            "group_id": group_id,
            "short_id": short_id,
            "op_type": op_type,
            "from_account": "v4vapp-test",
            "balance_request": balance_request,
        },
    )()


def _register_transfer_flows() -> Overwatch:
    """Register all transfer-triggered flow definitions."""
    Overwatch.reset()
    ow = Overwatch()
    Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
    Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
    Overwatch.register_flow(HIVE_TRANSFER_PAYWITHSATS_FLOW)
    Overwatch.register_flow(HIVE_TRANSFER_FAILURE_FLOW)
    Overwatch.register_flow(BALANCE_REQUEST_FLOW)
    Overwatch._loaded_from_redis = True
    return ow


# ---------------------------------------------------------------------------
# Tests: FlowDefinition
# ---------------------------------------------------------------------------


class TestBalanceRequestDefinition:
    def test_definition_exists(self):
        assert BALANCE_REQUEST_FLOW.name == "balance_request"
        assert BALANCE_REQUEST_FLOW.trigger_op_type == "transfer"

    def test_has_4_required_stages(self):
        assert len(BALANCE_REQUEST_FLOW.required_stages) == 4

    def test_has_4_total_stages(self):
        assert len(BALANCE_REQUEST_FLOW.stages) == 4

    def test_stage_names(self):
        names = BALANCE_REQUEST_FLOW.stage_names
        assert names == [
            "trigger_transfer",
            "customer_hive_in",
            "balance_reply_transfer_op",
            "customer_hive_out",
        ]

    def test_no_optional_stages(self):
        optional = [s for s in BALANCE_REQUEST_FLOW.stages if not s.required]
        assert len(optional) == 0

    def test_same_trigger_as_hive_to_keepsats(self):
        assert BALANCE_REQUEST_FLOW.trigger_op_type == HIVE_TO_KEEPSATS_FLOW.trigger_op_type

    def test_fewer_required_stages_than_hive_to_keepsats(self):
        assert len(BALANCE_REQUEST_FLOW.required_stages) < len(
            HIVE_TO_KEEPSATS_FLOW.required_stages
        )

    def test_is_subset_of_hive_to_keepsats(self):
        """Balance request stages are all present in hive_to_keepsats."""
        br_sigs = {(s.event_type, s.op_type, s.ledger_type) for s in BALANCE_REQUEST_FLOW.stages}
        h2k_sigs = {(s.event_type, s.op_type, s.ledger_type) for s in HIVE_TO_KEEPSATS_FLOW.stages}
        assert br_sigs.issubset(h2k_sigs)


# ---------------------------------------------------------------------------
# Tests: Overwatch integration
# ---------------------------------------------------------------------------


class TestBalanceRequestOverwatch:
    @pytest.mark.asyncio
    async def test_transfer_creates_five_candidates(self):
        """A balance-request transfer trigger creates all five transfer-triggered flows."""
        ow = _register_transfer_flows()
        fake = _fake_op(balance_request=True)
        event = _op_event("transfer", op=fake)
        await ow._try_create_flow(event, fake)
        assert len(ow.active_flows) == 5
        names = {f.flow_definition.name for f in ow.active_flows}
        assert names == {
            "hive_to_keepsats",
            "hive_to_keepsats_external",
            "hive_transfer_paywithsats",
            "hive_transfer_failure",
            "balance_request",
        }

    @pytest.mark.asyncio
    async def test_balance_request_completes_with_reply(self):
        """Balance request flow completes when the reply transfer and
        cust_h_out arrive."""
        ow = _register_transfer_flows()
        fake = _fake_op(balance_request=True)
        trigger = _op_event("transfer", op=fake)
        await ow._try_create_flow(trigger, fake)

        # cust_h_in (primary)
        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        # Reply transfer op (different short_id)
        await ow._dispatch(_op_event("transfer", group_id="gid_reply", short_id="3312_c5c697_1"))
        # cust_h_out ledger (reply)
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOMER_HIVE_OUT,
                group_id="gid_reply",
                short_id="3312_c5c697_1",
            )
        )

        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "balance_request" in completed_names

    @pytest.mark.asyncio
    async def test_sibling_candidates_cancelled_immediately_on_balance_request_complete(self):
        """When balance_request completes, hive_to_keepsats and
        hive_to_keepsats_external should be cancelled immediately (not kept in
        a superset grace period).

        balance_request has an event_filter on its trigger stage; that makes
        its stage signature distinct from the unfiltered trigger in
        hive_to_keepsats/hive_to_keepsats_external.  _resolve_candidates
        therefore does not treat them as supersets and removes them right away.
        """
        ow = _register_transfer_flows()
        fake = _fake_op(balance_request=True)
        trigger = _op_event("transfer", op=fake)
        await ow._try_create_flow(trigger, fake)

        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        await ow._dispatch(_op_event("transfer", group_id="gid_reply", short_id="3312_c5c697_1"))
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOMER_HIVE_OUT,
                group_id="gid_reply",
                short_id="3312_c5c697_1",
            )
        )

        # balance_request completed; sibling flows must be immediately removed
        active_names = {f.flow_definition.name for f in ow.active_flows}
        assert "hive_to_keepsats" not in active_names
        assert "hive_to_keepsats_external" not in active_names
        # and they must not be stuck in a superset grace period either
        superset_flows = [f for f in ow.active_flows if f.superset_grace_expires is not None]
        assert not superset_flows

    @pytest.mark.asyncio
    async def test_progress_reporting(self):
        """Progress should report correctly at each stage."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(BALANCE_REQUEST_FLOW)
        Overwatch._loaded_from_redis = True

        fake = _fake_op(balance_request=True)
        trigger = _op_event("transfer", op=fake)
        await ow._try_create_flow(trigger, fake)

        flow = ow.active_flows[0]
        assert flow.progress == "1/4 required stages complete"

        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        assert flow.progress == "2/4 required stages complete"

        await ow._dispatch(_op_event("transfer", group_id="gid_reply", short_id="3312_c5c697_1"))
        assert flow.progress == "3/4 required stages complete"

        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOMER_HIVE_OUT,
                group_id="gid_reply",
                short_id="3312_c5c697_1",
            )
        )
        assert flow.status == FlowStatus.COMPLETED
        assert flow.progress == "4/4 required stages complete"

    @pytest.mark.asyncio
    async def test_single_flow_no_superset_conflict(self):
        """When only balance_request is registered, no superset resolution
        is needed."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(BALANCE_REQUEST_FLOW)
        Overwatch._loaded_from_redis = True

        fake = _fake_op(balance_request=True)
        trigger = _op_event("transfer", op=fake)
        await ow._try_create_flow(trigger, fake)
        assert len(ow.active_flows) == 1

        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        await ow._dispatch(_op_event("transfer", group_id="gid_reply", short_id="3312_c5c697_1"))
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOMER_HIVE_OUT,
                group_id="gid_reply",
                short_id="3312_c5c697_1",
            )
        )

        assert len(ow.completed_flows) == 1
        assert len(ow.active_flows) == 0
        assert ow.completed_flows[0].flow_definition.name == "balance_request"


# ---------------------------------------------------------------------------
# Tests: cancel_flows_for_trigger (untracked account scenario)
# ---------------------------------------------------------------------------


class TestCancelFlowsForTrigger:
    @pytest.mark.asyncio
    async def test_cancel_all_candidates_for_trigger(self):
        """When processing produces no ledger entries (untracked accounts),
        cancel_flows_for_trigger removes all candidate flows."""
        ow = _register_transfer_flows()
        fake = _fake_op(balance_request=True)
        trigger = _op_event("transfer", op=fake)
        await ow._try_create_flow(trigger, fake)
        assert len(ow.active_flows) == 5

        cancelled = await ow.cancel_flows_for_trigger("gid_trigger")
        assert cancelled == 5
        assert len(ow.active_flows) == 0
        assert len(ow.completed_flows) == 0

    @pytest.mark.asyncio
    async def test_cancel_does_not_affect_other_triggers(self):
        """Only flows matching the cancelled trigger_group_id are removed."""
        ow = _register_transfer_flows()

        # Two separate triggers (both balance_request=True so all 4 defs match)
        fake_a = _fake_op(group_id="gid_a", short_id="aaaa_aaaaaa_1", balance_request=True)
        await ow._try_create_flow(
            _op_event("transfer", group_id="gid_a", short_id="aaaa_aaaaaa_1", op=fake_a),
            fake_a,
        )
        fake_b = _fake_op(group_id="gid_b", short_id="bbbb_bbbbbb_1", balance_request=True)
        await ow._try_create_flow(
            _op_event("transfer", group_id="gid_b", short_id="bbbb_bbbbbb_1", op=fake_b),
            fake_b,
        )
        assert len(ow.active_flows) == 10  # 5 per trigger

        cancelled = await ow.cancel_flows_for_trigger("gid_a")
        assert cancelled == 5
        assert len(ow.active_flows) == 5
        remaining_triggers = {f.trigger_group_id for f in ow.active_flows}
        assert remaining_triggers == {"gid_b"}

    @pytest.mark.asyncio
    async def test_cancel_returns_zero_for_unknown_trigger(self):
        """Cancelling a non-existent trigger is a no-op."""
        ow = _register_transfer_flows()
        cancelled = await ow.cancel_flows_for_trigger("gid_nonexistent")
        assert cancelled == 0

    @pytest.mark.asyncio
    async def test_cancel_skips_completed_flows(self):
        """Completed flows are not affected by cancel."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(BALANCE_REQUEST_FLOW)
        Overwatch._loaded_from_redis = True

        fake = _fake_op(balance_request=True)
        trigger = _op_event("transfer", op=fake)
        await ow._try_create_flow(trigger, fake)
        assert len(ow.active_flows) == 1

        # Complete the flow
        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        await ow._dispatch(_op_event("transfer", group_id="gid_reply", short_id="3312_c5c697_1"))
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOMER_HIVE_OUT,
                group_id="gid_reply",
                short_id="3312_c5c697_1",
            )
        )
        assert len(ow.completed_flows) == 1

        # Cancel should not touch the completed flow
        cancelled = await ow.cancel_flows_for_trigger("gid_trigger")
        assert cancelled == 0
        assert len(ow.completed_flows) == 1


# ---------------------------------------------------------------------------
# Tests: event_filter on FlowStage
# ---------------------------------------------------------------------------


class TestEventFilter:
    def test_trigger_stage_has_event_filter(self):
        """The trigger_transfer stage of BALANCE_REQUEST_FLOW should have
        the check_balance_request event_filter."""
        trigger_stage = BALANCE_REQUEST_FLOW.stages[0]
        assert trigger_stage.name == "trigger_transfer"
        assert trigger_stage.event_filter is check_balance_request

    def test_other_stages_have_no_filter(self):
        """Non-trigger stages should not have an event_filter."""
        for stage in BALANCE_REQUEST_FLOW.stages[1:]:
            assert stage.event_filter is None

    def test_check_balance_request_true(self):
        """check_balance_request returns True when op.balance_request is True."""
        fake = _fake_op(balance_request=True)
        event = _op_event("transfer", op=fake)
        assert check_balance_request(event) is True

    def test_check_balance_request_false(self):
        """check_balance_request returns False when op.balance_request is False."""
        fake = _fake_op(balance_request=False)
        event = _op_event("transfer", op=fake)
        assert check_balance_request(event) is False

    def test_check_balance_request_no_op(self):
        """check_balance_request returns False when event.op is None."""
        event = _op_event("transfer")  # op defaults to None
        assert check_balance_request(event) is False

    def test_stage_matches_with_filter_pass(self):
        """FlowStage.matches returns True when structural + filter both pass."""
        stage = BALANCE_REQUEST_FLOW.stages[0]
        fake = _fake_op(balance_request=True)
        event = _op_event("transfer", op=fake)
        assert stage.matches(event) is True

    def test_stage_matches_with_filter_reject(self):
        """FlowStage.matches returns False when structural passes but filter rejects."""
        stage = BALANCE_REQUEST_FLOW.stages[0]
        fake = _fake_op(balance_request=False)
        event = _op_event("transfer", op=fake)
        assert stage.matches(event) is False

    def test_stage_without_filter_still_matches(self):
        """A FlowStage with no event_filter matches on structural criteria alone."""
        stage = FlowStage(name="plain", event_type="op", op_type="transfer")
        event = _op_event("transfer")
        assert stage.matches(event) is True

    def test_event_filter_exception_returns_false(self):
        """If event_filter raises, matches() returns False."""

        def _bad_filter(event: FlowEvent) -> bool:
            raise ValueError("boom")

        stage = FlowStage(
            name="bad", event_type="op", op_type="transfer", event_filter=_bad_filter
        )
        event = _op_event("transfer")
        assert stage.matches(event) is False

    @pytest.mark.asyncio
    async def test_non_balance_request_creates_four_candidates(self):
        """A non-balance-request transfer creates 4 candidates
        (balance_request flow is filtered out, but failure flow is not)."""
        ow = _register_transfer_flows()
        fake = _fake_op(balance_request=False)
        event = _op_event("transfer", op=fake)
        await ow._try_create_flow(event, fake)
        assert len(ow.active_flows) == 4
        names = {f.flow_definition.name for f in ow.active_flows}
        assert "balance_request" not in names
        assert "hive_transfer_failure" in names

    def test_event_filter_excluded_from_serialization(self):
        """event_filter should not appear in model_dump (excluded from Redis)."""
        stage = BALANCE_REQUEST_FLOW.stages[0]
        dumped = stage.model_dump()
        assert "event_filter" not in dumped


# ---------------------------------------------------------------------------
# Tests: late-arrival ledger event buffering
# ---------------------------------------------------------------------------


class TestPendingLedgerEventBuffer:
    """Verify that ledger events arriving BEFORE the trigger op are buffered
    and replayed when the flow is eventually created.

    Regression for: keepsats_to_hive flows stalling because their ledger
    entries (written by an earlier processing run) arrive at the change stream
    BEFORE the trigger custom_json change event.
    """

    @pytest.mark.asyncio
    async def test_buffered_ledger_replayed_on_flow_creation(self):
        """Ledger event ingested before flow exists is replayed when created."""
        ow = Overwatch()
        Overwatch.reset()
        Overwatch.register_flow(BALANCE_REQUEST_FLOW)
        Overwatch._loaded_from_redis = True

        sid = "3306_06d160_1"
        gid = "gid_trigger"

        # Directly inject a ledger event into the buffer (simulating an event
        # that arrived before the flow existed — same as what ingest_ledger_entry
        # does when _dispatch returns None).
        ledger_evt = _ledger_event(LedgerType.CUSTOMER_HIVE_IN, group_id=gid, short_id=sid)
        Overwatch._pending_ledger_events[sid] = [(ledger_evt, datetime.now(tz=timezone.utc))]

        # Now the trigger op arrives and creates the flow
        fake = _fake_op(balance_request=True, group_id=gid, short_id=sid)
        trigger = _op_event("transfer", group_id=gid, short_id=sid, op=fake)
        await ow._try_create_flow(trigger, fake)

        # The buffer should have been drained …
        assert sid not in Overwatch._pending_ledger_events
        # … and the flow should now show the ledger stage as matched
        flows = [f for f in ow.active_flows if f.trigger_short_id == sid]
        assert flows, "Expected at least one active flow for the trigger"
        matched_types = {e.ledger_type for e in flows[0].events if e.event_type == "ledger"}
        assert LedgerType.CUSTOMER_HIVE_IN in matched_types

    @pytest.mark.asyncio
    async def test_buffer_not_replayed_for_different_short_id(self):
        """Buffered event for short_id A is not replayed into a flow for B."""
        ow = Overwatch()
        Overwatch.reset()
        Overwatch.register_flow(BALANCE_REQUEST_FLOW)
        Overwatch._loaded_from_redis = True

        sid_a = "aaaa_aaaaaa_1"
        sid_b = "bbbb_bbbbbb_1"

        ledger_evt_a = _ledger_event(LedgerType.CUSTOMER_HIVE_IN, group_id=sid_a, short_id=sid_a)
        Overwatch._pending_ledger_events[sid_a] = [(ledger_evt_a, datetime.now(tz=timezone.utc))]

        # Flow created for sid_b — should NOT drain sid_a buffer
        fake_b = _fake_op(balance_request=True, group_id=sid_b, short_id=sid_b)
        trigger_b = _op_event("transfer", group_id=sid_b, short_id=sid_b, op=fake_b)
        await ow._try_create_flow(trigger_b, fake_b)

        # sid_a buffer should still be intact
        assert sid_a in Overwatch._pending_ledger_events
        # sid_b buffer should never have been populated
        assert sid_b not in Overwatch._pending_ledger_events

    @pytest.mark.asyncio
    async def test_expired_buffer_not_replayed(self):
        """Buffer entries older than TTL are silently dropped on replay."""
        from datetime import timedelta

        ow = Overwatch()
        Overwatch.reset()
        Overwatch.register_flow(BALANCE_REQUEST_FLOW)
        Overwatch._loaded_from_redis = True

        sid = "3306_06d160_1"
        gid = "gid_trigger"

        # Manually inject an already-expired entry
        old_ts = datetime.now(tz=timezone.utc) - timedelta(minutes=10)
        old_event = _ledger_event(LedgerType.CUSTOMER_HIVE_IN, group_id=gid, short_id=sid)
        Overwatch._pending_ledger_events[sid] = [(old_event, old_ts)]

        fake = _fake_op(balance_request=True, group_id=gid, short_id=sid)
        trigger = _op_event("transfer", group_id=gid, short_id=sid, op=fake)
        await ow._try_create_flow(trigger, fake)

        # Buffer cleared (expired entries dropped), flow created but no ledger
        # stage matched from the stale event
        assert sid not in Overwatch._pending_ledger_events
        flows = [f for f in ow.active_flows if f.trigger_short_id == sid]
        assert flows
        ledger_events = [e for e in flows[0].events if e.event_type == "ledger"]
        assert ledger_events == []
