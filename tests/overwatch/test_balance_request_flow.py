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
    """Register all four transfer-triggered flow definitions."""
    Overwatch.reset()
    ow = Overwatch()
    Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
    Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
    Overwatch.register_flow(HIVE_TRANSFER_PAYWITHSATS_FLOW)
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
    async def test_transfer_creates_four_candidates(self):
        """A balance-request transfer trigger creates all four transfer-triggered flows."""
        ow = _register_transfer_flows()
        fake = _fake_op(balance_request=True)
        event = _op_event("transfer", op=fake)
        await ow._try_create_flow(event, fake)
        assert len(ow.active_flows) == 4
        names = {f.flow_definition.name for f in ow.active_flows}
        assert names == {
            "hive_to_keepsats",
            "hive_to_keepsats_external",
            "hive_transfer_paywithsats",
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
    async def test_superset_candidates_kept_in_grace_period(self):
        """When balance_request completes, hive_to_keepsats and
        hive_to_keepsats_external should be kept as superset candidates
        (their stages are a superset of balance_request's stages)."""
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

        # balance_request completed; superset candidates have grace period
        superset_flows = [f for f in ow.active_flows if f.superset_grace_expires is not None]
        superset_names = {f.flow_definition.name for f in superset_flows}
        # hive_to_keepsats and hive_to_keepsats_external are proper supersets
        assert "hive_to_keepsats" in superset_names
        assert "hive_to_keepsats_external" in superset_names

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
        assert len(ow.active_flows) == 4

        cancelled = await ow.cancel_flows_for_trigger("gid_trigger")
        assert cancelled == 4
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
        assert len(ow.active_flows) == 8  # 4 per trigger

        cancelled = await ow.cancel_flows_for_trigger("gid_a")
        assert cancelled == 4
        assert len(ow.active_flows) == 4
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
    async def test_non_balance_request_creates_three_candidates(self):
        """A non-balance-request transfer only creates 3 candidates
        (balance_request flow is filtered out)."""
        ow = _register_transfer_flows()
        fake = _fake_op(balance_request=False)
        event = _op_event("transfer", op=fake)
        await ow._try_create_flow(event, fake)
        assert len(ow.active_flows) == 3
        names = {f.flow_definition.name for f in ow.active_flows}
        assert "balance_request" not in names

    def test_event_filter_excluded_from_serialization(self):
        """event_filter should not appear in model_dump (excluded from Redis)."""
        stage = BALANCE_REQUEST_FLOW.stages[0]
        dumped = stage.model_dump()
        assert "event_filter" not in dumped
