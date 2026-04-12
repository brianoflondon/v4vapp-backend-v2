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
)
from v4vapp_backend_v2.process.process_overwatch import FlowEvent, FlowStatus, Overwatch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2026, 4, 12, 10, 20, 23, tzinfo=timezone.utc)


def _op_event(
    op_type: str,
    group_id: str = "gid_trigger",
    short_id: str = "3306_06d160_1",
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
) -> object:
    return type(
        "FakeOp",
        (),
        {
            "group_id": group_id,
            "short_id": short_id,
            "op_type": op_type,
            "from_account": "v4vapp-test",
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
        """A transfer trigger should create all four transfer-triggered flows."""
        ow = _register_transfer_flows()
        event = _op_event("transfer")
        await ow._try_create_flow(event, _fake_op())
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
        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())

        # cust_h_in (primary)
        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        # Reply transfer op (different short_id)
        await ow._dispatch(
            _op_event("transfer", group_id="gid_reply", short_id="3312_c5c697_1")
        )
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
        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())

        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        await ow._dispatch(
            _op_event("transfer", group_id="gid_reply", short_id="3312_c5c697_1")
        )
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOMER_HIVE_OUT,
                group_id="gid_reply",
                short_id="3312_c5c697_1",
            )
        )

        # balance_request completed; superset candidates have grace period
        superset_flows = [
            f
            for f in ow.active_flows
            if f.superset_grace_expires is not None
        ]
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

        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())

        flow = ow.active_flows[0]
        assert flow.progress == "1/4 required stages complete"

        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        assert flow.progress == "2/4 required stages complete"

        await ow._dispatch(
            _op_event("transfer", group_id="gid_reply", short_id="3312_c5c697_1")
        )
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

        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())
        assert len(ow.active_flows) == 1

        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        await ow._dispatch(
            _op_event("transfer", group_id="gid_reply", short_id="3312_c5c697_1")
        )
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
