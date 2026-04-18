"""
Tests for the External-to-Hive flow and superset resolution.

The external_to_hive flow is a superset of external_to_keepsats — it
includes all the same stages but the HIVE notification transfer and
CUSTOMER_HIVE_OUT are required (not optional).  Tests verify that:
  - Both candidates are created on an invoice trigger
  - The superset flow survives when external_to_keepsats completes first
  - The superset flow is correctly removed for simple deposits (no HIVE payout)
  - Both flows can complete independently when HIVE payout events arrive
"""

from datetime import datetime, timedelta, timezone

import pytest

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.process.overwatch_flows import (
    EXTERNAL_TO_HIVE_FLOW,
    EXTERNAL_TO_HIVE_LOOPBACK_FLOW,
    EXTERNAL_TO_KEEPSATS_FLOW,
    EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW,
    HIVE_TO_KEEPSATS_EXTERNAL_FLOW,
    HIVE_TO_KEEPSATS_FLOW,
    HIVE_TRANSFER_FAILURE_FLOW,
    HIVE_TRANSFER_PAYWITHSATS_FLOW,
    KEEPSATS_INTERNAL_TRANSFER_FLOW,
    KEEPSATS_TO_EXTERNAL_FLOW,
    KEEPSATS_TO_HIVE_FLOW,
)
from v4vapp_backend_v2.process.process_overwatch import FlowEvent, FlowStatus, Overwatch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2026, 3, 19, 9, 22, 56, tzinfo=timezone.utc)


def _op_event(
    op_type: str,
    group_id: str = "gid_invoice",
    short_id: str = "B9XaNJm/x4",
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
    group_id: str = "gid_invoice",
    short_id: str = "B9XaNJm/x4",
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
    op_type: str = "invoice",
    group_id: str = "gid_invoice",
    short_id: str = "B9XaNJm/x4",
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


# Shared events for both external_to_keepsats and external_to_hive
_SHARED_EVENTS = [
    lambda: _op_event("invoice"),
    lambda: _ledger_event(LedgerType.DEPOSIT_LIGHTNING),
    lambda: _op_event("custom_json", group_id="gid_notif", short_id="2688_15bf34_1"),
    lambda: _ledger_event(
        LedgerType.RECEIVE_LIGHTNING, group_id="gid_notif", short_id="2688_15bf34_1"
    ),
]

# HIVE payout events (only in external_to_hive as required)
_HIVE_PAYOUT_EVENTS = [
    lambda: _op_event("transfer", group_id="gid_hive", short_id="2692_f8f57d_1"),
    lambda: _ledger_event(
        LedgerType.CUSTOMER_HIVE_OUT, group_id="gid_hive", short_id="2692_f8f57d_1"
    ),
]


# ---------------------------------------------------------------------------
# Tests: FlowDefinition
# ---------------------------------------------------------------------------


class TestExternalToHiveDefinition:
    """Tests for the EXTERNAL_TO_HIVE_FLOW definition."""

    def test_definition_exists(self):
        assert EXTERNAL_TO_HIVE_FLOW.name == "external_to_hive"
        assert EXTERNAL_TO_HIVE_FLOW.trigger_op_type == "invoice"

    def test_has_6_required_stages(self):
        assert len(EXTERNAL_TO_HIVE_FLOW.required_stages) == 6

    def test_has_7_total_stages(self):
        assert len(EXTERNAL_TO_HIVE_FLOW.stages) == 7

    def test_includes_all_external_to_keepsats_stages(self):
        base_names = set(EXTERNAL_TO_KEEPSATS_FLOW.stage_names)
        superset_names = set(EXTERNAL_TO_HIVE_FLOW.stage_names)
        assert base_names.issubset(superset_names)

    def test_has_required_hive_payout_stages(self):
        required_names = {s.name for s in EXTERNAL_TO_HIVE_FLOW.required_stages}
        assert "hive_notification_transfer_op" in required_names
        assert "customer_hive_out" in required_names

    def test_hive_payout_optional_in_keepsats_required_in_hive(self):
        """The hive notification stages are optional in external_to_keepsats
        but required in external_to_hive."""
        keepsats_optional = {s.name for s in EXTERNAL_TO_KEEPSATS_FLOW.stages if not s.required}
        hive_required = {s.name for s in EXTERNAL_TO_HIVE_FLOW.required_stages}
        assert "hive_notification_transfer_op" in keepsats_optional
        assert "hive_notification_transfer_op" in hive_required
        assert "customer_hive_out" in keepsats_optional
        assert "customer_hive_out" in hive_required

    def test_same_trigger_as_external_to_keepsats(self):
        assert EXTERNAL_TO_HIVE_FLOW.trigger_op_type == EXTERNAL_TO_KEEPSATS_FLOW.trigger_op_type

    def test_optional_stages(self):
        optional = [s for s in EXTERNAL_TO_HIVE_FLOW.stages if not s.required]
        optional_names = {s.name for s in optional}
        assert optional_names == {"small_notification_custom_json_op"}


# ---------------------------------------------------------------------------
# Tests: Superset candidate resolution
# ---------------------------------------------------------------------------


class TestExternalToHiveSuperset:
    """Verify that resolve logic correctly handles the superset relationship."""

    def _register_all(self):
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch.register_flow(KEEPSATS_TO_HIVE_FLOW)
        Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
        Overwatch.register_flow(EXTERNAL_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(EXTERNAL_TO_HIVE_FLOW)
        Overwatch.register_flow(EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW)
        Overwatch.register_flow(EXTERNAL_TO_HIVE_LOOPBACK_FLOW)
        Overwatch.register_flow(KEEPSATS_INTERNAL_TRANSFER_FLOW)
        Overwatch.register_flow(HIVE_TRANSFER_PAYWITHSATS_FLOW)
        Overwatch.register_flow(HIVE_TRANSFER_FAILURE_FLOW)
        Overwatch._loaded_from_redis = True
        return ow

    @pytest.mark.asyncio
    async def test_invoice_creates_two_candidates(self):
        """An invoice trigger should create external_to_keepsats,
        external_to_hive, and their loopback variants."""
        ow = self._register_all()
        event = _op_event("invoice")
        await ow._try_create_flow(event, _fake_op())
        assert len(ow.active_flows) == 4
        names = {f.flow_definition.name for f in ow.active_flows}
        assert names == {
            "external_to_keepsats",
            "external_to_hive",
            "external_to_keepsats_loopback",
            "external_to_hive_loopback",
        }

    @pytest.mark.asyncio
    async def test_keepsats_completes_first_superset_kept(self):
        """When external_to_keepsats completes at 4/4, external_to_hive
        (the superset) should be kept alive."""
        ow = self._register_all()
        event = _op_event("invoice")
        await ow._try_create_flow(event, _fake_op())

        # Feed shared events to complete external_to_keepsats
        for factory in _SHARED_EVENTS[1:]:  # skip trigger (already added)
            await ow._dispatch(factory())

        # external_to_keepsats completed, external_to_hive still active
        # (loopback keepsats also completed)
        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "external_to_keepsats" in completed_names
        active_names = {f.flow_definition.name for f in ow.active_flows}
        assert "external_to_hive" in active_names

    @pytest.mark.asyncio
    async def test_both_complete_with_hive_payout(self):
        """Both flows complete when all events including HIVE payout arrive."""
        ow = self._register_all()
        event = _op_event("invoice")
        await ow._try_create_flow(event, _fake_op())

        # Feed shared events
        for factory in _SHARED_EVENTS[1:]:
            await ow._dispatch(factory())

        # external_to_keepsats already completed; feed HIVE payout
        for factory in _HIVE_PAYOUT_EVENTS:
            await ow._dispatch(factory())

        # All matching flows should be completed
        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "external_to_keepsats" in completed_names
        assert "external_to_hive" in completed_names
        assert len(ow.active_flows) == 0

    @pytest.mark.asyncio
    async def test_external_to_hive_progress(self):
        """Track progress of external_to_hive through the flow."""
        ow = self._register_all()
        event = _op_event("invoice")
        await ow._try_create_flow(event, _fake_op())

        # Find external_to_hive candidate
        hive_flow = next(
            f for f in ow.active_flows if f.flow_definition.name == "external_to_hive"
        )
        assert hive_flow.progress == "1/6 required stages complete"

        # Feed shared events
        for factory in _SHARED_EVENTS[1:]:
            await ow._dispatch(factory())

        # After shared events: 4/6 required
        hive_flow = next(
            (f for f in ow.active_flows if f.flow_definition.name == "external_to_hive"),
            None,
        )
        assert hive_flow is not None
        assert hive_flow.progress == "4/6 required stages complete"

        # Feed HIVE payout events
        for factory in _HIVE_PAYOUT_EVENTS:
            await ow._dispatch(factory())

        # Now 6/6 completed
        hive_flow = next(
            f for f in ow.completed_flows if f.flow_definition.name == "external_to_hive"
        )
        assert hive_flow.progress == "6/6 required stages complete"
        assert hive_flow.status == FlowStatus.COMPLETED


# ---------------------------------------------------------------------------
# Tests: Superset grace period
# ---------------------------------------------------------------------------


class TestExternalToHiveGracePeriod:
    """Verify superset grace period behaviour for external_to_hive."""

    def _register_all(self):
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(EXTERNAL_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(EXTERNAL_TO_HIVE_FLOW)
        Overwatch.register_flow(EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW)
        Overwatch.register_flow(EXTERNAL_TO_HIVE_LOOPBACK_FLOW)
        Overwatch._loaded_from_redis = True
        return ow

    @pytest.mark.asyncio
    async def test_grace_set_when_keepsats_completes(self):
        """external_to_hive should get a superset_grace_expires when
        external_to_keepsats completes."""
        ow = self._register_all()
        await ow._try_create_flow(_op_event("invoice"), _fake_op())

        for factory in _SHARED_EVENTS[1:]:
            await ow._dispatch(factory())

        assert len(ow.completed_flows) >= 1
        hive_flow = next(
            f for f in ow.active_flows if f.flow_definition.name == "external_to_hive"
        )
        assert hive_flow.superset_grace_expires is not None

    @pytest.mark.asyncio
    async def test_grace_cancelled_after_expiry(self):
        """external_to_hive should be cancelled when grace period expires
        without distinguishing events."""
        ow = self._register_all()
        await ow._try_create_flow(_op_event("invoice"), _fake_op())

        for factory in _SHARED_EVENTS[1:]:
            await ow._dispatch(factory())

        hive_flow = next(
            f for f in ow.active_flows if f.flow_definition.name == "external_to_hive"
        )
        # Force grace to expire
        hive_flow.superset_grace_expires = datetime.now(tz=timezone.utc) - timedelta(seconds=1)

        await ow.check_stalls()
        assert len(ow.active_flows) == 0
        # The candidate was removed from flow_instances entirely
        remaining = [f for f in ow.flow_instances if f.flow_definition.name == "external_to_hive"]
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_grace_cleared_by_hive_payout(self):
        """Grace timer should be cleared when a HIVE payout event arrives."""
        ow = self._register_all()
        await ow._try_create_flow(_op_event("invoice"), _fake_op())

        for factory in _SHARED_EVENTS[1:]:
            await ow._dispatch(factory())

        hive_flow = next(
            f for f in ow.active_flows if f.flow_definition.name == "external_to_hive"
        )
        assert hive_flow.superset_grace_expires is not None

        # HIVE transfer arrives — grace should be cleared
        await ow._dispatch(_HIVE_PAYOUT_EVENTS[0]())
        hive_flow = next(
            f for f in ow.active_flows if f.flow_definition.name == "external_to_hive"
        )
        assert hive_flow.superset_grace_expires is None

    @pytest.mark.asyncio
    async def test_transfer_not_creating_false_hive_to_keepsats(self):
        """The HIVE payout transfer should be absorbed by active
        external_to_hive — NOT trigger hive_to_keepsats candidates."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
        Overwatch.register_flow(EXTERNAL_TO_KEEPSATS_FLOW)
        Overwatch.register_flow(EXTERNAL_TO_HIVE_FLOW)
        Overwatch.register_flow(EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW)
        Overwatch.register_flow(EXTERNAL_TO_HIVE_LOOPBACK_FLOW)
        Overwatch._loaded_from_redis = True

        await ow._try_create_flow(_op_event("invoice"), _fake_op())
        for factory in _SHARED_EVENTS[1:]:
            await ow._dispatch(factory())

        # external_to_keepsats completed, external_to_hive still active
        assert len(ow.completed_flows) >= 1

        # HIVE transfer arrives — should be absorbed by external_to_hive
        result = await ow._dispatch(_HIVE_PAYOUT_EVENTS[0]())
        assert result is not None

        # No hive_to_keepsats candidates created
        flow_names = {f.flow_definition.name for f in ow.flow_instances}
        assert "hive_to_keepsats" not in flow_names
        assert "hive_to_keepsats_external" not in flow_names
