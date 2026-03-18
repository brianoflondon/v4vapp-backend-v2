"""
Tests for the Keepsats Internal Transfer flow and the late-event time window.

Flow: customer sends custom_json to transfer sats from their keepsats to
another customer → c_j_trans ledger → optional notification custom_json.

Also tests that the _dispatch second pass (late-event absorption) only
considers flows that completed within the _LATE_EVENT_WINDOW, preventing
unrelated events from being greedily absorbed by old completed flows.
"""

from datetime import datetime, timedelta, timezone

import pytest

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.process.overwatch_flows import (
    EXTERNAL_TO_KEEPSATS_FLOW,
    HIVE_TO_KEEPSATS_EXTERNAL_FLOW,
    HIVE_TO_KEEPSATS_FLOW,
    KEEPSATS_INTERNAL_TRANSFER_FLOW,
    KEEPSATS_TO_EXTERNAL_FLOW,
    KEEPSATS_TO_HBD_FLOW,
)
from v4vapp_backend_v2.process.process_overwatch import FlowEvent, Overwatch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2026, 3, 17, 18, 27, 24, tzinfo=timezone.utc)


def _op_event(
    op_type: str,
    group_id: str = "gid_trigger",
    short_id: str = "3688_2320ec_1",
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
    short_id: str = "3688_2320ec_1",
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
    op_type: str = "custom_json",
    group_id: str = "gid_trigger",
    short_id: str = "3688_2320ec_1",
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


def _register_all() -> Overwatch:
    Overwatch.reset()
    ow = Overwatch()
    Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
    Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
    Overwatch.register_flow(KEEPSATS_TO_HBD_FLOW)
    Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
    Overwatch.register_flow(EXTERNAL_TO_KEEPSATS_FLOW)
    Overwatch.register_flow(KEEPSATS_INTERNAL_TRANSFER_FLOW)
    Overwatch._loaded_from_redis = True
    return ow


# ---------------------------------------------------------------------------
# Tests: FlowDefinition
# ---------------------------------------------------------------------------


class TestKeepsatsInternalTransferDefinition:
    def test_definition_exists(self):
        assert KEEPSATS_INTERNAL_TRANSFER_FLOW.name == "keepsats_internal_transfer"
        assert KEEPSATS_INTERNAL_TRANSFER_FLOW.trigger_op_type == "custom_json"

    def test_has_2_required_stages(self):
        assert len(KEEPSATS_INTERNAL_TRANSFER_FLOW.required_stages) == 2

    def test_has_3_total_stages(self):
        assert len(KEEPSATS_INTERNAL_TRANSFER_FLOW.stages) == 3

    def test_stage_names(self):
        names = KEEPSATS_INTERNAL_TRANSFER_FLOW.stage_names
        assert "trigger_custom_json" in names
        assert "custom_json_transfer" in names
        assert "notification_custom_json_op" in names

    def test_notification_is_optional(self):
        optional = [s for s in KEEPSATS_INTERNAL_TRANSFER_FLOW.stages if not s.required]
        assert len(optional) == 1
        assert optional[0].name == "notification_custom_json_op"


# ---------------------------------------------------------------------------
# Tests: Overwatch integration
# ---------------------------------------------------------------------------


class TestKeepsatsInternalTransferOverwatch:
    @pytest.mark.asyncio
    async def test_custom_json_creates_three_candidates(self):
        """A custom_json trigger should create keepsats_to_hbd,
        keepsats_to_external, and keepsats_internal_transfer."""
        ow = _register_all()
        event = _op_event("custom_json")
        await ow._try_create_flow(event, _fake_op())
        names = {f.flow_definition.name for f in ow.active_flows}
        assert names == {
            "keepsats_to_hbd",
            "keepsats_to_external",
            "keepsats_internal_transfer",
        }

    @pytest.mark.asyncio
    async def test_internal_transfer_completes_with_c_j_trans(self):
        """Internal transfer completes when custom_json + c_j_trans arrive."""
        ow = _register_all()
        trigger = _op_event("custom_json")
        await ow._try_create_flow(trigger, _fake_op())

        await ow._dispatch(_ledger_event(LedgerType.CUSTOM_JSON_TRANSFER))

        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "keepsats_internal_transfer" in completed_names

    @pytest.mark.asyncio
    async def test_other_candidates_removed_on_completion(self):
        """When keepsats_internal_transfer completes, the other custom_json
        candidates should be removed (their events are all explainable)."""
        ow = _register_all()
        trigger = _op_event("custom_json")
        await ow._try_create_flow(trigger, _fake_op())

        await ow._dispatch(_ledger_event(LedgerType.CUSTOM_JSON_TRANSFER))

        # Only the completed flow should remain
        assert len(ow.active_flows) == 0
        assert len(ow.completed_flows) == 1
        assert ow.completed_flows[0].flow_definition.name == "keepsats_internal_transfer"

    @pytest.mark.asyncio
    async def test_notification_absorbed_after_completion(self):
        """Late notification custom_json is absorbed by the recently-completed
        internal transfer flow."""
        ow = _register_all()
        trigger = _op_event("custom_json")
        await ow._try_create_flow(trigger, _fake_op())

        await ow._dispatch(_ledger_event(LedgerType.CUSTOM_JSON_TRANSFER))
        assert len(ow.completed_flows) == 1

        # Notification arrives shortly after — absorbed as late event
        notif = _op_event("custom_json", group_id="gid_notif", short_id="3690_5a7a2f_1")
        result = await ow._dispatch(notif)
        assert result is not None
        assert len(ow.completed_flows[0].events) == 3


# ---------------------------------------------------------------------------
# Tests: Late-event time window
# ---------------------------------------------------------------------------


class TestLateEventTimeWindow:
    """Verify that the second pass only absorbs events into recently-completed
    flows, not old ones from unrelated transactions."""

    @pytest.mark.asyncio
    async def test_recent_completion_absorbs_late_event(self):
        """A flow that completed < 120s ago absorbs a late event."""
        ow = _register_all()

        # Create and complete an external_to_keepsats flow
        inv_trigger = _op_event("invoice", group_id="gid_inv", short_id="inv_hash")
        await ow._try_create_flow(
            inv_trigger,
            _fake_op("invoice", "gid_inv", "inv_hash"),
        )
        await ow._dispatch(
            _ledger_event(LedgerType.DEPOSIT_LIGHTNING, group_id="gid_inv", short_id="inv_hash")
        )
        await ow._dispatch(_op_event("custom_json", group_id="gid_ks", short_id="ks_notif"))
        await ow._dispatch(
            _ledger_event(LedgerType.RECEIVE_LIGHTNING, group_id="gid_ks", short_id="ks_notif")
        )
        assert len(ow.completed_flows) == 1

        # Force completed_at to "just now" so it's within the window
        ow.completed_flows[0].completed_at = datetime.now(tz=timezone.utc)

        # Late HIVE notification — should be absorbed
        late_transfer = _op_event("transfer", group_id="gid_hive", short_id="hive_notif")
        result = await ow._dispatch(late_transfer)
        assert result is not None  # absorbed

    @pytest.mark.asyncio
    async def test_old_completion_does_not_absorb(self):
        """A flow that completed > 120s ago does NOT absorb a new event."""
        ow = _register_all()

        # Create and complete an external_to_keepsats flow
        inv_trigger = _op_event("invoice", group_id="gid_inv", short_id="inv_hash")
        await ow._try_create_flow(
            inv_trigger,
            _fake_op("invoice", "gid_inv", "inv_hash"),
        )
        await ow._dispatch(
            _ledger_event(LedgerType.DEPOSIT_LIGHTNING, group_id="gid_inv", short_id="inv_hash")
        )
        await ow._dispatch(_op_event("custom_json", group_id="gid_ks", short_id="ks_notif"))
        await ow._dispatch(
            _ledger_event(LedgerType.RECEIVE_LIGHTNING, group_id="gid_ks", short_id="ks_notif")
        )
        assert len(ow.completed_flows) == 1

        # Force completed_at to 5 minutes ago — outside the window
        ow.completed_flows[0].completed_at = datetime.now(tz=timezone.utc) - timedelta(minutes=5)

        # Unrelated custom_json arrives — should NOT be absorbed
        unrelated = _op_event("custom_json", group_id="gid_unrelated", short_id="3688_2320ec_1")
        result = await ow._dispatch(unrelated)
        assert result is None  # not absorbed, falls through

    @pytest.mark.asyncio
    async def test_old_completed_flow_does_not_steal_new_trigger(self):
        """An old completed external_to_keepsats flow should not absorb
        a custom_json that is actually a new internal transfer trigger."""
        ow = _register_all()

        # Complete an external_to_keepsats flow
        inv_trigger = _op_event("invoice", group_id="gid_inv", short_id="inv_hash")
        await ow._try_create_flow(
            inv_trigger,
            _fake_op("invoice", "gid_inv", "inv_hash"),
        )
        await ow._dispatch(
            _ledger_event(LedgerType.DEPOSIT_LIGHTNING, group_id="gid_inv", short_id="inv_hash")
        )
        await ow._dispatch(_op_event("custom_json", group_id="gid_ks", short_id="ks_notif"))
        await ow._dispatch(
            _ledger_event(LedgerType.RECEIVE_LIGHTNING, group_id="gid_ks", short_id="ks_notif")
        )
        assert len(ow.completed_flows) == 1

        # Age the completed flow past the window
        ow.completed_flows[0].completed_at = datetime.now(tz=timezone.utc) - timedelta(minutes=5)

        # New internal transfer arrives — should NOT be absorbed
        new_trigger = _op_event("custom_json", group_id="gid_new", short_id="3688_2320ec_1")
        result = await ow._dispatch(new_trigger)
        assert result is None  # not absorbed

        # So _try_create_flow creates candidates
        result = await ow._try_create_flow(
            new_trigger, _fake_op("custom_json", "gid_new", "3688_2320ec_1")
        )
        names = {f.flow_definition.name for f in ow.active_flows}
        assert "keepsats_internal_transfer" in names

        # Complete the internal transfer
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOM_JSON_TRANSFER,
                group_id="gid_new",
                short_id="3688_2320ec_1",
            )
        )
        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "keepsats_internal_transfer" in completed_names
