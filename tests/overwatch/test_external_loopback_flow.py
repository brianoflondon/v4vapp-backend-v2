"""
Tests for the External Loopback (self-payment) flow definitions.

When a keepsats-initiated outbound payment lands on the same LND node,
process_tracked_event completes without creating Lightning accounting
ledger entries (deposit_l / recv_l).  The loopback flow definitions
handle this scenario by omitting those stages.

Tests verify:
  - Flow definitions have the correct stage counts
  - Loopback keepsats completes with just trigger + keepsats notification
  - Loopback hive completes when HIVE payout events also arrive
  - Superset resolution keeps the loopback hive alive when loopback
    keepsats completes
  - Normal (non-loopback) flows are not broken by the loopback definitions
"""

from datetime import datetime, timedelta, timezone

import pytest

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.process.overwatch_flows import (
    EXTERNAL_TO_HIVE_FLOW,
    EXTERNAL_TO_HIVE_LOOPBACK_FLOW,
    EXTERNAL_TO_KEEPSATS_FLOW,
    EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW,
)
from v4vapp_backend_v2.process.process_overwatch import FlowEvent, Overwatch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2026, 4, 10, 8, 36, 51, tzinfo=timezone.utc)


def _op_event(
    op_type: str,
    group_id: str = "gid_invoice_loopback",
    short_id: str = "nadsXnCBUJ",
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
    group_id: str = "gid_invoice_loopback",
    short_id: str = "nadsXnCBUJ",
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
    group_id: str = "gid_invoice_loopback",
    short_id: str = "nadsXnCBUJ",
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
    Overwatch.register_flow(EXTERNAL_TO_KEEPSATS_FLOW)
    Overwatch.register_flow(EXTERNAL_TO_HIVE_FLOW)
    Overwatch.register_flow(EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW)
    Overwatch.register_flow(EXTERNAL_TO_HIVE_LOOPBACK_FLOW)
    Overwatch._loaded_from_redis = True
    return ow


# ---------------------------------------------------------------------------
# Tests: FlowDefinition — external_to_keepsats_loopback
# ---------------------------------------------------------------------------


class TestExternalToKeepsatsLoopbackDefinition:
    def test_definition_exists(self):
        assert EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW.name == "external_to_keepsats_loopback"
        assert EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW.trigger_op_type == "invoice"

    def test_has_2_required_stages(self):
        assert len(EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW.required_stages) == 2

    def test_has_5_total_stages(self):
        assert len(EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW.stages) == 5

    def test_no_lightning_ledger_stages(self):
        names = EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW.stage_names
        assert "deposit_lightning" not in names
        assert "receive_lightning" not in names

    def test_has_expected_stage_names(self):
        names = EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW.stage_names
        assert "trigger_invoice" in names
        assert "keepsats_notification_op" in names
        assert "hive_notification_transfer_op" in names
        assert "customer_hive_out" in names
        assert "small_notification_custom_json_op" in names

    def test_optional_stages(self):
        optional = [s for s in EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW.stages if not s.required]
        optional_names = {s.name for s in optional}
        assert optional_names == {
            "hive_notification_transfer_op",
            "customer_hive_out",
            "small_notification_custom_json_op",
        }

    def test_same_trigger_as_normal(self):
        assert (
            EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW.trigger_op_type
            == EXTERNAL_TO_KEEPSATS_FLOW.trigger_op_type
        )


# ---------------------------------------------------------------------------
# Tests: FlowDefinition — external_to_hive_loopback
# ---------------------------------------------------------------------------


class TestExternalToHiveLoopbackDefinition:
    def test_definition_exists(self):
        assert EXTERNAL_TO_HIVE_LOOPBACK_FLOW.name == "external_to_hive_loopback"
        assert EXTERNAL_TO_HIVE_LOOPBACK_FLOW.trigger_op_type == "invoice"

    def test_has_4_required_stages(self):
        assert len(EXTERNAL_TO_HIVE_LOOPBACK_FLOW.required_stages) == 4

    def test_has_5_total_stages(self):
        assert len(EXTERNAL_TO_HIVE_LOOPBACK_FLOW.stages) == 5

    def test_no_lightning_ledger_stages(self):
        names = EXTERNAL_TO_HIVE_LOOPBACK_FLOW.stage_names
        assert "deposit_lightning" not in names
        assert "receive_lightning" not in names

    def test_has_expected_stage_names(self):
        names = EXTERNAL_TO_HIVE_LOOPBACK_FLOW.stage_names
        assert "trigger_invoice" in names
        assert "keepsats_notification_op" in names
        assert "hive_notification_transfer_op" in names
        assert "customer_hive_out" in names
        assert "small_notification_custom_json_op" in names

    def test_required_hive_payout_stages(self):
        required_names = {s.name for s in EXTERNAL_TO_HIVE_LOOPBACK_FLOW.required_stages}
        assert "hive_notification_transfer_op" in required_names
        assert "customer_hive_out" in required_names

    def test_optional_stages(self):
        optional = [s for s in EXTERNAL_TO_HIVE_LOOPBACK_FLOW.stages if not s.required]
        optional_names = {s.name for s in optional}
        assert optional_names == {"small_notification_custom_json_op"}

    def test_same_trigger_as_normal(self):
        assert (
            EXTERNAL_TO_HIVE_LOOPBACK_FLOW.trigger_op_type == EXTERNAL_TO_HIVE_FLOW.trigger_op_type
        )


# ---------------------------------------------------------------------------
# Tests: Overwatch — loopback keepsats flow
# ---------------------------------------------------------------------------


class TestExternalToKeepsatsLoopbackOverwatch:
    @pytest.mark.asyncio
    async def test_invoice_creates_four_candidates(self):
        """An invoice trigger creates normal + loopback candidates."""
        ow = _register_all()
        await ow._try_create_flow(_op_event("invoice"), _fake_op())
        assert len(ow.active_flows) == 4
        names = {f.flow_definition.name for f in ow.active_flows}
        assert names == {
            "external_to_keepsats",
            "external_to_hive",
            "external_to_keepsats_loopback",
            "external_to_hive_loopback",
        }

    @pytest.mark.asyncio
    async def test_loopback_keepsats_completes_without_lightning_ledger(self):
        """Loopback keepsats completes with just trigger + keepsats
        notification (no deposit_l / recv_l)."""
        ow = _register_all()
        await ow._try_create_flow(_op_event("invoice"), _fake_op())

        # Keepsats notification custom_json (different short_id)
        await ow._dispatch(
            _op_event("custom_json", group_id="gid_notif", short_id="3800_61dc3b_1")
        )

        # Loopback keepsats should be completed
        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "external_to_keepsats_loopback" in completed_names

        # Normal external_to_keepsats still active (needs deposit_l + recv_l)
        active_names = {f.flow_definition.name for f in ow.active_flows}
        assert "external_to_keepsats" in active_names

    @pytest.mark.asyncio
    async def test_loopback_hive_superset_kept_alive(self):
        """When loopback keepsats completes, loopback hive is kept as
        a superset candidate."""
        ow = _register_all()
        await ow._try_create_flow(_op_event("invoice"), _fake_op())

        await ow._dispatch(
            _op_event("custom_json", group_id="gid_notif", short_id="3800_61dc3b_1")
        )

        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "external_to_keepsats_loopback" in completed_names

        # Loopback hive kept with grace period
        loopback_hive = next(
            (f for f in ow.active_flows if f.flow_definition.name == "external_to_hive_loopback"),
            None,
        )
        assert loopback_hive is not None
        assert loopback_hive.superset_grace_expires is not None

    @pytest.mark.asyncio
    async def test_loopback_hive_completes_with_hive_payout(self):
        """Loopback hive completes when HIVE payout events arrive."""
        ow = _register_all()
        await ow._try_create_flow(_op_event("invoice"), _fake_op())

        # Keepsats notification
        await ow._dispatch(
            _op_event("custom_json", group_id="gid_notif", short_id="3800_61dc3b_1")
        )

        # HIVE payout
        await ow._dispatch(_op_event("transfer", group_id="gid_hive", short_id="3802_abc123_1"))
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOMER_HIVE_OUT,
                group_id="gid_hive",
                short_id="3802_abc123_1",
            )
        )

        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "external_to_keepsats_loopback" in completed_names
        assert "external_to_hive_loopback" in completed_names

    @pytest.mark.asyncio
    async def test_loopback_hive_cancelled_after_grace_expiry(self):
        """Loopback hive is cancelled when grace period expires without
        distinguishing HIVE payout events."""
        ow = _register_all()
        await ow._try_create_flow(_op_event("invoice"), _fake_op())

        await ow._dispatch(
            _op_event("custom_json", group_id="gid_notif", short_id="3800_61dc3b_1")
        )

        loopback_hive = next(
            f for f in ow.active_flows if f.flow_definition.name == "external_to_hive_loopback"
        )
        assert loopback_hive.superset_grace_expires is not None

        # Force grace to expire
        loopback_hive.superset_grace_expires = datetime.now(tz=timezone.utc) - timedelta(seconds=1)
        await ow.check_stalls()

        remaining = [
            f for f in ow.flow_instances if f.flow_definition.name == "external_to_hive_loopback"
        ]
        assert len(remaining) == 0


# ---------------------------------------------------------------------------
# Tests: Normal flow still works alongside loopback
# ---------------------------------------------------------------------------


class TestLoopbackWithNormalFlow:
    """Ensure normal (non-loopback) flows still complete correctly when
    loopback definitions are registered alongside them."""

    @pytest.mark.asyncio
    async def test_normal_flow_completes_with_lightning_ledger(self):
        """Normal external_to_keepsats completes at 4/4 even when
        loopback flows are registered."""
        ow = _register_all()
        await ow._try_create_flow(_op_event("invoice"), _fake_op())

        # deposit_l (only normal flows match this)
        await ow._dispatch(_ledger_event(LedgerType.DEPOSIT_LIGHTNING))

        # Keepsats notification
        await ow._dispatch(
            _op_event("custom_json", group_id="gid_notif", short_id="3800_61dc3b_1")
        )

        # recv_l (only normal flows match this)
        await ow._dispatch(
            _ledger_event(
                LedgerType.RECEIVE_LIGHTNING,
                group_id="gid_notif",
                short_id="3800_61dc3b_1",
            )
        )

        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "external_to_keepsats" in completed_names
        assert "external_to_keepsats_loopback" in completed_names

        # external_to_hive still active (needs HIVE payout)
        active_names = {f.flow_definition.name for f in ow.active_flows}
        assert "external_to_hive" in active_names

    @pytest.mark.asyncio
    async def test_normal_full_flow_with_hive_payout(self):
        """Complete normal flow including HIVE payout — all flows complete."""
        ow = _register_all()
        await ow._try_create_flow(_op_event("invoice"), _fake_op())

        await ow._dispatch(_ledger_event(LedgerType.DEPOSIT_LIGHTNING))
        await ow._dispatch(
            _op_event("custom_json", group_id="gid_notif", short_id="3800_61dc3b_1")
        )
        await ow._dispatch(
            _ledger_event(
                LedgerType.RECEIVE_LIGHTNING,
                group_id="gid_notif",
                short_id="3800_61dc3b_1",
            )
        )
        await ow._dispatch(_op_event("transfer", group_id="gid_hive", short_id="3802_abc123_1"))
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOMER_HIVE_OUT,
                group_id="gid_hive",
                short_id="3802_abc123_1",
            )
        )

        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "external_to_keepsats" in completed_names
        assert "external_to_hive" in completed_names
        assert len(ow.active_flows) == 0

    @pytest.mark.asyncio
    async def test_loopback_scenario_no_lightning_ledger(self):
        """Simulate the actual loopback scenario: invoice + keepsats
        notification + HIVE notification, but NO deposit_l / recv_l.
        Loopback flows complete, normal flows get cancelled via grace."""
        ow = _register_all()
        await ow._try_create_flow(_op_event("invoice"), _fake_op())

        # Keepsats notification (no deposit_l first!)
        await ow._dispatch(
            _op_event("custom_json", group_id="gid_notif", short_id="3800_61dc3b_1")
        )

        # Loopback keepsats completed
        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "external_to_keepsats_loopback" in completed_names

        # HIVE payout
        await ow._dispatch(_op_event("transfer", group_id="gid_hive", short_id="3802_abc123_1"))
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOMER_HIVE_OUT,
                group_id="gid_hive",
                short_id="3802_abc123_1",
            )
        )

        # Loopback hive completed
        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "external_to_hive_loopback" in completed_names

        # Normal flows still active (stuck without deposit_l / recv_l)
        active_names = {f.flow_definition.name for f in ow.active_flows}
        # At least normal keepsats should have grace set from loopback completion
        normal_keepsats = next(
            (f for f in ow.active_flows if f.flow_definition.name == "external_to_keepsats"),
            None,
        )
        if normal_keepsats is not None:
            assert normal_keepsats.superset_grace_expires is not None

            # Force grace expiry to clean up stuck normal flows
            normal_keepsats.superset_grace_expires = datetime.now(tz=timezone.utc) - timedelta(
                seconds=1
            )

        # Also expire any remaining active flows' grace
        for flow in ow.active_flows:
            if flow.superset_grace_expires is not None:
                flow.superset_grace_expires = datetime.now(tz=timezone.utc) - timedelta(seconds=1)

        await ow.check_stalls()

        # After grace expiry, normal flows are removed
        active_names = {f.flow_definition.name for f in ow.active_flows}
        assert "external_to_keepsats" not in active_names
        assert "external_to_hive" not in active_names
