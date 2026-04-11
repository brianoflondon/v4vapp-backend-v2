"""
Tests for the External-to-Keepsats flow.

Flow: External Lightning invoice received → sats stored in keepsats →
keepsats notification → optional HIVE notification (or custom_json for
small amounts).

A key benefit of this flow: once active (triggered by 'invoice'), the
subsequent custom_json and transfer events are absorbed by _dispatch
instead of creating false keepsats_to_hive / hive_to_keepsats candidates.
"""

from datetime import datetime, timezone

import pytest

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.process.overwatch_flows import (
    EXTERNAL_TO_HIVE_FLOW,
    EXTERNAL_TO_HIVE_LOOPBACK_FLOW,
    EXTERNAL_TO_KEEPSATS_FLOW,
    EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW,
    HIVE_TO_KEEPSATS_EXTERNAL_FLOW,
    HIVE_TO_KEEPSATS_FLOW,
    HIVE_TRANSFER_PAYWITHSATS_FLOW,
    KEEPSATS_INTERNAL_TRANSFER_FLOW,
    KEEPSATS_TO_EXTERNAL_FLOW,
    KEEPSATS_TO_HIVE_FLOW,
)
from v4vapp_backend_v2.process.process_overwatch import FlowEvent, Overwatch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2026, 3, 17, 17, 59, 58, tzinfo=timezone.utc)


def _op_event(
    op_type: str,
    group_id: str = "gid_invoice",
    short_id: str = "diU7Zopuqw",
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
    short_id: str = "diU7Zopuqw",
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
    short_id: str = "diU7Zopuqw",
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


# ---------------------------------------------------------------------------
# Tests: FlowDefinition
# ---------------------------------------------------------------------------


class TestExternalToKeepsatsDefinition:
    """Tests for the EXTERNAL_TO_KEEPSATS_FLOW definition."""

    def test_definition_exists(self):
        assert EXTERNAL_TO_KEEPSATS_FLOW.name == "external_to_keepsats"
        assert EXTERNAL_TO_KEEPSATS_FLOW.trigger_op_type == "invoice"

    def test_has_4_required_stages(self):
        assert len(EXTERNAL_TO_KEEPSATS_FLOW.required_stages) == 4

    def test_has_7_total_stages(self):
        assert len(EXTERNAL_TO_KEEPSATS_FLOW.stages) == 7

    def test_has_expected_stage_names(self):
        names = EXTERNAL_TO_KEEPSATS_FLOW.stage_names
        assert "trigger_invoice" in names
        assert "deposit_lightning" in names
        assert "keepsats_notification_op" in names
        assert "receive_lightning" in names
        assert "hive_notification_transfer_op" in names
        assert "customer_hive_out" in names
        assert "small_notification_custom_json_op" in names

    def test_optional_stages(self):
        optional = [s for s in EXTERNAL_TO_KEEPSATS_FLOW.stages if not s.required]
        optional_names = {s.name for s in optional}
        assert optional_names == {
            "hive_notification_transfer_op",
            "customer_hive_out",
            "small_notification_custom_json_op",
        }


# ---------------------------------------------------------------------------
# Tests: Overwatch integration
# ---------------------------------------------------------------------------


class TestExternalToKeepsatsOverwatch:
    """Full dispatch tests using the Overwatch singleton."""

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
        Overwatch._loaded_from_redis = True
        return ow

    @pytest.mark.asyncio
    async def test_invoice_creates_two_candidates(self):
        """An invoice trigger should create two candidates —
        external_to_keepsats and its superset external_to_hive
        (plus their loopback variants)."""
        ow = self._register_all()
        event = _op_event("invoice")
        result = await ow._try_create_flow(event, _fake_op())
        assert result == "trigger_invoice"
        assert len(ow.active_flows) == 4
        names = {f.flow_definition.name for f in ow.active_flows}
        assert names == {
            "external_to_keepsats",
            "external_to_hive",
            "external_to_keepsats_loopback",
            "external_to_hive_loopback",
        }

    @pytest.mark.asyncio
    async def test_full_flow_with_hive_notification(self):
        """Complete flow with HIVE notification (normal amount)."""
        ow = self._register_all()

        # Trigger
        trigger = _op_event("invoice")
        await ow._try_create_flow(trigger, _fake_op())

        # Primary: deposit_l
        await ow._dispatch(_ledger_event(LedgerType.DEPOSIT_LIGHTNING))

        # Keepsats notification: custom_json + recv_l (different short_id)
        await ow._dispatch(
            _op_event("custom_json", group_id="gid_notif", short_id="3143_0bb89a_1")
        )
        await ow._dispatch(
            _ledger_event(
                LedgerType.RECEIVE_LIGHTNING,
                group_id="gid_notif",
                short_id="3143_0bb89a_1",
            )
        )

        # Flow should be complete (4/4 required stages)
        # (loopback keepsats also completed earlier at 2/2)
        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "external_to_keepsats" in completed_names
        assert "external_to_keepsats_loopback" in completed_names

        # HIVE notification — absorbed by active external_to_hive
        await ow._dispatch(_op_event("transfer", group_id="gid_hive", short_id="3145_02a914_1"))
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOMER_HIVE_OUT,
                group_id="gid_hive",
                short_id="3145_02a914_1",
            )
        )

        # All matching flows completed
        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "external_to_keepsats" in completed_names
        assert "external_to_hive" in completed_names

    @pytest.mark.asyncio
    async def test_full_flow_without_hive_notification(self):
        """Complete flow without HIVE notification (small amount)."""
        ow = self._register_all()

        trigger = _op_event("invoice")
        await ow._try_create_flow(trigger, _fake_op())

        await ow._dispatch(_ledger_event(LedgerType.DEPOSIT_LIGHTNING))
        await ow._dispatch(
            _op_event("custom_json", group_id="gid_notif", short_id="3143_0bb89a_1")
        )
        await ow._dispatch(
            _ledger_event(
                LedgerType.RECEIVE_LIGHTNING,
                group_id="gid_notif",
                short_id="3143_0bb89a_1",
            )
        )

        # Complete with 4 required stages, no HIVE notification
        # (loopback keepsats also completed at 2/2)
        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "external_to_keepsats" in completed_names
        # external_to_hive still active (waiting for HIVE payout)
        active_names = {f.flow_definition.name for f in ow.active_flows}
        assert "external_to_hive" in active_names

    @pytest.mark.asyncio
    async def test_custom_json_absorbed_not_creating_false_candidates(self):
        """The custom_json from keepsats notification should be absorbed
        by the active external_to_keepsats flow — NOT trigger
        keepsats_to_hive / keepsats_to_external candidates."""
        ow = self._register_all()

        # Start external_to_keepsats via invoice trigger
        trigger = _op_event("invoice")
        await ow._try_create_flow(trigger, _fake_op())
        assert len(ow.active_flows) == 4

        # Deposit ledger
        await ow._dispatch(_ledger_event(LedgerType.DEPOSIT_LIGHTNING))

        # custom_json arrives — should be absorbed by all active flows
        cj_event = _op_event("custom_json", group_id="gid_notif", short_id="3143_0bb89a_1")
        result = await ow._dispatch(cj_event)
        assert result is not None  # absorbed

        # No new candidates should have been created
        # (loopback keepsats may have completed, but no false candidates)
        active_names = {f.flow_definition.name for f in ow.active_flows}
        assert "hive_to_keepsats" not in active_names
        assert "hive_to_keepsats_external" not in active_names

    @pytest.mark.asyncio
    async def test_transfer_absorbed_not_creating_false_candidates(self):
        """The HIVE notification transfer should be absorbed by the active
        external_to_keepsats flow — NOT trigger hive_to_keepsats candidates."""
        ow = self._register_all()

        # Start and partially advance
        trigger = _op_event("invoice")
        await ow._try_create_flow(trigger, _fake_op())
        await ow._dispatch(_ledger_event(LedgerType.DEPOSIT_LIGHTNING))
        await ow._dispatch(
            _op_event("custom_json", group_id="gid_notif", short_id="3143_0bb89a_1")
        )
        await ow._dispatch(
            _ledger_event(
                LedgerType.RECEIVE_LIGHTNING,
                group_id="gid_notif",
                short_id="3143_0bb89a_1",
            )
        )

        # Flow completed. Transfer arrives — absorbed by active external_to_hive.
        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "external_to_keepsats" in completed_names

        transfer_event = _op_event("transfer", group_id="gid_hive", short_id="3145_02a914_1")
        result = await ow._dispatch(transfer_event)
        # Transfer absorbed by active external_to_hive
        assert result is not None

        # No hive_to_keepsats candidates created
        flow_names = {f.flow_definition.name for f in ow.flow_instances}
        assert "hive_to_keepsats" not in flow_names
        assert "hive_to_keepsats_external" not in flow_names

    @pytest.mark.asyncio
    async def test_small_amount_custom_json_notification(self):
        """For small amounts, a second custom_json replaces the HIVE
        notification and is absorbed by the optional stage."""
        ow = self._register_all()

        trigger = _op_event("invoice")
        await ow._try_create_flow(trigger, _fake_op())
        await ow._dispatch(_ledger_event(LedgerType.DEPOSIT_LIGHTNING))
        await ow._dispatch(
            _op_event("custom_json", group_id="gid_notif", short_id="3143_0bb89a_1")
        )
        await ow._dispatch(
            _ledger_event(
                LedgerType.RECEIVE_LIGHTNING,
                group_id="gid_notif",
                short_id="3143_0bb89a_1",
            )
        )

        # Flow completed. Second custom_json (small-amount notification) arrives.
        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "external_to_keepsats" in completed_names

        small_notif = _op_event("custom_json", group_id="gid_small", short_id="3145_small_1")
        result = await ow._dispatch(small_notif)
        # Absorbed by active external_to_hive (or completed external_to_keepsats)
        assert result is not None

    @pytest.mark.asyncio
    async def test_invoice_does_not_create_other_flow_types(self):
        """Invoice trigger should not create hive_to_keepsats, keepsats_to_hive,
        or keepsats_to_external candidates."""
        ow = self._register_all()
        event = _op_event("invoice")
        await ow._try_create_flow(event, _fake_op())
        flow_names = {f.flow_definition.name for f in ow.active_flows}
        assert flow_names == {
            "external_to_keepsats",
            "external_to_hive",
            "external_to_keepsats_loopback",
            "external_to_hive_loopback",
        }
