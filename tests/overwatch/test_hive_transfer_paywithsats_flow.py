"""
Tests for the Hive Transfer Paywithsats flow.

Flow: User sends a small HIVE transfer (0.001 HIVE) with a memo like
"recipient.name #paywithsats:4000" → system broadcasts a KeepsatsTransfer
custom_json → c_j_trans ledger → optional notification custom_json.

This is distinct from hive_to_keepsats (which converts the HIVE amount to
keepsats) and from keepsats_internal_transfer (triggered by custom_json).
"""

from datetime import datetime, timezone

import pytest

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.process.overwatch_flows import (
    HIVE_TO_KEEPSATS_EXTERNAL_FLOW,
    HIVE_TO_KEEPSATS_FLOW,
    HIVE_TRANSFER_FAILURE_FLOW,
    HIVE_TRANSFER_PAYWITHSATS_FLOW,
)
from v4vapp_backend_v2.process.process_overwatch import FlowEvent, FlowStatus, Overwatch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2026, 4, 11, 13, 4, 36, tzinfo=timezone.utc)


def _op_event(
    op_type: str,
    group_id: str = "gid_trigger",
    short_id: str = "7856_799039_1",
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
    short_id: str = "7856_799039_1",
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
    short_id: str = "7856_799039_1",
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
    """Register the transfer-triggered flow definitions."""
    Overwatch.reset()
    ow = Overwatch()
    Overwatch.register_flow(HIVE_TO_KEEPSATS_FLOW)
    Overwatch.register_flow(HIVE_TO_KEEPSATS_EXTERNAL_FLOW)
    Overwatch.register_flow(HIVE_TRANSFER_PAYWITHSATS_FLOW)
    Overwatch.register_flow(HIVE_TRANSFER_FAILURE_FLOW)
    Overwatch._loaded_from_redis = True
    return ow


# ---------------------------------------------------------------------------
# Tests: FlowDefinition
# ---------------------------------------------------------------------------


class TestHiveTransferPaywithsatsDefinition:
    def test_definition_exists(self):
        assert HIVE_TRANSFER_PAYWITHSATS_FLOW.name == "hive_transfer_paywithsats"
        assert HIVE_TRANSFER_PAYWITHSATS_FLOW.trigger_op_type == "transfer"

    def test_has_4_required_stages(self):
        assert len(HIVE_TRANSFER_PAYWITHSATS_FLOW.required_stages) == 4

    def test_has_5_total_stages(self):
        assert len(HIVE_TRANSFER_PAYWITHSATS_FLOW.stages) == 5

    def test_stage_names(self):
        names = HIVE_TRANSFER_PAYWITHSATS_FLOW.stage_names
        assert "trigger_transfer" in names
        assert "customer_hive_in" in names
        assert "keepsats_transfer_op" in names
        assert "custom_json_transfer" in names
        assert "notification_custom_json_op" in names

    def test_notification_is_optional(self):
        optional = [s for s in HIVE_TRANSFER_PAYWITHSATS_FLOW.stages if not s.required]
        assert len(optional) == 1
        assert optional[0].name == "notification_custom_json_op"

    def test_same_trigger_as_hive_to_keepsats(self):
        assert (
            HIVE_TRANSFER_PAYWITHSATS_FLOW.trigger_op_type == HIVE_TO_KEEPSATS_FLOW.trigger_op_type
        )

    def test_fewer_required_stages_than_hive_to_keepsats(self):
        assert len(HIVE_TRANSFER_PAYWITHSATS_FLOW.required_stages) < len(
            HIVE_TO_KEEPSATS_FLOW.required_stages
        )


# ---------------------------------------------------------------------------
# Tests: Overwatch integration
# ---------------------------------------------------------------------------


class TestHiveTransferPaywithsatsOverwatch:
    @pytest.mark.asyncio
    async def test_transfer_creates_four_candidates(self):
        """A transfer trigger should create hive_to_keepsats,
        hive_to_keepsats_external, hive_transfer_paywithsats, and
        hive_transfer_failure."""
        ow = _register_transfer_flows()
        event = _op_event("transfer")
        await ow._try_create_flow(event, _fake_op())
        assert len(ow.active_flows) == 4
        names = {f.flow_definition.name for f in ow.active_flows}
        assert names == {
            "hive_to_keepsats",
            "hive_to_keepsats_external",
            "hive_transfer_paywithsats",
            "hive_transfer_failure",
        }

    @pytest.mark.asyncio
    async def test_paywithsats_completes_with_all_stages(self):
        """Paywithsats flow completes when cust_h_in + custom_json +
        c_j_trans arrive."""
        ow = _register_transfer_flows()
        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())

        # cust_h_in
        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        # KeepsatsTransfer custom_json op
        await ow._dispatch(_op_event("custom_json", group_id="gid_cj", short_id="7857_abc123_1"))
        # c_j_trans ledger
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOM_JSON_TRANSFER,
                group_id="gid_cj",
                short_id="7857_abc123_1",
            )
        )

        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "hive_transfer_paywithsats" in completed_names

    @pytest.mark.asyncio
    async def test_candidates_removed_on_completion(self):
        """When hive_transfer_paywithsats completes, the other candidates
        are removed because paywithsats has a CUSTOM_JSON_TRANSFER stage
        that doesn't appear in hive_to_keepsats or external — so the
        winner is NOT a proper subset, and the candidates have no unique
        events either."""
        ow = _register_transfer_flows()
        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())

        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        await ow._dispatch(_op_event("custom_json", group_id="gid_cj", short_id="7857_abc123_1"))
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOM_JSON_TRANSFER,
                group_id="gid_cj",
                short_id="7857_abc123_1",
            )
        )

        assert len(ow.completed_flows) == 1
        assert ow.completed_flows[0].flow_definition.name == "hive_transfer_paywithsats"

        # Candidates removed: paywithsats has CUSTOM_JSON_TRANSFER which is
        # not in the other flows, so it's not a proper subset → candidates killed
        assert len(ow.active_flows) == 0

    @pytest.mark.asyncio
    async def test_no_active_flows_remain_after_completion(self):
        """All candidate flows are removed immediately upon paywithsats
        completion — no grace period cleanup needed."""
        ow = _register_transfer_flows()
        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())

        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        await ow._dispatch(_op_event("custom_json", group_id="gid_cj", short_id="7857_abc123_1"))
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOM_JSON_TRANSFER,
                group_id="gid_cj",
                short_id="7857_abc123_1",
            )
        )

        assert len(ow.completed_flows) == 1
        assert len(ow.active_flows) == 0

    @pytest.mark.asyncio
    async def test_notification_absorbed_after_completion(self):
        """Late notification custom_json is absorbed by the recently-completed
        paywithsats flow."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TRANSFER_PAYWITHSATS_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())

        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        await ow._dispatch(_op_event("custom_json", group_id="gid_cj", short_id="7857_abc123_1"))
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOM_JSON_TRANSFER,
                group_id="gid_cj",
                short_id="7857_abc123_1",
            )
        )

        assert len(ow.completed_flows) == 1
        # Force recent completion
        ow.completed_flows[0].completed_at = datetime.now(tz=timezone.utc)

        # Notification arrives shortly after
        notif = _op_event("custom_json", group_id="gid_notif", short_id="7858_def456_1")
        result = await ow._dispatch(notif)
        assert result is not None
        assert len(ow.completed_flows[0].events) == 5

    @pytest.mark.asyncio
    async def test_progress_reporting(self):
        """Progress should report correctly at each stage."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TRANSFER_PAYWITHSATS_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())

        flow = ow.active_flows[0]
        assert flow.progress == "1/4 required stages complete"

        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        assert flow.progress == "2/4 required stages complete"

        await ow._dispatch(_op_event("custom_json", group_id="gid_cj", short_id="7857_abc123_1"))
        assert flow.progress == "3/4 required stages complete"

        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOM_JSON_TRANSFER,
                group_id="gid_cj",
                short_id="7857_abc123_1",
            )
        )
        assert flow.status == FlowStatus.COMPLETED
