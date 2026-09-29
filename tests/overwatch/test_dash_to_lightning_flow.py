"""Tests for the Dash-to-Lightning overwatch flow."""

from datetime import UTC, datetime

import pytest

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.process.overwatch_flows import DASH_TO_LIGHTNING_FLOW, FLOW_DEFINITIONS
from v4vapp_backend_v2.process.process_overwatch import FlowEvent, FlowStatus, Overwatch

_TS = datetime(2026, 8, 29, 12, 0, 0, tzinfo=UTC)


def _op_event(
    op_type: str,
    group_id: str = "dash_inv_1",
    short_id: str = "ext-1",
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
    group_id: str = "dash_inv_1_d_conv_s",
    short_id: str = "ext-1",
) -> FlowEvent:
    return FlowEvent(
        event_type="ledger",
        timestamp=_TS,
        group_id=group_id,
        short_id=short_id,
        ledger_type=ledger_type,
        group="primary",
    )


def _fake_op(group_id: str = "dash_inv_1", short_id: str = "ext-1") -> object:
    return type(
        "FakeDashOp",
        (),
        {
            "group_id": group_id,
            "short_id": short_id,
            "op_type": "dash_invoice",
            "from_account": "alice",
            "cust_id": "alice",
            "conv": None,
            "log_str": "dash invoice",
            "log_extra": {},
        },
    )()


class TestDashToLightningDefinition:
    def test_registered(self):
        assert FLOW_DEFINITIONS["dash_to_lightning"] is DASH_TO_LIGHTNING_FLOW
        assert DASH_TO_LIGHTNING_FLOW.trigger_op_type == "dash_invoice"

    def test_required_stages_are_trigger_and_conversion(self):
        names = [s.name for s in DASH_TO_LIGHTNING_FLOW.required_stages]
        assert names == ["trigger_dash_invoice", "conv_dash_to_sats"]

    def test_payment_stages_optional(self):
        optional = {s.name for s in DASH_TO_LIGHTNING_FLOW.stages if not s.required}
        assert "payment_op" in optional
        assert "withdraw_lightning" in optional
        assert "fee_expense" in optional
        assert "fee_income" in optional
        assert "dash_test_pay" in optional


class TestDashToLightningProgress:
    @pytest.mark.asyncio
    async def test_trigger_creates_flow(self):
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(DASH_TO_LIGHTNING_FLOW)
        Overwatch._loaded_from_redis = True

        result = await ow._try_create_flow(_op_event("dash_invoice"), _fake_op())
        assert result == "trigger_dash_invoice"
        assert len(ow.active_flows) == 1
        assert ow.active_flows[0].flow_definition.name == "dash_to_lightning"

    @pytest.mark.asyncio
    async def test_completes_after_conversion_without_lightning(self):
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(DASH_TO_LIGHTNING_FLOW)
        Overwatch._loaded_from_redis = True

        await ow._try_create_flow(_op_event("dash_invoice"), _fake_op())
        await ow._dispatch(_ledger_event(LedgerType.CONV_DASH_TO_SATS))

        assert len(ow.completed_flows) == 1
        flow = ow.completed_flows[0]
        assert flow.status == FlowStatus.COMPLETED
        assert "trigger_dash_invoice" in flow.matched_stage_names
        assert "conv_dash_to_sats" in flow.matched_stage_names

    @pytest.mark.asyncio
    async def test_payment_stages_absorbed_when_present(self):
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(DASH_TO_LIGHTNING_FLOW)
        Overwatch._loaded_from_redis = True

        await ow._try_create_flow(_op_event("dash_invoice"), _fake_op())
        await ow._dispatch(_ledger_event(LedgerType.FEE_INCOME, group_id="dash_inv_1_fee_inc"))
        await ow._dispatch(_op_event("payment", group_id="pay_hash", short_id="pay1"))
        await ow._dispatch(
            _ledger_event(LedgerType.WITHDRAW_LIGHTNING, group_id="pay_hash", short_id="pay1")
        )
        await ow._dispatch(_ledger_event(LedgerType.CONV_DASH_TO_SATS))

        flow = ow.completed_flows[0]
        assert "fee_income" in flow.matched_stage_names
        assert "payment_op" in flow.matched_stage_names
        assert "withdraw_lightning" in flow.matched_stage_names
