"""
Tests for the Hive Transfer Failure flow.

Flow: User sends a HIVE/HBD transfer to the server account, but the system
cannot process the request (amount below minimum, above maximum, conversion
limits exceeded, Lightning decode failure, LND payment failure, etc.).
The system returns the full amount to the sender.

This flow has the same stage signatures as balance_request, but without the
event_filter — it acts as a catch-all for any transfer that ends in an
immediate refund.  When balance_request also matches (because the memo is
``#balance_request``), both flows complete simultaneously and the superset
candidates (hive_to_keepsats, hive_to_keepsats_external) are placed in
grace — the correct behaviour.
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

_TS = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)


def _op_event(
    op_type: str,
    group_id: str = "gid_trigger",
    short_id: str = "5691_010648_23",
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
    short_id: str = "5691_010648_23",
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
    group_id: str = "gid_trigger",
    short_id: str = "5691_010648_23",
) -> object:
    return type(
        "FakeOp",
        (),
        {
            "group_id": group_id,
            "short_id": short_id,
            "op_type": "transfer",
            "from_account": "magi.network",
        },
    )()


def _register_transfer_flows() -> Overwatch:
    """Register the transfer-triggered flow definitions (no balance_request)."""
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


class TestHiveTransferFailureDefinition:
    def test_definition_exists(self):
        assert HIVE_TRANSFER_FAILURE_FLOW.name == "hive_transfer_failure"
        assert HIVE_TRANSFER_FAILURE_FLOW.trigger_op_type == "transfer"

    def test_has_4_required_stages(self):
        assert len(HIVE_TRANSFER_FAILURE_FLOW.required_stages) == 4

    def test_has_4_total_stages(self):
        assert len(HIVE_TRANSFER_FAILURE_FLOW.stages) == 4

    def test_stage_names(self):
        names = HIVE_TRANSFER_FAILURE_FLOW.stage_names
        assert names == [
            "trigger_transfer",
            "customer_hive_in",
            "refund_transfer_op",
            "customer_hive_out",
        ]

    def test_no_optional_stages(self):
        optional = [s for s in HIVE_TRANSFER_FAILURE_FLOW.stages if not s.required]
        assert len(optional) == 0

    def test_no_event_filter_on_trigger(self):
        trigger = HIVE_TRANSFER_FAILURE_FLOW.stages[0]
        assert trigger.event_filter is None

    def test_same_stage_sigs_as_balance_request(self):
        """Failure flow has identical stage signatures to balance_request."""
        from v4vapp_backend_v2.process.overwatch_flows import BALANCE_REQUEST_FLOW

        def _sig(s):
            return (s.event_type, s.op_type, s.ledger_type)

        failure_sigs = {_sig(s) for s in HIVE_TRANSFER_FAILURE_FLOW.stages}
        br_sigs = {_sig(s) for s in BALANCE_REQUEST_FLOW.stages}
        assert failure_sigs == br_sigs


# ---------------------------------------------------------------------------
# Tests: Overwatch integration
# ---------------------------------------------------------------------------


class TestHiveTransferFailureOverwatch:
    @pytest.mark.asyncio
    async def test_transfer_creates_four_candidates(self):
        """A transfer trigger creates hive_to_keepsats, hive_to_keepsats_external,
        hive_transfer_paywithsats, and hive_transfer_failure."""
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
    async def test_failure_flow_completes_on_refund(self):
        """Failure flow completes when the refund transfer and CUSTOMER_HIVE_OUT
        arrive (full 4-stage sequence)."""
        ow = _register_transfer_flows()
        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())

        # Primary: CUSTOMER_HIVE_IN
        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        # Refund: transfer op (different short_id — the return transfer)
        await ow._dispatch(_op_event("transfer", group_id="gid_refund", short_id="5691_010648_24"))
        # Refund: CUSTOMER_HIVE_OUT
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOMER_HIVE_OUT,
                group_id="gid_refund",
                short_id="5691_010648_24",
            )
        )

        completed_names = {f.flow_definition.name for f in ow.completed_flows}
        assert "hive_transfer_failure" in completed_names

    @pytest.mark.asyncio
    async def test_superset_candidates_get_grace_period(self):
        """When failure flow completes, hive_to_keepsats and
        hive_to_keepsats_external are kept as superset candidates
        because their stage signatures are a proper superset of the
        failure flow's."""
        ow = _register_transfer_flows()
        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())

        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        await ow._dispatch(_op_event("transfer", group_id="gid_refund", short_id="5691_010648_24"))
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOMER_HIVE_OUT,
                group_id="gid_refund",
                short_id="5691_010648_24",
            )
        )

        superset_flows = [f for f in ow.active_flows if f.superset_grace_expires is not None]
        superset_names = {f.flow_definition.name for f in superset_flows}
        assert "hive_to_keepsats" in superset_names
        assert "hive_to_keepsats_external" in superset_names

    @pytest.mark.asyncio
    async def test_paywithsats_removed_on_failure_completion(self):
        """When failure flow completes, paywithsats is removed because
        its stage signatures are not a superset of the failure flow's
        (and vice versa)."""
        ow = _register_transfer_flows()
        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())

        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        await ow._dispatch(_op_event("transfer", group_id="gid_refund", short_id="5691_010648_24"))
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOMER_HIVE_OUT,
                group_id="gid_refund",
                short_id="5691_010648_24",
            )
        )

        active_names = {f.flow_definition.name for f in ow.active_flows}
        assert "hive_transfer_paywithsats" not in active_names

    @pytest.mark.asyncio
    async def test_progress_reporting(self):
        """Progress reports correctly at each stage."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TRANSFER_FAILURE_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())
        flow = ow.active_flows[0]
        assert flow.progress == "1/4 required stages complete"

        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        assert flow.progress == "2/4 required stages complete"

        await ow._dispatch(_op_event("transfer", group_id="gid_refund", short_id="5691_010648_24"))
        assert flow.progress == "3/4 required stages complete"

        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOMER_HIVE_OUT,
                group_id="gid_refund",
                short_id="5691_010648_24",
            )
        )
        assert flow.status == FlowStatus.COMPLETED
        assert flow.progress == "4/4 required stages complete"

    @pytest.mark.asyncio
    async def test_single_flow_no_superset_conflict(self):
        """When only failure flow is registered, no superset resolution needed."""
        Overwatch.reset()
        ow = Overwatch()
        Overwatch.register_flow(HIVE_TRANSFER_FAILURE_FLOW)
        Overwatch._loaded_from_redis = True

        trigger = _op_event("transfer")
        await ow._try_create_flow(trigger, _fake_op())
        assert len(ow.active_flows) == 1

        await ow._dispatch(_ledger_event(LedgerType.CUSTOMER_HIVE_IN))
        await ow._dispatch(_op_event("transfer", group_id="gid_refund", short_id="5691_010648_24"))
        await ow._dispatch(
            _ledger_event(
                LedgerType.CUSTOMER_HIVE_OUT,
                group_id="gid_refund",
                short_id="5691_010648_24",
            )
        )

        assert len(ow.completed_flows) == 1
        assert len(ow.active_flows) == 0
        assert ow.completed_flows[0].flow_definition.name == "hive_transfer_failure"
