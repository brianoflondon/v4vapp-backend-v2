"""Tests that VSC custom_json ops (cj_id starting with 'vsc.') are not picked
up by overwatch as flow triggers.

These are MAGI BTC transactions routed via the VSC smart-contract layer.
They share the 'custom_json' op_type with keepsats flows but never produce
ledger entries, so they must be filtered out before flow instances are
created.
"""

import pytest

from v4vapp_backend_v2.process.overwatch_flows import (
    KEEPSATS_INTERNAL_TRANSFER_FLOW,
    KEEPSATS_TO_EXTERNAL_FLOW,
    KEEPSATS_TO_HIVE_FLOW,
)
from v4vapp_backend_v2.process.process_overwatch import FlowEvent, Overwatch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_vsc_op(cj_id: str = "vsc.call", from_account: str = "gadrian") -> object:
    """Return a minimal fake VSC custom_json op."""
    return type(
        "FakeVSCOp",
        (),
        {
            "op_type": "custom_json",
            "cj_id": cj_id,
            "from_account": from_account,
            "group_id": "gid_vsc_test",
            "short_id": "vsc_test_1",
            "json_data": None,
        },
    )()


def _vsc_event(group_id: str = "gid_vsc_test") -> FlowEvent:
    return FlowEvent(
        event_type="op",
        group_id=group_id,
        short_id="vsc_test_1",
        op_type="custom_json",
        group="primary",
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_overwatch():
    """Reset the Overwatch singleton and register the standard flows before each test."""
    Overwatch.reset()
    ow = Overwatch()
    Overwatch.register_flow(KEEPSATS_TO_HIVE_FLOW)
    Overwatch.register_flow(KEEPSATS_TO_EXTERNAL_FLOW)
    Overwatch.register_flow(KEEPSATS_INTERNAL_TRANSFER_FLOW)
    # Mark as loaded so _ensure_loaded() does not try to contact Redis.
    Overwatch._loaded_from_redis = True
    yield
    Overwatch.reset()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_vsc_call_does_not_create_flow():
    """A 'vsc.call' custom_json must not create candidate flows."""
    ow = Overwatch()
    op = _make_vsc_op(cj_id="vsc.call")
    result = await ow._try_create_flow(_vsc_event(), op)
    assert result is None
    assert ow.flow_instances == []


@pytest.mark.asyncio
async def test_vsc_any_prefix_does_not_create_flow():
    """Any cj_id starting with 'vsc.' (e.g. 'vsc.execute') must be skipped."""
    ow = Overwatch()
    op = _make_vsc_op(cj_id="vsc.execute")
    result = await ow._try_create_flow(_vsc_event("gid_vsc_execute"), op)
    assert result is None
    assert ow.flow_instances == []
