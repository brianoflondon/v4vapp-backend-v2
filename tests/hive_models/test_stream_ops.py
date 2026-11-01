import os
from datetime import timedelta
from pathlib import Path

import pytest

from tests.load_data import load_hive_events
from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes
from v4vapp_backend_v2.hive_models.stream_ops import stream_ops_async, virtual_blocks_to_fetch


@pytest.fixture(autouse=True)
def set_base_config_path_combined(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig._instance", None
    )  # Resetting InternalConfig instance


@pytest.mark.asyncio
async def test_stream_ops_async_live_from_hive():
    """
    Test the stream_ops_async function to ensure it yields operations correctly.
    """
    opNames = []
    look_back = timedelta(seconds=10)

    # Call the async generator function and collect results
    results = []
    async for op in stream_ops_async(look_back=look_back, stop_now=True, opNames=opNames):
        print(op.log_str)
        results.append(op)

    # Check that the results are as expected
    assert len(results) > 0


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Skipping test on GitHub Actions"
)
@pytest.mark.asyncio
async def test_stream_ops_async_live_from_hive_stop_now():
    """
    Test the stream_ops_async function to ensure it yields operations correctly.
    """
    opNames = ["producer_reward"]
    look_back = timedelta(minutes=0.5)

    # Call the async generator function and collect results
    results = []
    async for op in stream_ops_async(look_back=look_back, stop_now=True, opNames=opNames):
        print(op.log_str)
        results.append(op)

    # Check that the results are as expected
    assert len(results) > 0


@pytest.mark.asyncio
async def test_stream_ops_block_range(mocker):
    """
    Test the stream_ops_async function to ensure it yields operations correctly.
    """
    opNames = ["update_proposal_votes"]

    fake_event = next(load_hive_events(OpTypes.UPDATE_PROPOSAL_VOTES)).copy()
    fake_event["block_num"] = 95_157_382

    fake_rpc = mocker.Mock(url="https://mock.hive.node")
    fake_rpc.next.return_value = None
    fake_rpc.timeout = 12  # fail-fast shape so _ensure_stream_hive keeps this client
    fake_rpc.nodes = None
    fake_hive = mocker.Mock(rpc=fake_rpc, keys=None)
    fake_hive.set_default_nodes.return_value = None

    fake_blockchain = mocker.Mock()
    fake_blockchain.get_current_block_num.return_value = 95_157_382

    def fake_stream(*args, **kwargs):
        if kwargs.get("only_virtual_ops"):
            return iter([])
        return iter([fake_event])

    fake_blockchain.stream.side_effect = fake_stream

    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.TrackedBaseModel.update_quote",
        return_value=None,
    )
    # Clients are built via make_stream_hive (not Hive() in this module).
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.make_stream_hive",
        return_value=fake_hive,
    )
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.close_hive_client",
        return_value=None,
    )
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.get_blockchain_instance",
        return_value=fake_blockchain,
    )

    # Call the async generator function and collect results
    results = []
    async for op in stream_ops_async(start=95_157_371, stop=95_157_382, opNames=opNames):
        print(op.log_str)
        results.append(op)

    # Check that the results are as expected
    assert len(results) > 0


def test_virtual_blocks_one_step_keeps_two_block_lag():
    """A step from 766 to 767 still fetches only 765."""
    assert list(virtual_blocks_to_fetch(766, 767)) == [765]


def test_virtual_blocks_gap_includes_block_just_left():
    """Order 1839180890: real stream jumped 111268746 → 111268748.

    Block 111268747 had no tracked real op. The fills live in 111268746.
    """
    assert list(virtual_blocks_to_fetch(111_268_746, 111_268_748)) == [
        111_268_745,
        111_268_746,
    ]


def test_virtual_blocks_wider_gap_stops_before_new_block():
    assert list(virtual_blocks_to_fetch(100, 105)) == [99, 100, 101, 102, 103]


def test_virtual_blocks_ignores_non_advancing_and_genesis():
    assert list(virtual_blocks_to_fetch(50, 50)) == []
    assert list(virtual_blocks_to_fetch(50, 40)) == []
    assert list(virtual_blocks_to_fetch(1, 2)) == []


def _stream_test_doubles(mocker, real_events):
    fake_rpc = mocker.Mock(url="https://mock.hive.node")
    fake_rpc.next.return_value = None
    fake_rpc.timeout = 12
    fake_rpc.nodes = None
    fake_hive = mocker.Mock(rpc=fake_rpc, keys=None)
    fake_hive.set_default_nodes.return_value = None

    virtual_starts: list[int] = []

    def fake_stream(*args, **kwargs):
        if kwargs.get("only_virtual_ops"):
            virtual_starts.append(kwargs["start"])
            return iter([])
        return iter(real_events)

    fake_blockchain = mocker.Mock()
    fake_blockchain.get_current_block_num.return_value = real_events[-1]["block_num"]
    fake_blockchain.stream.side_effect = fake_stream

    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.TrackedBaseModel.update_quote",
        return_value=None,
    )
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.make_stream_hive",
        return_value=fake_hive,
    )
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.close_hive_client",
        return_value=None,
    )
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.get_blockchain_instance",
        return_value=fake_blockchain,
    )
    return virtual_starts


@pytest.mark.asyncio
async def test_stream_fetches_virtuals_for_block_left_behind_by_a_gap(mocker):
    """A quiet block between two real ops must not drop the earlier block's virtuals."""
    base = next(load_hive_events(OpTypes.UPDATE_PROPOSAL_VOTES)).copy()
    first = base.copy()
    first["block_num"] = 100
    second = base.copy()
    second["block_num"] = 102
    virtual_starts = _stream_test_doubles(mocker, [first, second])

    results = []
    async for op in stream_ops_async(start=100, stop=102, opNames=["update_proposal_votes"]):
        results.append(op.block_num)

    assert results == [100, 102]
    assert virtual_starts == [99, 100]


@pytest.mark.asyncio
async def test_stream_one_block_step_fetches_only_the_lag_block(mocker):
    base = next(load_hive_events(OpTypes.UPDATE_PROPOSAL_VOTES)).copy()
    first = base.copy()
    first["block_num"] = 100
    second = base.copy()
    second["block_num"] = 101
    virtual_starts = _stream_test_doubles(mocker, [first, second])

    async for _op in stream_ops_async(start=100, stop=101, opNames=["update_proposal_votes"]):
        pass

    assert virtual_starts == [99]
