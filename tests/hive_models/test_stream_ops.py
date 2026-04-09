import os
from datetime import timedelta
from pathlib import Path

import pytest

from tests.load_data import load_hive_events
from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes
from v4vapp_backend_v2.hive_models.stream_ops import stream_ops_async


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
    fake_hive = mocker.Mock(rpc=fake_rpc)
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
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.get_good_nodes",
        return_value=["https://mock.hive.node"],
    )
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.get_hive_client",
        return_value=fake_hive,
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
