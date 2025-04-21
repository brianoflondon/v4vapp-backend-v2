from datetime import timedelta
import os
from pathlib import Path

import pytest

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
async def test_stream_ops_block_range():
    """
    Test the stream_ops_async function to ensure it yields operations correctly.
    """
    opNames = ["update_proposal_votes"]

    # Call the async generator function and collect results
    results = []
    async for op in stream_ops_async(start=95_157_371, stop=95_157_382, opNames=opNames):
        print(op.log_str)
        results.append(op)

    # Check that the results are as expected
    assert len(results) > 0
