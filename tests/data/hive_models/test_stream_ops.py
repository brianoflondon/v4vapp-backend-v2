from datetime import timedelta
from pathlib import Path
from typing import Generator, List

import pytest
from unittest.mock import patch

from tests.load_data import load_hive_events
from v4vapp_backend_v2.hive_models.op_all import OpAny
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

    # Mock the stream method of the blockchain instance
    opNames = []
    look_back = timedelta(seconds=10)

    # Call the async generator function and collect results
    results = []
    async for op in stream_ops_async(look_back=look_back, stop_now=True, opNames=opNames):
        print(op.log_str)
        results.append(op)

    # Check that the results are as expected
    assert len(results) > 0


# @pytest.mark.asyncio
# async def test_stream_ops_async():
#     """
#     Test the stream_ops_async function to ensure it yields operations correctly.
#     """

#     def mock_blockchain_stream(*args, **kwargs) -> Generator:
#         # Mock the blockchain stream method to yield test events
#         for hive_event in load_hive_events():
#             yield hive_event

#     # Patch blockchain.stream globally for all calls
#     with patch(
#         "v4vapp_backend_v2.hive_models.stream_ops.Blockchain.stream",
#     ) as mock_stream:
#         mock_stream.side_effect = mock_blockchain_stream

#         # Call the async generator function and collect results
#         results: List[OpAny] = []
#         async for op in stream_ops_async(
#             look_back=timedelta(seconds=10),
#             opNames=["transfer", "fill_vesting_withdraw", "comment"],
#             stop_now=True,
#         ):
#             results.append(op)
#             print(op)
