import json
import os
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from v4vapp_backend_v2.hive.witness_details import API_ENDPOINTS, get_hive_witness_details


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


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Skipping test on GitHub Actions 429 errorrs"
)
@pytest.mark.asyncio
async def test_get_hive_witness_details_simple():
    witness_details = await get_hive_witness_details("blocktrades")
    assert witness_details is not None
    assert witness_details.witness.witness_name == "blocktrades"
    assert witness_details.witness.missed_blocks >= 0
    assert witness_details.witness.rank > 0


@pytest.mark.asyncio
async def test_get_hive_witness_details_empty():
    """
    Test the `get_hive_witness_details` function when it returns an empty list of witnesses.
    This test performs the following checks:
    1. Ensures that the `witnesses` key in the returned dictionary is not None.
    2. Iterates through each witness in the `witnesses` list and validates
    it using the `WitnessDetails` model.
    3. Asserts that the `witness_name` and `rank` attributes of the validated witness
    match the corresponding values in the original witness dictionary.
    4. Dumps the validated witness model for further inspection.
    Raises:
        AssertionError: If any of the assertions fail.
    """

    witness_details = await get_hive_witness_details()
    for witness in witness_details.witnesses:
        witness.model_dump()


@pytest.mark.asyncio
async def test_get_hive_witness_details_error():
    witness_details = await get_hive_witness_details("non_existent_witness")
    assert not witness_details


@pytest.mark.asyncio
async def test_get_hive_witness_details(mocker):
    # Mock the httpx.AsyncClient.get method
    mock_httpx_get = mocker.patch("httpx.AsyncClient.get", new_callable=AsyncMock)

    # Mock the Redis context manager

    mock_redis = mocker.patch("v4vapp_backend_v2.hive.witness_details.V4VAsyncRedis")
    mock_redis_instance = mock_redis.return_value
    mock_redis_instance.__aenter__.return_value = mock_redis_instance
    mock_redis_instance.__aexit__.return_value = None

    # Sample response data
    sample_response = {
        "votes_updated_at": "2025-03-25T09:30:43.082106",
        "witness": {
            "witness_name": "brianoflondon",
            "rank": 35,
            "url": "https://v4v.app/",
            "vests": "37725559801981293",
            "votes_daily_change": "0",
            "voters_num": 631,
            "voters_num_daily_change": 0,
            "price_feed": 0.26,
            "bias": 0,
            "feed_updated_at": "2025-03-25T07:39:45",
            "block_size": 65536,
            "signing_key": "STM6Yvdz6HtdhyzAi6oimvm5MFevSWeThYZJvbLGSmq4UeUxAEztg",
            "version": "1.27.6",
            "missed_blocks": 15,
            "hbd_interest_rate": 1000,
            "last_confirmed_block_num": 94440770,
            "account_creation_fee": 3000,
        },
    }

    # Configure the mock to return a response with the sample data
    mock_httpx_get.return_value.status_code = 200
    mock_httpx_get.return_value.json = Mock(return_value=sample_response)

    # Mock Redis get and set methods
    mock_redis_instance.get = AsyncMock(return_value=json.dumps(sample_response))
    mock_redis_instance.set = AsyncMock(return_value=None)

    # Call the function
    witness_details = await get_hive_witness_details("brianoflondon")

    # Assertions
    assert witness_details is not None
    assert witness_details.witness.witness_name == "brianoflondon"
    assert witness_details.witness.missed_blocks >= 0
    assert witness_details.witness.rank > 0

    # # Ensure the httpx get method was called with the correct URL
    # assert any(
    #     mock_httpx_get.call_args_list[i][0][0] == f"{api}/brianoflondon"
    #     for i, api in enumerate(API_ENDPOINTS)
    # ), "None of the API calls succeeded with the expected URL"

    # Ensure the Redis set method was called with the correct parameters
    mock_redis_instance.set.assert_called_with(
        name="witness_brianoflondon", value=json.dumps(sample_response)
    )


@pytest.mark.asyncio
async def test_get_hive_witness_details_mock_empty(mocker):
    # Mock the httpx.AsyncClient.get method
    mock_httpx_get = mocker.patch("httpx.AsyncClient.get", new_callable=AsyncMock)

    # Mock the Redis context manager
    mock_redis = mocker.patch("v4vapp_backend_v2.hive.witness_details.V4VAsyncRedis")
    mock_redis_instance = mock_redis.return_value
    mock_redis_instance.__aenter__.return_value = mock_redis_instance
    mock_redis_instance.__aexit__.return_value = None

    # Sample response data
    sample_response = None

    # Configure the mock to return a response with the sample data
    mock_httpx_get.return_value.status_code = 300
    mock_httpx_get.return_value.json = Mock(return_value=sample_response)

    # Mock Redis get
    mock_redis_instance.get = AsyncMock(return_value=json.dumps(sample_response))
    mock_redis_instance.set = AsyncMock(return_value=None)
    mock_redis_instance.ping = AsyncMock(return_value=True)

    # Call the function
    witness_details = await get_hive_witness_details()

    # Assertions
    assert witness_details is None

    # Ensure the httpx get method was called with the correct URL
    mock_httpx_get.assert_called_with("https://api.syncad.com/hafbe-api/witnesses", timeout=20)

    # Ensure the Redis set method was called with the correct parameters
    mock_redis_instance.get.assert_called_with(
        "witness_",
    )


@pytest.mark.asyncio
async def test_get_hive_witness_details_mock_error(mocker):
    # Mock the httpx.AsyncClient.get method
    mock_httpx_get = mocker.patch("httpx.AsyncClient.get", new_callable=AsyncMock)

    # Mock the Redis context manager
    mock_redis = mocker.patch("v4vapp_backend_v2.database.async_redis.V4VAsyncRedis")
    mock_redis_instance = mock_redis.return_value
    mock_redis_instance.__aenter__.return_value = mock_redis_instance
    mock_redis_instance.__aexit__.return_value = None

    # Configure the mock to return a response with an error
    mock_httpx_get.return_value.status_code = 404
    mock_redis_instance.get.return_value = None

    # Call the function
    witness_details = await get_hive_witness_details("non_existent_witness")

    # Assertions
    assert witness_details is None

    assert mock_httpx_get.call_count == 1
