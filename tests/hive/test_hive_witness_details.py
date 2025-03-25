import json
from unittest.mock import AsyncMock, Mock

import pytest

from v4vapp_backend_v2.hive.hive_extras import get_hive_witness_details


@pytest.mark.asyncio
async def test_get_hive_witness_details(mocker):
    # Mock the httpx.AsyncClient.get method
    mock_httpx_get = mocker.patch("httpx.AsyncClient.get", new_callable=AsyncMock)

    # Mock the Redis context manager
    mock_redis = mocker.patch("v4vapp_backend_v2.hive.hive_extras.V4VAsyncRedis")
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

    # Ensure the httpx get method was called with the correct URL
    mock_httpx_get.assert_called_with(
        "https://api.syncad.com/hafbe-api/witnesses/brianoflondon", timeout=20
    )

    # Ensure the Redis set method was called with the correct parameters
    mock_redis_instance.set.assert_called_with(
        name="witness_brianoflondon", value=json.dumps(sample_response)
    )


@pytest.mark.asyncio
async def test_get_hive_witness_details_empty(mocker):
    # Mock the httpx.AsyncClient.get method
    mock_httpx_get = mocker.patch("httpx.AsyncClient.get", new_callable=AsyncMock)

    # Mock the Redis context manager
    mock_redis = mocker.patch("v4vapp_backend_v2.hive.hive_extras.V4VAsyncRedis")
    mock_redis_instance = mock_redis.return_value
    mock_redis_instance.__aenter__.return_value = mock_redis_instance
    mock_redis_instance.__aexit__.return_value = None

    # Sample response data
    sample_response = None

    # Configure the mock to return a response with the sample data
    mock_httpx_get.return_value.status_code = 300
    mock_httpx_get.return_value.json.return_value = sample_response

    # Mock Redis get
    mock_redis_instance.get = AsyncMock(return_value=json.dumps(sample_response))

    # Call the function
    witness_details = await get_hive_witness_details()

    # Assertions
    assert witness_details is None

    # Ensure the httpx get method was called with the correct URL
    mock_httpx_get.assert_called_with(
        "https://api.syncad.com/hafbe-api/witnesses", timeout=20
    )

    # Ensure the Redis set method was called with the correct parameters
    mock_redis_instance.get.assert_called_with(
        "witness_",
    )


@pytest.mark.asyncio
async def test_get_hive_witness_details_error(mocker):
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

    # Ensure the httpx get method was called with the correct URL
    mock_httpx_get.assert_called_with(
        "https://api.syncad.com/hafbe-api/witnesses/non_existent_witness", timeout=20
    )
