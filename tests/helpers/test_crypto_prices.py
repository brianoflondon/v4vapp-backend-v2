import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from httpx import Request, Response
from nectar.hive import Hive
from nectar.market import Market

from v4vapp_backend_v2.helpers.crypto_prices import (
    ALL_PRICES_COINGECKO,
    ALL_PRICES_COINMARKETCAP,
    AllQuotes,
    Binance,
    CoinGecko,
    CoinMarketCap,
    HiveInternalMarket,
    QuoteResponse,
)


@pytest.fixture(autouse=True)
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    yield
    # No need to restore the original value, monkeypatch will handle it


def mock_coin_gecko(mocker):
    with open("tests/data/crypto_prices/CoinGecko.json") as f_in:
        coingecko_resp = json.load(f_in).get("raw_response")

    mock_response = Response(
        status_code=200,
        request=Request(method="GET", url=ALL_PRICES_COINGECKO),
        json=coingecko_resp,
    )
    mocker.patch("httpx.AsyncClient.get", new=AsyncMock(return_value=mock_response))
    return coingecko_resp


@pytest.mark.asyncio
async def test_coin_gecko_quote_service(mocker):
    coingecko_resp = mock_coin_gecko(mocker)
    service = CoinGecko()
    quote = await service.get_quote(use_cache=False)
    assert quote is not None
    assert quote.fetch_date is not None
    assert quote.raw_response == coingecko_resp
    quote = await service.get_quote(use_cache=True)
    assert quote is not None
    assert quote.fetch_date is not None
    assert quote.raw_response == coingecko_resp


@pytest.mark.asyncio
async def test_coin_gecko_quote_service_error(mocker):
    service = CoinGecko()

    # Mock the client.get method to return a rate limit error response
    mock_response = Response(
        status_code=429,  # HTTP status code for Too Many Requests (rate limit)
        request=Request(method="GET", url=ALL_PRICES_COINGECKO),
        json={"error": "Rate limit exceeded"},
    )
    mocker.patch("httpx.AsyncClient.get", new=AsyncMock(return_value=mock_response))

    quote = await service.get_quote(use_cache=False)

    assert "Rate limit exceeded" in quote.error


def mock_binance(mocker):
    """
    Mocks the Binance Spot client to return predefined data for testing purposes.

    Args:
        mocker: The pytest-mock fixture used to patch objects during testing.

    Returns:
        dict: The mocked Binance API response loaded from a JSON file.

    Notes:
        - The function patches the `Spot` class in the `v4vapp_backend_v2.helpers.crypto_prices` module.
        - The mocked response is loaded from the file `tests/data/crypto_prices/Binance.json`.
        - The JSON file should contain a key "raw_response" with the expected API response.
    """
    mock_spot_client = mocker.patch("v4vapp_backend_v2.helpers.crypto_prices.Spot")
    with open("tests/data/crypto_prices/Binance.json") as f_in:
        binance_resp = json.load(f_in).get("raw_response")
    mock_spot_client.return_value.book_ticker.return_value = binance_resp
    return binance_resp


@pytest.mark.asyncio
async def test_binance_quote_service(mocker):
    service = Binance()
    binance_resp = mock_binance(mocker)
    quote = await service.get_quote(use_cache=False)
    assert quote is not None
    assert quote.raw_response == binance_resp
    quote = await service.get_quote(use_cache=True)
    assert quote is not None
    assert quote.raw_response == binance_resp


def mock_binance_error(mocker):
    mock_spot_client = mocker.patch("v4vapp_backend_v2.helpers.crypto_prices.Spot")
    mock_spot_client.return_value.book_ticker.side_effect = Exception("Test error")


@pytest.mark.asyncio
async def test_binance_quote_service_error(mocker):
    service = Binance()

    mock_binance_error(mocker)

    quote = await service.get_quote(use_cache=False)

    assert "Test error" in quote.error


def mock_coin_market_cap(mocker):
    with open("tests/data/crypto_prices/CoinMarketCap.json") as f_in:
        coinmarketcap_resp = json.load(f_in).get("raw_response")

    mock_response = Response(
        status_code=200,
        request=Request(
            method="GET",
            url=ALL_PRICES_COINMARKETCAP,
        ),
        json=coinmarketcap_resp,
    )
    mocker.patch("httpx.AsyncClient.get", new=AsyncMock(return_value=mock_response))
    return coinmarketcap_resp


@pytest.mark.asyncio
async def test_coin_market_cap_quote_service(mocker):
    service = CoinMarketCap()

    coinmarketcap_resp = mock_coin_market_cap(mocker)

    quote = await service.get_quote(use_cache=False)
    assert quote is not None
    assert quote.raw_response == coinmarketcap_resp
    quote = await service.get_quote(use_cache=True)
    assert quote is not None
    assert quote.raw_response == coinmarketcap_resp


@pytest.mark.asyncio
async def test_coin_market_cap_quote_service_error(mocker):
    service = CoinMarketCap()

    # Mock the client.get method to return a rate limit error response
    mock_response = Response(
        status_code=429,  # HTTP status code for Too Many Requests (rate limit)
        request=Request(
            method="GET",
            url=ALL_PRICES_COINMARKETCAP,
        ),
        json={"status": {"error_code": 429, "error_message": "Rate limit exceeded"}},
    )
    mocker.patch("httpx.AsyncClient.get", new=AsyncMock(return_value=mock_response))

    quote = await service.get_quote(use_cache=False)

    assert "Rate limit exceeded" in quote.error


def mock_hive_internal_market(mocker):
    # Load mock response data
    with open("tests/data/crypto_prices/HiveInternalMarket.json") as f_in:
        hive_internal_market_resp = json.load(f_in).get("raw_response")

    # Mock the get_hive_client function to return a mock Hive instance
    mock_hive = mocker.MagicMock(spec=Hive)
    mocker.patch("v4vapp_backend_v2.hive.hive_extras.get_hive_client", return_value=mock_hive)

    # Mock the Market class
    mock_market = mocker.MagicMock(spec=Market)
    mocker.patch("v4vapp_backend_v2.hive.hive_extras.Market", return_value=mock_market)

    # Configure the mock market's ticker method to return our test data
    mock_market.ticker.return_value = hive_internal_market_resp
    return hive_internal_market_resp


@pytest.mark.asyncio
async def test_hive_internal_market_service(mocker):
    # Create the service instance
    service = HiveInternalMarket()
    hive_internal_market_resp = mock_hive_internal_market(mocker)
    # Call the service method
    quote = await service.get_quote()
    # Assertions
    assert quote is not None
    assert quote.raw_response == hive_internal_market_resp


@pytest.mark.asyncio
async def test_hive_internal_market_service_error(mocker):
    service = HiveInternalMarket()

    # Mock the get_hive_client function to return a mock Hive instance
    mock_hive = mocker.MagicMock(spec=Hive)
    mocker.patch("v4vapp_backend_v2.hive.hive_extras.get_hive_client", return_value=mock_hive)

    # Mock the Market class
    mock_market = mocker.MagicMock(spec=Market)
    mocker.patch("v4vapp_backend_v2.hive.hive_extras.Market", return_value=mock_market)

    # Configure the mock market's ticker method to raise an exception
    mock_market.ticker.side_effect = Exception("Test error")

    # Call the service method and assert that it raises the expected error
    quote = await service.get_quote(use_cache=False)

    assert "Problem calling Hive Market API Test error" in quote.error


@pytest.mark.asyncio
async def test_get_all_quotes(mocker, set_base_config_path):
    # Load all responses
    with open("tests/data/crypto_prices/CoinGecko.json") as f:
        coingecko_resp = json.load(f).get("raw_response")
    with open("tests/data/crypto_prices/CoinMarketCap.json") as f:
        coinmarketcap_resp = json.load(f).get("raw_response")

    # Define the side_effect function
    def mock_get(url, *args, **kwargs):
        if "coingecko.com" in url:
            return Response(status_code=200, json=coingecko_resp)
        elif "coinmarketcap.com" in url:
            return Response(status_code=200, json=coinmarketcap_resp)
        return Response(status_code=404)  # Default case

    # Mock the Redis check_cache method to always return None
    mocker.patch(
        "v4vapp_backend_v2.helpers.crypto_prices.QuoteService.check_cache",
        return_value=None,
    )
    mocker.patch(
        "v4vapp_backend_v2.helpers.crypto_prices.QuoteService.set_cache",
        return_value=None,
    )
    # Do not use the redis cache at the object level.
    mock_redis = mocker.patch("v4vapp_backend_v2.helpers.crypto_prices.V4VAsyncRedis")
    mock_redis_instance = mock_redis.return_value
    mock_redis_instance.__aenter__.return_value = mock_redis_instance
    mock_redis_instance.__aexit__.return_value = None
    mock_redis_instance.setex = AsyncMock(return_value=None)
    mock_redis_instance.get = AsyncMock(return_value=None)

    # Apply the patch
    mocker.patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=mock_get))
    binance_resp = mock_binance(mocker)  # Binance uses a different client (Spot)
    hive_resp = mock_hive_internal_market(mocker)  # Hive uses its own client

    # Test
    all_quotes = AllQuotes()
    await all_quotes.get_all_quotes()

    # Assertions
    assert all_quotes.quotes["CoinGecko"].raw_response == coingecko_resp
    assert all_quotes.quotes["Binance"].raw_response == binance_resp
    assert all_quotes.quotes["CoinMarketCap"].raw_response == coinmarketcap_resp
    assert all_quotes.quotes["HiveInternalMarket"].raw_response == hive_resp

    # Test the authoritative quote fetch
    quote = all_quotes.quote
    assert quote is not None
    assert quote.error == ""


def load_and_mock_responses(mocker, failing_service):
    # Load all successful responses
    with open("tests/data/crypto_prices/CoinGecko.json") as f:
        coingecko_resp = json.load(f).get("raw_response")
    with open("tests/data/crypto_prices/CoinMarketCap.json") as f:
        coinmarketcap_resp = json.load(f).get("raw_response")
    with open("tests/data/crypto_prices/Binance.json") as f:
        binance_resp = json.load(f).get("raw_response")
    with open("tests/data/crypto_prices/HiveInternalMarket.json") as f:
        hive_resp = json.load(f).get("raw_response")

    # Mock HTTP client for CoinGecko and CoinMarketCap
    def mock_http_get(url, *args, **kwargs):
        if "coingecko.com" in url:
            if failing_service == "CoinGecko":
                return Response(
                    status_code=429,
                    request=Request(method="GET", url=url),
                    json={"error": "Rate limit exceeded"},
                )
            return Response(status_code=200, json=coingecko_resp)
        elif "coinmarketcap.com" in url:
            if failing_service == "CoinMarketCap":
                return Response(
                    status_code=429,
                    request=Request(method="GET", url=url),
                    json={
                        "status": {
                            "error_code": 429,
                            "error_message": "Rate limit exceeded",
                        }
                    },
                )
            return Response(status_code=200, json=coinmarketcap_resp)
        return Response(status_code=404)

    mocker.patch("httpx.AsyncClient.get", new=AsyncMock(side_effect=mock_http_get))

    # Mock Binance
    mock_spot_client = mocker.patch("v4vapp_backend_v2.helpers.crypto_prices.Spot")
    if failing_service == "Binance":
        mock_spot_client.return_value.book_ticker.side_effect = Exception("Binance API error")
    else:
        mock_spot_client.return_value.book_ticker.return_value = binance_resp

    # Mock Hive Internal Market
    _ = mocker.patch("v4vapp_backend_v2.hive.hive_extras.get_hive_client")
    mock_market = mocker.patch("v4vapp_backend_v2.hive.hive_extras.Market")
    if failing_service == "HiveInternalMarket":
        mock_market.return_value.ticker.side_effect = Exception("Hive market error")
    else:
        mock_market.return_value.ticker.return_value = hive_resp
    return coingecko_resp, coinmarketcap_resp, binance_resp, hive_resp


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failing_service",
    ["CoinGecko", "CoinMarketCap", "Binance", "HiveInternalMarket"],
)
async def test_get_all_quotes_with_single_failure(mocker, failing_service):
    """
    Test that AllQuotes handles a single service failure correctly while others succeed.
    Parametrized to test each service failing independently.
    """
    # Do not use the redis cache at the object level.
    mock_redis = mocker.patch("v4vapp_backend_v2.helpers.crypto_prices.V4VAsyncRedis")
    mock_redis_instance = mock_redis.return_value
    mock_redis_instance.__aenter__.return_value = mock_redis_instance
    mock_redis_instance.__aexit__.return_value = None
    mock_redis_instance.setex = AsyncMock(return_value=None)
    mock_redis_instance.get = AsyncMock(return_value=None)

    # Extracted the setup into this function to avoid code duplication
    coingecko_resp, coinmarketcap_resp, binance_resp, hive_resp = load_and_mock_responses(
        mocker, failing_service
    )

    # Execute the test
    all_quotes = AllQuotes()
    await all_quotes.get_all_quotes(use_cache=False)

    # Test the authoritative quote fetch
    quote = all_quotes.quote
    quote_ages = [quote.age_p for quote in all_quotes.quotes.values()]
    for age in quote_ages:
        assert age > 0.0
        assert age < 1000.0
    assert quote is not None
    assert quote.error == ""

    # Assertions based on which service is failing
    if failing_service == "CoinGecko":
        assert all_quotes.quotes[failing_service].error
        assert all_quotes.quotes["CoinMarketCap"].raw_response == coinmarketcap_resp
        assert all_quotes.quotes["Binance"].raw_response == binance_resp
        assert all_quotes.quotes["HiveInternalMarket"].raw_response == hive_resp
        assert all_quotes.quote == all_quotes.quotes["Binance"]
    elif failing_service == "CoinMarketCap":
        assert all_quotes.quotes[failing_service].error
        assert all_quotes.quotes["CoinGecko"].raw_response == coingecko_resp
        assert all_quotes.quotes["Binance"].raw_response == binance_resp
        assert all_quotes.quotes["HiveInternalMarket"].raw_response == hive_resp
        assert all_quotes.quote == all_quotes.quotes["Binance"]
    elif failing_service == "Binance":
        assert all_quotes.quotes[failing_service].error
        assert all_quotes.quotes["CoinGecko"].raw_response == coingecko_resp
        assert all_quotes.quotes["CoinMarketCap"].raw_response == coinmarketcap_resp
        assert all_quotes.quotes["HiveInternalMarket"].raw_response == hive_resp
        assert all_quotes.quote == all_quotes.calculate_average_quote()
    elif failing_service == "HiveInternalMarket":
        assert all_quotes.quotes[failing_service].error
        assert all_quotes.quotes["CoinGecko"].raw_response == coingecko_resp
        assert all_quotes.quotes["CoinMarketCap"].raw_response == coinmarketcap_resp
        assert all_quotes.quotes["Binance"].raw_response == binance_resp
        assert all_quotes.quote.hive_hbd == all_quotes.hive_hbd

    for service_name, quote in all_quotes.quotes.items():
        print(service_name, quote.fetch_date, quote.error)
        if service_name != failing_service:
            assert quote is not None
        assert quote.fetch_date is not None
        assert quote.raw_response is not None


def test_quote_response_fetch_date():
    quote = QuoteResponse()
    assert quote.fetch_date == datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert quote.age > 1742126888  # 55 years in seconds back to Jan 1 1970


async def fetch_all_quote_json_files():
    """
    Fetches all quote data and writes them to JSON files.
    To be run to fetch new test data.

    This function creates an instance of the AllQuotes class, retrieves all quotes,
    and writes each quote to a separate JSON file in the 'tests/data/crypto_prices/'
    directory. The filename for each quote is derived from the service name.

    The JSON files are formatted with an indentation of 2 spaces.

    Raises:
        Any exceptions raised by the AllQuotes class methods or file operations.
    """
    all_quotes = AllQuotes()
    await all_quotes.get_all_quotes(timeout=1000)
    for service_name, quote in all_quotes.quotes.items():
        with open(f"tests/data/crypto_prices/{service_name}.json", "w") as f_out:
            # Write the JSON string directly
            f_out.write(quote.model_dump_json(indent=2))

    with open("tests/data/crypto_prices/all_quotes.json", "w") as f_out:
        json.dump(all_quotes.model_dump(), f_out, indent=2, default=str)


if __name__ == "__main__":
    asyncio.run(fetch_all_quote_json_files())
