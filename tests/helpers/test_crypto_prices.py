import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from httpx import Request, Response

from v4vapp_backend_v2.helpers.crypto_prices import (
    Binance,
    BinanceError,
    CoinGecko,
    CoinGeckoError,
    CoinMarketCap,
    CoinMarketCapError,
    AllQuotes,
)


@pytest.fixture
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path
    )
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    yield
    # No need to restore the original value, monkeypatch will handle it


@pytest.mark.asyncio
async def test_coin_gecko_quote_service(mocker):
    with open("tests/data/crypto_prices/coingecko_resp.json") as f_in:
        coingecko_resp = json.load(f_in)

    mock_response = Response(
        status_code=200,
        request=Request(
            method="GET", url="https://api.coingecko.com/api/v3/simple/price"
        ),
        json=coingecko_resp,
    )
    mocker.patch("httpx.AsyncClient.get", new=AsyncMock(return_value=mock_response))
    service = CoinGecko()
    quote = await service.get_quote(use_cache=False)
    assert quote is not None
    assert quote.sats_hive == 285.5003
    assert quote.sats_hbd == 1161.1572
    assert quote.hive_usd == 0.2325
    assert quote.hbd_usd == 0.9456
    assert quote.btc_usd == 81436
    assert quote.hive_hbd == 0.2459
    assert quote.fetch_date is not None
    assert quote.raw_response == coingecko_resp
    assert quote.quote_age < 20


@pytest.mark.asyncio
async def test_coin_gecko_quote_service_error(mocker):
    service = CoinGecko()

    # Mock the client.get method to return a rate limit error response
    mock_response = Response(
        status_code=429,  # HTTP status code for Too Many Requests (rate limit)
        request=Request(
            method="GET", url="https://api.coingecko.com/api/v3/simple/price"
        ),
        json={"error": "Rate limit exceeded"},
    )
    mocker.patch("httpx.AsyncClient.get", new=AsyncMock(return_value=mock_response))

    with pytest.raises(CoinGeckoError) as exc_info:
        await service.get_quote(use_cache=False)

    assert "Rate limit exceeded" in str(exc_info.value)


@pytest.mark.asyncio
async def test_binance_quote_service(mocker, set_base_config_path):
    service = Binance()

    # Mock the Spot client
    mock_spot_client = mocker.patch("v4vapp_backend_v2.helpers.crypto_prices.Spot")

    # Mock the book_ticker method to return the desired response
    with open("tests/data/crypto_prices/binance_resp.json") as f_in:
        book_ticker_response = json.load(f_in)
    mock_spot_client.return_value.book_ticker.return_value = book_ticker_response

    quote = await service.get_quote()
    assert quote is not None
    assert quote.hive_usd == 0.2326
    assert quote.hbd_usd == 1
    assert quote.btc_usd == 81325.2
    assert quote.sats_hive == 286.0122
    assert quote.raw_response is not None


@pytest.mark.asyncio
async def test_binance_quote_service_error(mocker, set_base_config_path):
    service = Binance()

    # Mock the Spot client
    mock_spot_client = mocker.patch("v4vapp_backend_v2.helpers.crypto_prices.Spot")

    # Mock the book_ticker method to raise an exception
    mock_spot_client.return_value.book_ticker.side_effect = Exception("Test error")

    with pytest.raises(BinanceError) as exc_info:
        await service.get_quote(use_cache=False)

    assert "Test error" in str(exc_info.value)


@pytest.mark.asyncio
async def test_coin_market_cap_quote_service(mocker):
    service = CoinMarketCap()

    with open("tests/data/crypto_prices/coinmarketcap_resp.json") as f_in:
        cmc_resp = json.load(f_in)

    mock_response = Response(
        status_code=200,
        request=Request(
            method="GET",
            url="https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
        ),
        json=cmc_resp,
    )
    mocker.patch("httpx.AsyncClient.get", new=AsyncMock(return_value=mock_response))

    quote = await service.get_quote()
    assert quote is not None
    assert quote.raw_response == cmc_resp
    assert quote.hive_usd == 0.2387
    assert quote.hbd_usd == 1.0443
    assert quote.btc_usd == 83259.9


@pytest.mark.asyncio
async def test_coin_market_cap_quote_service_error(mocker):
    service = CoinMarketCap()

    # Mock the client.get method to return a rate limit error response
    mock_response = Response(
        status_code=429,  # HTTP status code for Too Many Requests (rate limit)
        request=Request(
            method="GET",
            url="https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
        ),
        json={"status": {"error_code": 429, "error_message": "Rate limit exceeded"}},
    )
    mocker.patch("httpx.AsyncClient.get", new=AsyncMock(return_value=mock_response))

    with pytest.raises(CoinMarketCapError) as exc_info:
        await service.get_quote(use_cache=False)

    assert "Rate limit exceeded" in str(exc_info.value)


@pytest.mark.asyncio
async def test_get_all_quote():
    all_quotes = AllQuotes()
    await all_quotes.get_all_quotes()
    with open("tests/data/crypto_prices/all_quotes.json", "w") as f_out:
        json.dump(all_quotes.model_dump(), f_out, indent=2, default=str)
