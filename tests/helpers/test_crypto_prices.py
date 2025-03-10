from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from httpx import Request, Response

from v4vapp_backend_v2.helpers.crypto_prices import (
    BinanceQuoteService,
    CoinGeckoError,
    CoinGeckoQuoteService,
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
    raw_response = {
        "bitcoin": {"btc": 1.0, "usd": 81436, "eur": 75267, "aud": 128894},
        "hive": {
            "btc": 2.85e-06,
            "usd": 0.232532,
            "eur": 0.214918,
            "aud": 0.368044,
        },
        "hive_dollar": {
            "btc": 1.161e-05,
            "usd": 0.945628,
            "eur": 0.873998,
            "aud": 1.5,
        },
    }
    mock_response = Response(
        status_code=200,
        request=Request(
            method="GET", url="https://api.coingecko.com/api/v3/simple/price"
        ),
        json=raw_response,
    )
    mocker.patch("httpx.AsyncClient.get", new=AsyncMock(return_value=mock_response))
    service = CoinGeckoQuoteService()
    quote = await service.get_quote()
    assert quote is not None
    assert quote.sats_hive == 285.5003
    assert quote.sats_hbd == 1161.1572
    assert quote.hive_usd == 0.2325
    assert quote.hbd_usd == 0.9456
    assert quote.btc_usd == 81436
    assert quote.hive_hbd == 0.2459
    assert quote.fetch_date is not None
    assert quote.raw_response == raw_response
    assert quote.quote_age < 20


@pytest.mark.asyncio
async def test_coin_gecko_quote_service_error(mocker):
    service = CoinGeckoQuoteService()

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
        await service.get_quote()

    assert "Rate limit exceeded" in str(exc_info.value)


@pytest.mark.asyncio
async def test_binance_quote_service(mocker, set_base_config_path):
    service = BinanceQuoteService()

    # Mock the Spot client
    mock_spot_client = mocker.patch("v4vapp_backend_v2.helpers.crypto_prices.Spot")

    # Mock the book_ticker method to return the desired response
    mock_spot_client.return_value.book_ticker.return_value = [
        {
            "symbol": "BTCUSDT",
            "bidPrice": "81325.23000000",
            "bidQty": "0.28212000",
            "askPrice": "81325.24000000",
            "askQty": "1.92630000",
        },
        {
            "symbol": "HIVEBTC",
            "bidPrice": "0.00000285",
            "bidQty": "31190.00000000",
            "askPrice": "0.00000287",
            "askQty": "13158.00000000",
        },
        {
            "symbol": "HIVEUSDT",
            "bidPrice": "0.23250000",
            "bidQty": "4245.00000000",
            "askPrice": "0.23270000",
            "askQty": "7161.00000000",
        },
    ]

    quote = await service.get_quote()
    assert quote is not None
    assert quote.hive_usd == 0.2326
    assert quote.hbd_usd == 1
    assert quote.btc_usd == 81325.2
    assert quote.sats_hive == 286.0122
    assert quote.raw_response is not None
