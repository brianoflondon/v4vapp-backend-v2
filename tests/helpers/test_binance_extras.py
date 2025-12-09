"""
Unit tests for the binance_extras module.
"""

import os
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from binance.error import ClientError

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.helpers.binance_extras import (
    BinanceErrorBadConnection,
    BinanceErrorBelowMinimum,
    BinanceErrorLowBalance,
    MarketOrderResult,
    MarketSellResult,
    get_balances,
    get_client,
    get_current_price,
    get_min_order_quantity,
    get_symbol_info,
    market_buy,
    market_order,
    market_sell,
    market_sell_to_btc,
)


@pytest.fixture(autouse=True)
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    """
    Fixture to set up test configuration paths and environment variables.
    Uses the same pattern as test_crypto_prices.py
    """
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )

    # Set environment variables for Binance testnet credentials if not already set
    # These can be overridden by actual environment variables
    if not os.getenv("BINANCE_TESTNET_API_KEY"):
        monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "test_binance_testnet_api_key")
    if not os.getenv("BINANCE_TESTNET_API_SECRET"):
        monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "test_binance_testnet_api_secret")

    yield
    InternalConfig().shutdown()  # Ensure proper cleanup after tests


class TestGetClient:
    """Tests for the get_client function."""

    def test_get_client_mainnet(self, mocker, monkeypatch):
        """Test getting a mainnet Binance client when config is set to mainnet."""
        # Reset InternalConfig singleton to allow fresh initialization
        monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)

        mock_client = MagicMock()
        mock_spot = mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.Client",
            return_value=mock_client,
        )

        # Mock the binance_config to return mainnet mode
        mock_mainnet_config = MagicMock()
        mock_mainnet_config.resolved_api_key = "mainnet_api_key"
        mock_mainnet_config.resolved_api_secret = "mainnet_api_secret"

        mock_binance_config = MagicMock()
        mock_binance_config.use_testnet = False  # Explicitly set to False
        mock_binance_config.mainnet = mock_mainnet_config

        # Patch the InternalConfig to return our mock config
        mock_internal_config_instance = MagicMock()
        mock_internal_config_instance.binance_config = mock_binance_config
        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.InternalConfig",
            return_value=mock_internal_config_instance,
        )

        client = get_client(testnet=False)

        assert client == mock_client
        mock_spot.assert_called_once()
        # Verify it was NOT called with testnet base_url
        call_kwargs = mock_spot.call_args[1]
        assert "base_url" not in call_kwargs

    def test_get_client_testnet(self, mocker):
        """Test getting a testnet Binance client."""
        mock_client = MagicMock()
        mock_spot = mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.Client",
            return_value=mock_client,
        )

        client = get_client(testnet=True)

        assert client == mock_client
        mock_spot.assert_called_once()
        # Verify it was called with testnet base_url
        call_kwargs = mock_spot.call_args[1]
        assert call_kwargs.get("base_url") == "https://testnet.binance.vision"

    def test_get_client_exception(self, mocker):
        """Test that exceptions are re-raised from get_client."""
        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.Client",
            side_effect=Exception("Connection failed"),
        )

        with pytest.raises(Exception, match="Connection failed"):
            get_client(testnet=True)


class TestGetBalances:
    """Tests for the get_balances function."""

    def test_get_balances_success(self, mocker):
        """Test successful retrieval of balances."""
        mock_client = MagicMock()
        mock_client.account.return_value = {
            "balances": [
                {"asset": "BTC", "free": "0.5"},
                {"asset": "USDT", "free": "1000.0"},
                {"asset": "ETH", "free": "2.5"},
                {"asset": "DOGE", "free": "100.0"},  # Not requested
            ]
        }
        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        symbols = ["BTC", "USDT", "ETH"]
        balances = get_balances(symbols, testnet=True)

        assert balances["BTC"] == Decimal("0.5")
        assert balances["USDT"] == Decimal("1000.0")
        assert balances["ETH"] == Decimal("2.5")
        assert balances["SATS"] == Decimal("50000000")  # 0.5 BTC * 100_000_000
        assert "DOGE" not in balances

    def test_get_balances_with_btc_sats_conversion(self, mocker):
        """Test that SATS is calculated when BTC balance exists."""
        mock_client = MagicMock()
        mock_client.account.return_value = {
            "balances": [
                {"asset": "BTC", "free": "1.0"},
            ]
        }
        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        balances = get_balances(["BTC"], testnet=True)

        assert balances["BTC"] == Decimal("1.0")
        assert balances["SATS"] == Decimal("100000000")

    def test_get_balances_zero_btc_no_sats(self, mocker):
        """Test that SATS is not added when BTC balance is zero."""
        mock_client = MagicMock()
        mock_client.account.return_value = {
            "balances": [
                {"asset": "BTC", "free": "0.0"},
                {"asset": "USDT", "free": "500.0"},
            ]
        }
        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        balances = get_balances(["BTC", "USDT"], testnet=True)

        assert balances["BTC"] == Decimal("0.0")
        assert balances["USDT"] == Decimal("500.0")
        assert "SATS" not in balances

    def test_get_balances_empty_symbols(self, mocker):
        """Test getting balances with empty symbols list."""
        mock_client = MagicMock()
        mock_client.account.return_value = {
            "balances": [
                {"asset": "BTC", "free": "0.5"},
            ]
        }
        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        balances = get_balances([], testnet=True)

        assert balances == {}

    def test_get_balances_missing_symbol(self, mocker):
        """Test that requested symbols not in account are initialized to 0."""
        mock_client = MagicMock()
        mock_client.account.return_value = {
            "balances": [
                {"asset": "BTC", "free": "0.5"},
            ]
        }
        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        balances = get_balances(["BTC", "XRP"], testnet=True)

        assert balances["BTC"] == Decimal("0.5")
        assert balances["XRP"] == Decimal("0.0")

    def test_get_balances_client_error(self, mocker):
        """Test handling of Binance ClientError."""
        mock_client = MagicMock()
        mock_client.account.side_effect = ClientError(
            status_code=403,
            error_code=-2015,
            error_message="Invalid API-key, IP, or permissions for action.",
            header={},
        )
        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        with pytest.raises(BinanceErrorBadConnection) as exc_info:
            get_balances(["BTC"], testnet=True)

        assert "Invalid API-key, IP, or permissions for action." in str(exc_info.value)

    def test_get_balances_generic_exception(self, mocker):
        """Test handling of generic exceptions."""
        mock_client = MagicMock()
        mock_client.account.side_effect = Exception("Network timeout")
        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        with pytest.raises(BinanceErrorBadConnection):
            get_balances(["BTC"], testnet=True)


class TestGetCurrentPrice:
    """Tests for the get_current_price function."""

    def test_get_current_price_success(self, mocker):
        """Test successful price retrieval."""
        mock_client = MagicMock()
        mock_client.book_ticker.return_value = {
            "symbol": "BTCUSDT",
            "bidPrice": "50000.00",
            "bidQty": "1.0",
            "askPrice": "50001.00",
            "askQty": "1.0",
        }
        mock_client.ticker_price.return_value = {
            "symbol": "BTCUSDT",
            "price": "50000.50",
        }
        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        price = get_current_price("BTCUSDT", testnet=False)

        assert price["ask_price"] == "50001.00"
        assert price["bid_price"] == "50000.00"
        assert price["current_price"] == "50000.50"
        mock_client.book_ticker.assert_called_once_with("BTCUSDT")
        mock_client.ticker_price.assert_called_once_with("BTCUSDT")

    def test_get_current_price_testnet(self, mocker):
        """Test price retrieval from testnet."""
        mock_client = MagicMock()
        mock_client.book_ticker.return_value = {
            "symbol": "ETHUSDT",
            "bidPrice": "3000.00",
            "askPrice": "3001.00",
        }
        mock_client.ticker_price.return_value = {
            "symbol": "ETHUSDT",
            "price": "3000.50",
        }
        mock_get_client = mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        price = get_current_price("ETHUSDT", testnet=True)

        mock_get_client.assert_called_once_with(True)
        assert price["ask_price"] == "3001.00"
        assert price["bid_price"] == "3000.00"
        assert price["current_price"] == "3000.50"

    def test_get_current_price_different_symbols(self, mocker):
        """Test price retrieval for different trading pairs."""
        mock_client = MagicMock()
        mock_client.book_ticker.return_value = {
            "symbol": "HIVEUSDT",
            "bidPrice": "0.25",
            "askPrice": "0.26",
        }
        mock_client.ticker_price.return_value = {
            "symbol": "HIVEUSDT",
            "price": "0.255",
        }
        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        price = get_current_price("HIVEUSDT")

        assert price["ask_price"] == "0.26"
        assert price["bid_price"] == "0.25"
        assert price["current_price"] == "0.255"


class TestExceptionClasses:
    """Tests for custom exception classes."""

    def test_binance_error_low_balance(self):
        """Test BinanceErrorLowBalance exception."""
        with pytest.raises(BinanceErrorLowBalance):
            raise BinanceErrorLowBalance("Insufficient balance")

    def test_binance_error_bad_connection(self):
        """Test BinanceErrorBadConnection exception."""
        with pytest.raises(BinanceErrorBadConnection):
            raise BinanceErrorBadConnection("Connection failed")

    def test_binance_error_below_minimum(self):
        """Test BinanceErrorBelowMinimum exception."""
        with pytest.raises(BinanceErrorBelowMinimum):
            raise BinanceErrorBelowMinimum("Order below minimum")

    def test_exception_message(self):
        """Test exception messages are preserved."""
        try:
            raise BinanceErrorLowBalance("Low BTC balance")
        except BinanceErrorLowBalance as e:
            assert str(e) == "Low BTC balance"

        try:
            raise BinanceErrorBadConnection("API key invalid")
        except BinanceErrorBadConnection as e:
            assert str(e) == "API key invalid"

        try:
            raise BinanceErrorBelowMinimum("Below minimum order")
        except BinanceErrorBelowMinimum as e:
            assert str(e) == "Below minimum order"


class TestMarketSellResult:
    """Tests for the MarketSellResult model."""

    def test_from_binance_response(self):
        """Test creating MarketSellResult from Binance API response."""
        response = {
            "symbol": "HIVEBTC",
            "orderId": 12345,
            "clientOrderId": "abc123",
            "transactTime": 1702000000000,
            "origQty": "100.00000000",
            "executedQty": "100.00000000",
            "cummulativeQuoteQty": "0.00010000",
            "status": "FILLED",
            "type": "MARKET",
            "side": "SELL",
            "fills": [
                {
                    "price": "0.00000100",
                    "qty": "100.00000000",
                    "commission": "0.00000001",
                    "commissionAsset": "BTC",
                    "tradeId": 1,
                }
            ],
        }

        result = MarketSellResult.from_binance_response(response)

        assert result.symbol == "HIVEBTC"
        assert result.order_id == 12345
        assert result.client_order_id == "abc123"
        assert result.transact_time == 1702000000000
        assert result.orig_qty == Decimal("100")
        assert result.executed_qty == Decimal("100")
        assert result.cummulative_quote_qty == Decimal("0.0001")
        assert result.status == "FILLED"
        assert result.type == "MARKET"
        assert result.side == "SELL"
        assert result.avg_price == Decimal("0.000001")  # 0.0001 BTC / 100 HIVE
        assert len(result.fills) == 1
        assert result.raw_response == response

    def test_from_binance_response_zero_executed_qty(self):
        """Test MarketSellResult when no quantity was executed."""
        response = {
            "symbol": "HIVEBTC",
            "orderId": 12345,
            "clientOrderId": "abc123",
            "transactTime": 1702000000000,
            "origQty": "100.00000000",
            "executedQty": "0.00000000",
            "cummulativeQuoteQty": "0.00000000",
            "status": "EXPIRED",
            "type": "MARKET",
            "side": "SELL",
            "fills": [],
        }

        result = MarketSellResult.from_binance_response(response)

        assert result.executed_qty == Decimal("0")
        assert result.avg_price == Decimal("0")  # Should not divide by zero

    def test_from_binance_response_partial_fill(self):
        """Test MarketSellResult with partial fill and multiple fills."""
        response = {
            "symbol": "HIVEBTC",
            "orderId": 12345,
            "clientOrderId": "abc123",
            "transactTime": 1702000000000,
            "origQty": "100.00000000",
            "executedQty": "80.00000000",
            "cummulativeQuoteQty": "0.00009600",
            "status": "PARTIALLY_FILLED",
            "type": "MARKET",
            "side": "SELL",
            "fills": [
                {
                    "price": "0.00000120",
                    "qty": "50.00000000",
                    "commission": "0.00000001",
                    "commissionAsset": "BTC",
                    "tradeId": 1,
                },
                {
                    "price": "0.00000120",
                    "qty": "30.00000000",
                    "commission": "0.00000001",
                    "commissionAsset": "BTC",
                    "tradeId": 2,
                },
            ],
        }

        result = MarketSellResult.from_binance_response(response)

        assert result.executed_qty == Decimal("80")
        assert result.avg_price == Decimal("0.0000012")  # 0.000096 BTC / 80 HIVE
        assert len(result.fills) == 2


class TestGetSymbolInfo:
    """Tests for the get_symbol_info function."""

    def test_get_symbol_info_success(self, mocker):
        """Test successful retrieval of symbol info."""
        mock_client = MagicMock()
        mock_client.exchange_info.return_value = {
            "symbols": [
                {
                    "symbol": "HIVEBTC",
                    "status": "TRADING",
                    "filters": [
                        {"filterType": "LOT_SIZE", "minQty": "1.00000000"},
                        {"filterType": "MIN_NOTIONAL", "minNotional": "0.00010000"},
                    ],
                }
            ]
        }
        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        info = get_symbol_info("HIVEBTC", testnet=True)

        assert info["symbol"] == "HIVEBTC"
        assert info["status"] == "TRADING"
        mock_client.exchange_info.assert_called_once_with(symbol="HIVEBTC")

    def test_get_symbol_info_not_found(self, mocker):
        """Test when symbol is not found."""
        mock_client = MagicMock()
        mock_client.exchange_info.return_value = {"symbols": []}
        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        info = get_symbol_info("INVALIDBTC", testnet=True)

        assert info == {}


class TestGetMinOrderQuantity:
    """Tests for the get_min_order_quantity function."""

    def test_get_min_order_quantity_success(self, mocker):
        """Test successful retrieval of minimum order quantity."""
        mock_client = MagicMock()
        mock_client.exchange_info.return_value = {
            "symbols": [
                {
                    "symbol": "HIVEBTC",
                    "filters": [
                        {
                            "filterType": "LOT_SIZE",
                            "minQty": "1.00000000",
                            "maxQty": "9000000.00000000",
                            "stepSize": "1.00000000",
                        },
                        {
                            "filterType": "MIN_NOTIONAL",
                            "minNotional": "0.00010000",
                        },
                    ],
                }
            ]
        }
        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        min_qty, min_notional = get_min_order_quantity("HIVEBTC", testnet=True)

        assert min_qty == Decimal("1")
        assert min_notional == Decimal("0.0001")

    def test_get_min_order_quantity_notional_filter(self, mocker):
        """Test with NOTIONAL filter instead of MIN_NOTIONAL."""
        mock_client = MagicMock()
        mock_client.exchange_info.return_value = {
            "symbols": [
                {
                    "symbol": "HIVEBTC",
                    "filters": [
                        {"filterType": "LOT_SIZE", "minQty": "1.00000000"},
                        {"filterType": "NOTIONAL", "minNotional": "0.00020000"},
                    ],
                }
            ]
        }
        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        min_qty, min_notional = get_min_order_quantity("HIVEBTC", testnet=True)

        assert min_qty == Decimal("1")
        assert min_notional == Decimal("0.0002")

    def test_get_min_order_quantity_no_filters(self, mocker):
        """Test when filters are empty."""
        mock_client = MagicMock()
        mock_client.exchange_info.return_value = {
            "symbols": [{"symbol": "HIVEBTC", "filters": []}]
        }
        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        min_qty, min_notional = get_min_order_quantity("HIVEBTC", testnet=True)

        assert min_qty == Decimal("0")
        assert min_notional == Decimal("0")


class TestMarketSellToBtc:
    """Tests for the market_sell_to_btc function."""

    def _mock_exchange_info(self, mocker, min_qty="1.0", min_notional="0.00005"):
        """Helper to mock exchange_info response."""
        return {
            "symbols": [
                {
                    "symbol": "HIVEBTC",
                    "filters": [
                        {"filterType": "LOT_SIZE", "minQty": str(min_qty)},
                        {"filterType": "MIN_NOTIONAL", "minNotional": str(min_notional)},
                    ],
                }
            ]
        }

    def _mock_book_ticker(self):
        """Helper to mock book_ticker response."""
        return {
            "symbol": "HIVEBTC",
            "bidPrice": "0.00000100",
            "askPrice": "0.00000101",
        }

    def _mock_ticker_price(self):
        """Helper to mock ticker_price response."""
        return {"symbol": "HIVEBTC", "price": "0.00000100"}

    def _mock_new_order_response(self, quantity="100"):
        """Helper to create mock new_order response."""
        qty = Decimal(quantity)
        return {
            "symbol": "HIVEBTC",
            "orderId": 12345,
            "clientOrderId": "testOrder123",
            "transactTime": 1702000000000,
            "origQty": str(qty),
            "executedQty": str(qty),
            "cummulativeQuoteQty": str(qty * Decimal("0.000001")),
            "status": "FILLED",
            "type": "MARKET",
            "side": "SELL",
            "fills": [
                {
                    "price": "0.00000100",
                    "qty": str(qty),
                    "commission": "0.00000001",
                    "commissionAsset": "BTC",
                    "tradeId": 1,
                }
            ],
        }

    def test_market_sell_to_btc_success(self, mocker):
        """Test successful market sell order."""
        mock_client = MagicMock()
        mock_client.exchange_info.return_value = self._mock_exchange_info(mocker)
        mock_client.book_ticker.return_value = self._mock_book_ticker()
        mock_client.ticker_price.return_value = self._mock_ticker_price()
        mock_client.new_order.return_value = self._mock_new_order_response("100")

        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        result = market_sell_to_btc(from_asset="HIVE", quantity=Decimal("100"), testnet=True)

        assert result.symbol == "HIVEBTC"
        assert result.status == "FILLED"
        assert result.executed_qty == Decimal("100")
        assert result.side == "SELL"
        assert result.type == "MARKET"
        mock_client.new_order.assert_called_once_with(
            symbol="HIVEBTC",
            side="SELL",
            type="MARKET",
            quantity="100",
        )

    def test_market_sell_to_btc_below_min_lot_size(self, mocker):
        """Test that BinanceErrorBelowMinimum is raised when quantity is below min lot size."""
        mock_client = MagicMock()
        mock_client.exchange_info.return_value = self._mock_exchange_info(mocker, min_qty="10.0")

        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        with pytest.raises(BinanceErrorBelowMinimum) as exc_info:
            market_sell_to_btc(from_asset="HIVE", quantity=Decimal("5"), testnet=True)

        assert "below minimum lot size" in str(exc_info.value)
        assert "5" in str(exc_info.value)
        assert "10" in str(exc_info.value)

    def test_market_sell_to_btc_below_min_notional(self, mocker):
        """Test that BinanceErrorBelowMinimum is raised when notional value is too low."""
        mock_client = MagicMock()
        # Set min_notional to 0.001 BTC (higher than 100 * 0.000001 = 0.0001)
        mock_client.exchange_info.return_value = self._mock_exchange_info(
            mocker, min_qty="1.0", min_notional="0.001"
        )
        mock_client.book_ticker.return_value = self._mock_book_ticker()
        mock_client.ticker_price.return_value = self._mock_ticker_price()

        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        # 100 HIVE * 0.000001 BTC = 0.0001 BTC, which is below 0.001 min notional
        with pytest.raises(BinanceErrorBelowMinimum) as exc_info:
            market_sell_to_btc(from_asset="HIVE", quantity=Decimal("100"), testnet=True)

        assert "below minimum" in str(exc_info.value)

    def test_market_sell_to_btc_client_error(self, mocker):
        """Test handling of Binance ClientError during order placement."""
        mock_client = MagicMock()
        mock_client.exchange_info.return_value = self._mock_exchange_info(mocker)
        mock_client.book_ticker.return_value = self._mock_book_ticker()
        mock_client.ticker_price.return_value = self._mock_ticker_price()
        mock_client.new_order.side_effect = ClientError(
            status_code=400,
            error_code=-1013,
            error_message="Filter failure: LOT_SIZE",
            header={},
        )

        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        with pytest.raises(BinanceErrorBadConnection) as exc_info:
            market_sell_to_btc(from_asset="HIVE", quantity=Decimal("100"), testnet=True)

        assert "Filter failure" in str(exc_info.value)

    def test_market_sell_to_btc_generic_error(self, mocker):
        """Test handling of generic exceptions during order placement."""
        mock_client = MagicMock()
        mock_client.exchange_info.return_value = self._mock_exchange_info(mocker)
        mock_client.book_ticker.return_value = self._mock_book_ticker()
        mock_client.ticker_price.return_value = self._mock_ticker_price()
        mock_client.new_order.side_effect = Exception("Network timeout")

        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        with pytest.raises(BinanceErrorBadConnection) as exc_info:
            market_sell_to_btc(from_asset="HIVE", quantity=Decimal("100"), testnet=True)

        assert "Network timeout" in str(exc_info.value)

    def test_market_sell_to_btc_records_fills(self, mocker):
        """Test that the result correctly records fill details."""
        mock_client = MagicMock()
        mock_client.exchange_info.return_value = self._mock_exchange_info(mocker)
        mock_client.book_ticker.return_value = self._mock_book_ticker()
        mock_client.ticker_price.return_value = self._mock_ticker_price()

        # Multiple fills at different prices
        response = {
            "symbol": "HIVEBTC",
            "orderId": 12345,
            "clientOrderId": "testOrder123",
            "transactTime": 1702000000000,
            "origQty": "100.00000000",
            "executedQty": "100.00000000",
            "cummulativeQuoteQty": "0.00010500",  # Total BTC received
            "status": "FILLED",
            "type": "MARKET",
            "side": "SELL",
            "fills": [
                {
                    "price": "0.00000100",
                    "qty": "60.00000000",
                    "commission": "0.00000001",
                    "commissionAsset": "BTC",
                    "tradeId": 1,
                },
                {
                    "price": "0.00000110",
                    "qty": "40.00000000",
                    "commission": "0.00000001",
                    "commissionAsset": "BTC",
                    "tradeId": 2,
                },
            ],
        }
        mock_client.new_order.return_value = response

        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        result = market_sell_to_btc(from_asset="HIVE", quantity=Decimal("100"), testnet=True)

        assert len(result.fills) == 2
        assert result.cummulative_quote_qty == Decimal("0.000105")  # Total BTC received
        assert result.avg_price == Decimal("0.00000105")  # 0.000105 / 100
        assert result.raw_response == response


class TestMarketOrder:
    """Tests for the generic market_order function."""

    def _mock_exchange_info(self, symbol="BTCUSDT", min_qty="0.001", min_notional="10"):
        """Helper to mock exchange_info response."""
        return {
            "symbols": [
                {
                    "symbol": symbol,
                    "filters": [
                        {"filterType": "LOT_SIZE", "minQty": str(min_qty)},
                        {"filterType": "MIN_NOTIONAL", "minNotional": str(min_notional)},
                    ],
                }
            ]
        }

    def _mock_book_ticker(self, symbol="BTCUSDT", bid="50000.00", ask="50001.00"):
        """Helper to mock book_ticker response."""
        return {
            "symbol": symbol,
            "bidPrice": bid,
            "askPrice": ask,
        }

    def _mock_ticker_price(self, symbol="BTCUSDT", price="50000.50"):
        """Helper to mock ticker_price response."""
        return {"symbol": symbol, "price": price}

    def _mock_new_order_response(self, symbol="BTCUSDT", side="BUY", quantity="0.01"):
        """Helper to create mock new_order response."""
        qty = Decimal(quantity)
        price = Decimal("50000")
        return {
            "symbol": symbol,
            "orderId": 12345,
            "clientOrderId": "testOrder123",
            "transactTime": 1702000000000,
            "origQty": str(qty),
            "executedQty": str(qty),
            "cummulativeQuoteQty": str(qty * price),
            "status": "FILLED",
            "type": "MARKET",
            "side": side,
            "fills": [
                {
                    "price": str(price),
                    "qty": str(qty),
                    "commission": "0.00001",
                    "commissionAsset": "BNB",
                    "tradeId": 1,
                }
            ],
        }

    def test_market_order_buy_success(self, mocker):
        """Test successful market buy order."""
        mock_client = MagicMock()
        mock_client.exchange_info.return_value = self._mock_exchange_info()
        mock_client.book_ticker.return_value = self._mock_book_ticker()
        mock_client.ticker_price.return_value = self._mock_ticker_price()
        mock_client.new_order.return_value = self._mock_new_order_response(side="BUY")

        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        result = market_order(symbol="BTCUSDT", side="BUY", quantity=Decimal("0.01"), testnet=True)

        assert result.symbol == "BTCUSDT"
        assert result.side == "BUY"
        assert result.status == "FILLED"
        assert result.executed_qty == Decimal("0.01")
        mock_client.new_order.assert_called_once_with(
            symbol="BTCUSDT",
            side="BUY",
            type="MARKET",
            quantity="0.01",
        )

    def test_market_order_sell_success(self, mocker):
        """Test successful market sell order."""
        mock_client = MagicMock()
        mock_client.exchange_info.return_value = self._mock_exchange_info()
        mock_client.book_ticker.return_value = self._mock_book_ticker()
        mock_client.ticker_price.return_value = self._mock_ticker_price()
        mock_client.new_order.return_value = self._mock_new_order_response(side="SELL")

        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        result = market_order(
            symbol="BTCUSDT", side="SELL", quantity=Decimal("0.01"), testnet=True
        )

        assert result.symbol == "BTCUSDT"
        assert result.side == "SELL"
        assert result.status == "FILLED"
        mock_client.new_order.assert_called_once_with(
            symbol="BTCUSDT",
            side="SELL",
            type="MARKET",
            quantity="0.01",
        )

    def test_market_order_invalid_side(self, mocker):
        """Test that ValueError is raised for invalid side."""
        with pytest.raises(ValueError, match="Invalid side"):
            market_order(symbol="BTCUSDT", side="INVALID", quantity=Decimal("0.01"), testnet=True)

    def test_market_order_case_insensitive_side(self, mocker):
        """Test that side is case-insensitive."""
        mock_client = MagicMock()
        mock_client.exchange_info.return_value = self._mock_exchange_info()
        mock_client.book_ticker.return_value = self._mock_book_ticker()
        mock_client.ticker_price.return_value = self._mock_ticker_price()
        mock_client.new_order.return_value = self._mock_new_order_response(side="BUY")

        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        # Test lowercase
        result = market_order(symbol="BTCUSDT", side="buy", quantity=Decimal("0.01"), testnet=True)
        assert result.side == "BUY"

    def test_market_order_below_min_qty(self, mocker):
        """Test that BinanceErrorBelowMinimum is raised for quantity below minimum."""
        mock_client = MagicMock()
        mock_client.exchange_info.return_value = self._mock_exchange_info(min_qty="0.01")

        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        with pytest.raises(BinanceErrorBelowMinimum) as exc_info:
            market_order(symbol="BTCUSDT", side="BUY", quantity=Decimal("0.001"), testnet=True)

        assert "below minimum lot size" in str(exc_info.value)

    def test_market_order_below_min_notional(self, mocker):
        """Test that BinanceErrorBelowMinimum is raised for notional below minimum."""
        mock_client = MagicMock()
        # min_notional=1000 USDT, but 0.01 BTC * 50000 = 500 USDT
        mock_client.exchange_info.return_value = self._mock_exchange_info(
            min_qty="0.001", min_notional="1000"
        )
        mock_client.book_ticker.return_value = self._mock_book_ticker()
        mock_client.ticker_price.return_value = self._mock_ticker_price()

        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        with pytest.raises(BinanceErrorBelowMinimum) as exc_info:
            market_order(symbol="BTCUSDT", side="BUY", quantity=Decimal("0.01"), testnet=True)

        assert "below minimum" in str(exc_info.value)

    def test_market_order_uses_ask_price_for_buy(self, mocker):
        """Test that buy orders use ask price for notional estimation."""
        mock_client = MagicMock()
        # Set min_notional just under what we'd get with ask price
        # 0.01 * 50001 = 500.01, so min_notional of 500 should pass with ask
        mock_client.exchange_info.return_value = self._mock_exchange_info(
            min_qty="0.001", min_notional="500"
        )
        mock_client.book_ticker.return_value = self._mock_book_ticker(
            bid="49999.00", ask="50001.00"
        )
        mock_client.ticker_price.return_value = self._mock_ticker_price()
        mock_client.new_order.return_value = self._mock_new_order_response(side="BUY")

        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        # Should pass because 0.01 * 50001 (ask) = 500.01 > 500
        result = market_order(symbol="BTCUSDT", side="BUY", quantity=Decimal("0.01"), testnet=True)
        assert result.status == "FILLED"

    def test_market_order_uses_bid_price_for_sell(self, mocker):
        """Test that sell orders use bid price for notional estimation."""
        mock_client = MagicMock()
        # 0.01 * 50000 = 500, so min_notional of 501 should fail with bid
        mock_client.exchange_info.return_value = self._mock_exchange_info(
            min_qty="0.001", min_notional="501"
        )
        mock_client.book_ticker.return_value = self._mock_book_ticker(
            bid="50000.00", ask="50002.00"
        )
        mock_client.ticker_price.return_value = self._mock_ticker_price()

        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        # Should fail because 0.01 * 50000 (bid) = 500 < 501
        with pytest.raises(BinanceErrorBelowMinimum):
            market_order(symbol="BTCUSDT", side="SELL", quantity=Decimal("0.01"), testnet=True)

    def test_market_order_different_symbols(self, mocker):
        """Test market order with different trading pairs."""
        mock_client = MagicMock()
        mock_client.exchange_info.return_value = self._mock_exchange_info(
            symbol="ETHBTC", min_qty="0.01", min_notional="0.0001"
        )
        mock_client.book_ticker.return_value = self._mock_book_ticker(
            symbol="ETHBTC", bid="0.05", ask="0.051"
        )
        mock_client.ticker_price.return_value = self._mock_ticker_price(
            symbol="ETHBTC", price="0.0505"
        )
        mock_client.new_order.return_value = {
            "symbol": "ETHBTC",
            "orderId": 99999,
            "clientOrderId": "ethOrder",
            "transactTime": 1702000000000,
            "origQty": "1.0",
            "executedQty": "1.0",
            "cummulativeQuoteQty": "0.05",
            "status": "FILLED",
            "type": "MARKET",
            "side": "SELL",
            "fills": [],
        }

        mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.get_client",
            return_value=mock_client,
        )

        result = market_order(symbol="ETHBTC", side="SELL", quantity=Decimal("1.0"), testnet=True)

        assert result.symbol == "ETHBTC"
        assert result.cummulative_quote_qty == Decimal("0.05")


class TestMarketSell:
    """Tests for the market_sell convenience function."""

    def test_market_sell_calls_market_order(self, mocker):
        """Test that market_sell correctly calls market_order with side='SELL'."""
        mock_market_order = mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.market_order",
            return_value=MagicMock(),
        )

        market_sell(symbol="BTCUSDT", quantity=Decimal("0.01"), testnet=True)

        mock_market_order.assert_called_once_with(
            symbol="BTCUSDT",
            side="SELL",
            quantity=Decimal("0.01"),
            testnet=True,
        )


class TestMarketBuy:
    """Tests for the market_buy convenience function."""

    def test_market_buy_calls_market_order(self, mocker):
        """Test that market_buy correctly calls market_order with side='BUY'."""
        mock_market_order = mocker.patch(
            "v4vapp_backend_v2.helpers.binance_extras.market_order",
            return_value=MagicMock(),
        )

        market_buy(symbol="BTCUSDT", quantity=Decimal("0.01"), testnet=True)

        mock_market_order.assert_called_once_with(
            symbol="BTCUSDT",
            side="BUY",
            quantity=Decimal("0.01"),
            testnet=True,
        )


class TestMarketOrderResult:
    """Tests for the MarketOrderResult model (renamed from MarketSellResult)."""

    def test_market_order_result_alias(self):
        """Test that MarketSellResult is an alias for MarketOrderResult."""
        assert MarketSellResult is MarketOrderResult

    def test_from_binance_response_buy_order(self):
        """Test creating MarketOrderResult from a buy order response."""
        response = {
            "symbol": "BTCUSDT",
            "orderId": 12345,
            "clientOrderId": "buy123",
            "transactTime": 1702000000000,
            "origQty": "0.01",
            "executedQty": "0.01",
            "cummulativeQuoteQty": "500.00",
            "status": "FILLED",
            "type": "MARKET",
            "side": "BUY",
            "fills": [
                {
                    "price": "50000.00",
                    "qty": "0.01",
                    "commission": "0.00001",
                    "commissionAsset": "BNB",
                    "tradeId": 1,
                }
            ],
        }

        result = MarketOrderResult.from_binance_response(response)

        assert result.symbol == "BTCUSDT"
        assert result.side == "BUY"
        assert result.executed_qty == Decimal("0.01")
        assert result.cummulative_quote_qty == Decimal("500")
        assert result.avg_price == Decimal("50000")  # 500 / 0.01


# def test_my_test():
#     result = market_order("HIVEBTC", "SELL", Decimal("1000"), testnet=True)

#     pprint(result.model_dump())
