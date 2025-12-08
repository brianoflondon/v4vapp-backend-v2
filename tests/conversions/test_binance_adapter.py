"""
Unit tests for binance_adapter module.

Tests the Binance implementation of the ExchangeProtocol.
"""

from decimal import Decimal
from unittest.mock import patch

import pytest

from v4vapp_backend_v2.conversion.binance_adapter import BinanceAdapter
from v4vapp_backend_v2.conversion.exchange_protocol import (
    ExchangeBelowMinimumError,
    ExchangeConnectionError,
    ExchangeMinimums,
    ExchangeOrderResult,
)
from v4vapp_backend_v2.helpers.binance_extras import (
    BinanceErrorBadConnection,
    BinanceErrorBelowMinimum,
    MarketOrderResult,
)


class TestBinanceAdapterInit:
    """Tests for BinanceAdapter initialization."""

    def test_adapter_default_testnet_false(self):
        """Test adapter defaults to non-testnet."""
        adapter = BinanceAdapter()
        assert adapter.testnet is False

    def test_adapter_testnet_true(self):
        """Test adapter with testnet flag."""
        adapter = BinanceAdapter(testnet=True)
        assert adapter.testnet is True

    def test_exchange_name(self):
        """Test exchange_name property."""
        adapter = BinanceAdapter()
        assert adapter.exchange_name == "binance"


class TestBinanceAdapterBuildSymbol:
    """Tests for build_symbol method."""

    def test_build_symbol(self):
        """Test building symbol from base and quote assets."""
        adapter = BinanceAdapter()
        assert adapter.build_symbol("HIVE", "BTC") == "HIVEBTC"
        assert adapter.build_symbol("BTC", "USDT") == "BTCUSDT"


class TestBinanceAdapterGetMinOrderRequirements:
    """Tests for get_min_order_requirements method."""

    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_min_order_quantity")
    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_symbol_info")
    def test_get_min_order_requirements_success(self, mock_symbol_info, mock_get_min):
        """Test successful retrieval of minimums."""
        mock_get_min.return_value = (Decimal("1.0"), Decimal("0.0001"))
        mock_symbol_info.return_value = {
            "filters": [
                {"filterType": "LOT_SIZE", "stepSize": "1.00000000"},
            ]
        }

        adapter = BinanceAdapter()
        minimums = adapter.get_min_order_requirements("HIVE", "BTC")

        assert isinstance(minimums, ExchangeMinimums)
        assert minimums.min_qty == Decimal("1.0")
        assert minimums.min_notional == Decimal("0.0001")
        assert minimums.step_size == Decimal("1.00000000")

    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_min_order_quantity")
    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_symbol_info")
    def test_get_min_order_requirements_no_step_size(self, mock_symbol_info, mock_get_min):
        """Test when step size is not in symbol info."""
        mock_get_min.return_value = (Decimal("1.0"), Decimal("0.0001"))
        mock_symbol_info.return_value = {"filters": []}

        adapter = BinanceAdapter()
        minimums = adapter.get_min_order_requirements("HIVE", "BTC")

        assert minimums.step_size == Decimal("0")

    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_min_order_quantity")
    def test_get_min_order_requirements_connection_error(self, mock_get_min):
        """Test handling connection error."""
        mock_get_min.side_effect = BinanceErrorBadConnection("Connection failed")

        adapter = BinanceAdapter()

        with pytest.raises(ExchangeConnectionError) as exc_info:
            adapter.get_min_order_requirements("HIVE", "BTC")

        assert "Connection failed" in str(exc_info.value)


class TestBinanceAdapterGetBalance:
    """Tests for get_balance method."""

    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_balances")
    def test_get_balance_success(self, mock_get_balances):
        """Test successful balance retrieval."""
        mock_get_balances.return_value = {"HIVE": Decimal("1000.5")}

        adapter = BinanceAdapter()
        balance = adapter.get_balance("HIVE")

        assert balance == Decimal("1000.5")

    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_balances")
    def test_get_balance_not_found(self, mock_get_balances):
        """Test balance for asset not found returns 0."""
        mock_get_balances.return_value = {}

        adapter = BinanceAdapter()
        balance = adapter.get_balance("UNKNOWN")

        assert balance == Decimal("0")

    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_balances")
    def test_get_balance_connection_error(self, mock_get_balances):
        """Test handling connection error."""
        mock_get_balances.side_effect = BinanceErrorBadConnection("Connection failed")

        adapter = BinanceAdapter()

        with pytest.raises(ExchangeConnectionError):
            adapter.get_balance("HIVE")


class TestBinanceAdapterGetCurrentPrice:
    """Tests for get_current_price method."""

    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_current_price")
    def test_get_current_price_success(self, mock_get_price):
        """Test successful price retrieval."""
        mock_get_price.return_value = {
            "bid_price": "0.00001234",
            "ask_price": "0.00001240",
            "current_price": "0.00001237",
        }

        adapter = BinanceAdapter()
        price = adapter.get_current_price("HIVE", "BTC")

        assert price == Decimal("0.00001234")  # Uses bid price

    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_current_price")
    def test_get_current_price_connection_error(self, mock_get_price):
        """Test handling connection error."""
        mock_get_price.side_effect = BinanceErrorBadConnection("Connection failed")

        adapter = BinanceAdapter()

        with pytest.raises(ExchangeConnectionError):
            adapter.get_current_price("HIVE", "BTC")


class TestBinanceAdapterMarketSell:
    """Tests for market_sell method."""

    @patch("v4vapp_backend_v2.conversion.binance_adapter.market_sell")
    def test_market_sell_success(self, mock_sell):
        """Test successful market sell."""
        mock_sell.return_value = MarketOrderResult(
            symbol="HIVEBTC",
            order_id=12345,
            client_order_id="test123",
            transact_time=1234567890,
            orig_qty=Decimal("100"),
            executed_qty=Decimal("100"),
            cummulative_quote_qty=Decimal("0.00123"),
            status="FILLED",
            type="MARKET",
            side="SELL",
            avg_price=Decimal("0.0000123"),
            fills=[{"commission": "0.0000001", "commissionAsset": "BTC"}],
            raw_response={"orderId": 12345},
        )

        adapter = BinanceAdapter()
        result = adapter.market_sell("HIVE", "BTC", Decimal("100"))

        assert isinstance(result, ExchangeOrderResult)
        assert result.exchange == "binance"
        assert result.symbol == "HIVEBTC"
        assert result.side == "SELL"
        assert result.executed_qty == Decimal("100")
        assert result.fee == Decimal("0.0000001")
        assert result.fee_asset == "BTC"

    @patch("v4vapp_backend_v2.conversion.binance_adapter.market_sell")
    def test_market_sell_below_minimum(self, mock_sell):
        """Test handling below minimum error."""
        mock_sell.side_effect = BinanceErrorBelowMinimum("Order too small")

        adapter = BinanceAdapter()

        with pytest.raises(ExchangeBelowMinimumError):
            adapter.market_sell("HIVE", "BTC", Decimal("0.5"))

    @patch("v4vapp_backend_v2.conversion.binance_adapter.market_sell")
    def test_market_sell_connection_error(self, mock_sell):
        """Test handling connection error."""
        mock_sell.side_effect = BinanceErrorBadConnection("Connection failed")

        adapter = BinanceAdapter()

        with pytest.raises(ExchangeConnectionError):
            adapter.market_sell("HIVE", "BTC", Decimal("100"))


class TestBinanceAdapterMarketBuy:
    """Tests for market_buy method."""

    @patch("v4vapp_backend_v2.conversion.binance_adapter.market_buy")
    def test_market_buy_success(self, mock_buy):
        """Test successful market buy."""
        mock_buy.return_value = MarketOrderResult(
            symbol="HIVEBTC",
            order_id=54321,
            client_order_id="test456",
            transact_time=1234567890,
            orig_qty=Decimal("100"),
            executed_qty=Decimal("100"),
            cummulative_quote_qty=Decimal("0.00123"),
            status="FILLED",
            type="MARKET",
            side="BUY",
            avg_price=Decimal("0.0000123"),
            fills=[{"commission": "0.01", "commissionAsset": "HIVE"}],
            raw_response={"orderId": 54321},
        )

        adapter = BinanceAdapter()
        result = adapter.market_buy("HIVE", "BTC", Decimal("100"))

        assert isinstance(result, ExchangeOrderResult)
        assert result.exchange == "binance"
        assert result.symbol == "HIVEBTC"
        assert result.side == "BUY"
        assert result.executed_qty == Decimal("100")
        assert result.fee == Decimal("0.01")
        assert result.fee_asset == "HIVE"

    @patch("v4vapp_backend_v2.conversion.binance_adapter.market_buy")
    def test_market_buy_below_minimum(self, mock_buy):
        """Test handling below minimum error."""
        mock_buy.side_effect = BinanceErrorBelowMinimum("Order too small")

        adapter = BinanceAdapter()

        with pytest.raises(ExchangeBelowMinimumError):
            adapter.market_buy("HIVE", "BTC", Decimal("0.5"))

    @patch("v4vapp_backend_v2.conversion.binance_adapter.market_buy")
    def test_market_buy_connection_error(self, mock_buy):
        """Test handling connection error."""
        mock_buy.side_effect = BinanceErrorBadConnection("Connection failed")

        adapter = BinanceAdapter()

        with pytest.raises(ExchangeConnectionError):
            adapter.market_buy("HIVE", "BTC", Decimal("100"))


class TestBinanceAdapterConvertResult:
    """Tests for _convert_result method."""

    def test_convert_result_extracts_fees(self):
        """Test that fees are correctly extracted from fills."""
        adapter = BinanceAdapter()

        binance_result = MarketOrderResult(
            symbol="HIVEBTC",
            order_id=12345,
            client_order_id="test",
            transact_time=1234567890,
            orig_qty=Decimal("100"),
            executed_qty=Decimal("100"),
            cummulative_quote_qty=Decimal("0.00123"),
            status="FILLED",
            type="MARKET",
            side="SELL",
            avg_price=Decimal("0.0000123"),
            fills=[
                {"commission": "0.0000001", "commissionAsset": "BTC"},
                {"commission": "0.0000002", "commissionAsset": "BTC"},
            ],
            raw_response={},
        )

        result = adapter._convert_result(binance_result, "SELL", Decimal("100"))

        assert result.fee == Decimal("0.0000003")  # Sum of all fees
        assert result.fee_asset == "BTC"

    def test_convert_result_empty_fills(self):
        """Test conversion with no fills."""
        adapter = BinanceAdapter()

        binance_result = MarketOrderResult(
            symbol="HIVEBTC",
            order_id=12345,
            client_order_id="test",
            transact_time=1234567890,
            orig_qty=Decimal("100"),
            executed_qty=Decimal("100"),
            cummulative_quote_qty=Decimal("0.00123"),
            status="FILLED",
            type="MARKET",
            side="SELL",
            avg_price=Decimal("0.0000123"),
            fills=[],
            raw_response={},
        )

        result = adapter._convert_result(binance_result, "SELL", Decimal("100"))

        assert result.fee == Decimal("0")
        assert result.fee_asset == ""


class TestBinanceAdapterProtocolCompliance:
    """Tests to verify BinanceAdapter satisfies ExchangeProtocol."""

    def test_adapter_has_all_required_methods(self):
        """Test adapter has all methods required by protocol."""
        adapter = BinanceAdapter()

        assert hasattr(adapter, "exchange_name")
        assert hasattr(adapter, "market_sell")
        assert hasattr(adapter, "market_buy")
        assert hasattr(adapter, "get_min_order_requirements")
        assert hasattr(adapter, "get_current_price")
        assert hasattr(adapter, "get_balance")
        assert callable(adapter.market_sell)
        assert callable(adapter.market_buy)
        assert callable(adapter.get_min_order_requirements)
        assert callable(adapter.get_current_price)
        assert callable(adapter.get_balance)

    def test_adapter_inherits_from_base(self):
        """Test adapter inherits from BaseExchangeAdapter."""
        from v4vapp_backend_v2.conversion.exchange_protocol import BaseExchangeAdapter

        adapter = BinanceAdapter()
        assert isinstance(adapter, BaseExchangeAdapter)
