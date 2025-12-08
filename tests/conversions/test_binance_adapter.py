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


class TestBinanceAdapterAssetDecimals:
    """Tests for asset decimal precision and rounding."""

    def test_get_asset_decimals_known_assets(self):
        """Test get_asset_decimals returns correct values for known assets."""
        adapter = BinanceAdapter()

        assert adapter.get_asset_decimals("HIVE") == 0
        assert adapter.get_asset_decimals("BTC") == 8
        assert adapter.get_asset_decimals("BNB") == 2
        assert adapter.get_asset_decimals("USDT") == 2

    def test_get_asset_decimals_unknown_asset(self):
        """Test get_asset_decimals returns default for unknown assets."""
        adapter = BinanceAdapter()

        # Unknown assets should get DEFAULT_DECIMALS (8)
        assert adapter.get_asset_decimals("UNKNOWN") == BinanceAdapter.DEFAULT_DECIMALS
        assert adapter.get_asset_decimals("XYZ") == 8

    def test_round_quantity_hive_integer(self):
        """Test HIVE is rounded to whole numbers."""
        adapter = BinanceAdapter()

        # Should round down to nearest integer
        assert adapter.round_quantity("HIVE", Decimal("199.999")) == Decimal("199")
        assert adapter.round_quantity("HIVE", Decimal("100.5")) == Decimal("100")
        assert adapter.round_quantity("HIVE", Decimal("50.1")) == Decimal("50")

    def test_round_quantity_hive_already_integer(self):
        """Test HIVE integers are unchanged."""
        adapter = BinanceAdapter()

        assert adapter.round_quantity("HIVE", Decimal("100")) == Decimal("100")
        assert adapter.round_quantity("HIVE", Decimal("1")) == Decimal("1")

    def test_round_quantity_btc_8_decimals(self):
        """Test BTC is rounded to 8 decimal places."""
        adapter = BinanceAdapter()

        assert adapter.round_quantity("BTC", Decimal("0.123456789")) == Decimal("0.12345678")
        assert adapter.round_quantity("BTC", Decimal("1.000000001")) == Decimal("1.00000000")

    def test_round_quantity_bnb_2_decimals(self):
        """Test BNB is rounded to 2 decimal places."""
        adapter = BinanceAdapter()

        assert adapter.round_quantity("BNB", Decimal("10.555")) == Decimal("10.55")
        assert adapter.round_quantity("BNB", Decimal("5.999")) == Decimal("5.99")

    def test_round_quantity_always_rounds_down(self):
        """Test that rounding always uses ROUND_DOWN."""
        adapter = BinanceAdapter()

        # Even with .9999... it should round down
        assert adapter.round_quantity("HIVE", Decimal("99.9999")) == Decimal("99")
        assert adapter.round_quantity("BNB", Decimal("99.999")) == Decimal("99.99")

    def test_round_quantity_small_amounts_become_zero(self):
        """Test that very small HIVE amounts become 0."""
        adapter = BinanceAdapter()

        assert adapter.round_quantity("HIVE", Decimal("0.5")) == Decimal("0")
        assert adapter.round_quantity("HIVE", Decimal("0.999")) == Decimal("0")

    @patch("v4vapp_backend_v2.conversion.binance_adapter.market_sell")
    def test_market_sell_rounds_quantity(self, mock_sell):
        """Test market_sell rounds quantity before calling Binance."""
        mock_sell.return_value = MarketOrderResult(
            symbol="HIVEBTC",
            order_id=12345,
            client_order_id="test123",
            transact_time=1234567890,
            orig_qty=Decimal("199"),  # Rounded from 199.999
            executed_qty=Decimal("199"),
            cummulative_quote_qty=Decimal("0.00245"),
            status="FILLED",
            type="MARKET",
            side="SELL",
            avg_price=Decimal("0.0000123"),
            fills=[{"commission": "0.0000001", "commissionAsset": "BTC"}],
            raw_response={"orderId": 12345},
        )

        adapter = BinanceAdapter()
        result = adapter.market_sell("HIVE", "BTC", Decimal("199.999"))

        # Verify the call to market_sell used rounded quantity
        mock_sell.assert_called_once()
        call_kwargs = mock_sell.call_args.kwargs
        assert call_kwargs["quantity"] == Decimal("199")  # Not 199.999

    @patch("v4vapp_backend_v2.conversion.binance_adapter.market_buy")
    def test_market_buy_rounds_quantity(self, mock_buy):
        """Test market_buy rounds quantity before calling Binance."""
        mock_buy.return_value = MarketOrderResult(
            symbol="HIVEBTC",
            order_id=54321,
            client_order_id="test456",
            transact_time=1234567890,
            orig_qty=Decimal("100"),  # Rounded from 100.5
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
        result = adapter.market_buy("HIVE", "BTC", Decimal("100.5"))

        # Verify the call to market_buy used rounded quantity
        mock_buy.assert_called_once()
        call_kwargs = mock_buy.call_args.kwargs
        assert call_kwargs["quantity"] == Decimal("100")  # Not 100.5

    def test_market_sell_below_minimum_after_rounding(self):
        """Test market_sell raises error when rounding makes quantity 0."""
        adapter = BinanceAdapter()

        with pytest.raises(ExchangeBelowMinimumError) as exc_info:
            adapter.market_sell("HIVE", "BTC", Decimal("0.5"))

        assert "rounds to 0" in str(exc_info.value)

    def test_market_buy_below_minimum_after_rounding(self):
        """Test market_buy raises error when rounding makes quantity 0."""
        adapter = BinanceAdapter()

        with pytest.raises(ExchangeBelowMinimumError) as exc_info:
            adapter.market_buy("HIVE", "BTC", Decimal("0.999"))

        assert "rounds to 0" in str(exc_info.value)


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
