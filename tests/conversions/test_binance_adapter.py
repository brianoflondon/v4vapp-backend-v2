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

    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_mid_price")
    def test_get_current_price_success(self, mock_get_price):
        """Test successful price retrieval."""
        mock_get_price.return_value = Decimal("0.00001237")

        adapter = BinanceAdapter()
        price = adapter.get_current_price("HIVE", "BTC")

        assert price == Decimal("0.00001237")  # Uses current price

    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_mid_price")
    def test_get_current_price_connection_error(self, mock_get_price):
        """Test handling connection error."""
        mock_get_price.side_effect = BinanceErrorBadConnection("Connection failed")

        adapter = BinanceAdapter()

        with pytest.raises(ExchangeConnectionError):
            adapter.get_current_price("HIVE", "BTC")


class TestBinanceAdapterMarketSell:
    """Tests for market_sell method."""

    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_mid_price")
    @patch("v4vapp_backend_v2.conversion.binance_adapter.market_sell")
    def test_market_sell_success(self, mock_sell, mock_get_price):
        """Test successful market sell with BNB commission (mainnet behavior)."""
        mock_get_price.return_value = Decimal("0.002")
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
            fills=[
                {
                    "price": "0.0000123",
                    "qty": "100",
                    "commission": "0.00008532",
                    "commissionAsset": "BNB",
                    "tradeId": 12345,
                }
            ],
            raw_response={"orderId": 12345},
        )

        adapter = BinanceAdapter()
        result = adapter.market_sell("HIVE", "BTC", Decimal("100"))

        assert isinstance(result, ExchangeOrderResult)
        assert result.exchange == "binance"
        assert result.symbol == "HIVEBTC"
        assert result.side == "SELL"
        assert result.executed_qty == Decimal("100")
        assert result.fee_msats == Decimal("17064")
        assert result.fee_asset == "BNB"
        # Verify trade_quote is included
        assert result.trade_quote is not None
        assert result.trade_quote.source == "binance_trade"
        assert result.base_asset == "HIVE"
        assert result.quote_asset == "BTC"

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

    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_mid_price")
    @patch("v4vapp_backend_v2.conversion.binance_adapter.market_buy")
    def test_market_buy_success(self, mock_buy, mock_get_price):
        """Test successful market buy with BNB commission (mainnet behavior)."""
        mock_get_price.return_value = Decimal("0.002")
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
            fills=[
                {
                    "price": "0.0000123",
                    "qty": "100",
                    "commission": "0.00006730",
                    "commissionAsset": "BNB",
                    "tradeId": 54321,
                }
            ],
            raw_response={"orderId": 54321},
        )

        adapter = BinanceAdapter()
        result = adapter.market_buy("HIVE", "BTC", Decimal("100"))

        assert isinstance(result, ExchangeOrderResult)
        assert result.exchange == "binance"
        assert result.symbol == "HIVEBTC"
        assert result.side == "BUY"
        assert result.executed_qty == Decimal("100")
        assert result.fee_msats == Decimal("13460")
        assert result.fee_asset == "BNB"
        # Verify trade_quote is included
        assert result.trade_quote is not None
        assert result.trade_quote.source == "binance_trade"
        assert result.base_asset == "HIVE"
        assert result.quote_asset == "BTC"

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

    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_mid_price")
    def test_convert_result_extracts_fees(self, mock_get_price):
        """Test that fees are correctly extracted from fills with BNB commission."""
        mock_get_price.return_value = Decimal("0.002")
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
                {
                    "price": "0.0000123",
                    "qty": "60",
                    "commission": "0.00006730",
                    "commissionAsset": "BNB",
                    "tradeId": 1,
                },
                {
                    "price": "0.0000123",
                    "qty": "40",
                    "commission": "0.00000756",
                    "commissionAsset": "BNB",
                    "tradeId": 2,
                },
            ],
            raw_response={},
        )

        result = adapter._convert_result(binance_result, "SELL", Decimal("100"), "HIVE", "BTC")

        assert result.fee_msats == Decimal("14972")  # Converted to msats
        assert result.fee_asset == "BNB"
        assert result.trade_quote is not None
        assert result.trade_quote.source == "binance_trade"

    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_mid_price")
    def test_convert_result_multi_fill_mainnet_style(self, mock_get_price):
        """Test conversion with multiple fills matching real mainnet response.

        This tests a scenario based on actual mainnet data where an order
        was filled across 4 separate trades, each with its own BNB commission.
        """
        mock_get_price.return_value = Decimal("0.002")
        adapter = BinanceAdapter()

        # This matches the structure from mainnet: 1084 HIVE sold across 4 fills
        binance_result = MarketOrderResult(
            symbol="HIVEBTC",
            order_id=165516618,
            client_order_id="7LAlRFbQ7fwrBVkwsP5Wpc",
            transact_time=1765147140145,
            orig_qty=Decimal("1084"),
            executed_qty=Decimal("1084"),
            cummulative_quote_qty=Decimal("0.00120324"),
            status="FILLED",
            type="LIMIT",
            side="SELL",
            avg_price=Decimal("0.00000111"),  # 0.00120324 / 1084
            fills=[
                {
                    "price": "0.00000111",
                    "qty": "800.00000000",
                    "commission": "0.00006730",
                    "commissionAsset": "BNB",
                    "tradeId": 8229963,
                },
                {
                    "price": "0.00000111",
                    "qty": "92.00000000",
                    "commission": "0.00000756",
                    "commissionAsset": "BNB",
                    "tradeId": 8229964,
                },
                {
                    "price": "0.00000111",
                    "qty": "179.00000000",
                    "commission": "0.00001512",
                    "commissionAsset": "BNB",
                    "tradeId": 8229965,
                },
                {
                    "price": "0.00000111",
                    "qty": "13.00000000",
                    "commission": "0.00000075",
                    "commissionAsset": "BNB",
                    "tradeId": 8229966,
                },
            ],
            raw_response={"orderId": 165516618},
        )

        result = adapter._convert_result(binance_result, "SELL", Decimal("1084"), "HIVE", "BTC")

        # Total fee should be sum of all fill commissions
        expected_fee = (
            Decimal("0.00006730")
            + Decimal("0.00000756")
            + Decimal("0.00001512")
            + Decimal("0.00000075")
        )
        assert result.fee_msats == Decimal("18146")  # Converted to msats
        assert result.fee_asset == "BNB"
        assert result.executed_qty == Decimal("1084")
        assert result.quote_qty == Decimal("0.00120324")
        assert result.status == "FILLED"
        assert result.trade_quote is not None
        assert result.base_asset == "HIVE"
        assert result.quote_asset == "BTC"

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

        result = adapter._convert_result(binance_result, "SELL", Decimal("100"), "HIVE", "BTC")

        assert result.fee_msats == Decimal("0")
        assert result.fee_asset == ""
        assert result.trade_quote is not None


class TestBinanceAdapterFeeConversion:
    """Tests for fee conversion to CryptoConv (msats tracking)."""

    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_mid_price")
    def test_fee_conversion_bnb_to_msats(self, mock_get_price):
        """Test that BNB fees are correctly converted to msats via BTC."""
        # Mock BNBBTC mid-price: 1 BNB = 0.01 BTC (for easy calculation)
        mock_get_price.return_value = Decimal("0.01")

        adapter = BinanceAdapter()
        fee_bnb = Decimal("0.00009073")  # Real mainnet fee example

        fee_conv = adapter._convert_fee_to_msats(fee_bnb, "BNB")

        # 0.00009073 BNB * 0.01 BTC/BNB = 0.0000009073 BTC
        # 0.0000009073 BTC * 100,000,000 sats/BTC = 90.73 sats
        # 90.73 sats * 1000 msats/sat = 90730 msats
        assert fee_conv == Decimal("90730")

    def test_fee_conversion_btc_direct(self):
        """Test that BTC fees don't need price lookup."""
        adapter = BinanceAdapter()
        fee_btc = Decimal("0.00000050")  # 50 sats

        fee_conv = adapter._convert_fee_to_msats(fee_btc, "BTC")

        assert fee_conv == Decimal("50000")

    def test_fee_conversion_zero_fee(self):
        """Test that zero fee returns Decimal(0)."""
        adapter = BinanceAdapter()

        fee_msats = adapter._convert_fee_to_msats(Decimal("0"), "BNB")

        assert fee_msats == Decimal("0")

    @patch("v4vapp_backend_v2.conversion.binance_adapter.get_mid_price")
    def test_convert_result_includes_fee_conv(self, mock_get_price):
        """Test that _convert_result includes fee_conv in result."""
        # Mock BNBBTC mid-price
        mock_get_price.return_value = Decimal("0.01")

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
                {
                    "price": "0.0000123",
                    "qty": "100",
                    "commission": "0.00008532",
                    "commissionAsset": "BNB",
                    "tradeId": 12345,
                }
            ],
            raw_response={},
        )

        result = adapter._convert_result(binance_result, "SELL", Decimal("100"), "HIVE", "BTC")

        assert result.fee_msats > 0
        assert result.trade_quote is not None
        assert result.trade_quote.sats_hive > 0

    def test_convert_result_empty_fills_no_fee_conv(self):
        """Test that empty fills result in zero fee_msats but still has trade_quote."""
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

        result = adapter._convert_result(binance_result, "SELL", Decimal("100"), "HIVE", "BTC")

        assert result.fee_msats == Decimal("0")
        assert result.trade_quote is not None


class TestBinanceAdapterBuildTradeQuote:
    """Tests for _build_trade_quote method."""

    def test_build_trade_quote_hive_btc(self):
        """Test building trade quote for HIVE/BTC pair.

        The trade_quote now fetches market prices and calculates hive_usd based on
        the trade's avg_price and market btc_usd rate, so that sats_hive reflects
        the actual trade execution rate.
        """
        adapter = BinanceAdapter()
        avg_price = Decimal("0.0000123")  # 1 HIVE = 0.0000123 BTC

        quote = adapter._build_trade_quote("HIVE", "BTC", avg_price, {"test": "data"})

        assert quote.source == "binance_trade"
        # btc_usd comes from market quote, not fixed to 1
        assert quote.btc_usd > 0
        # hive_usd is calculated as avg_price * btc_usd to produce correct sats_hive
        # sats_hive = (SATS_PER_BTC / btc_usd) * hive_usd = avg_price * SATS_PER_BTC
        expected_sats_hive = avg_price * Decimal("100_000_000")
        assert quote.sats_hive == expected_sats_hive.quantize(Decimal("0.000001"))
        assert quote.fetch_date is not None
        # hbd_usd and hive_hbd should come from market quote
        assert quote.hbd_usd > 0
        assert quote.hive_hbd > 0

    def test_build_trade_quote_hive_btc_realistic_price(self):
        """Test trade quote with realistic HIVE/BTC price."""
        adapter = BinanceAdapter()
        # Realistic price: 1 HIVE ≈ 111 sats = 0.00000111 BTC
        avg_price = Decimal("0.00000111")

        quote = adapter._build_trade_quote("HIVE", "BTC", avg_price, {})

        # sats_hive should be 0.00000111 * 100_000_000 = 111
        assert quote.sats_hive == Decimal("111.000000")

    def test_build_trade_quote_hbd_btc(self):
        """Test building trade quote for HBD/BTC pair."""
        adapter = BinanceAdapter()
        avg_price = Decimal("0.000010")  # 1 HBD = 0.00001 BTC

        quote = adapter._build_trade_quote("HBD", "BTC", avg_price, {})

        assert quote.source == "binance_trade"
        # btc_usd comes from market quote
        assert quote.btc_usd > 0
        # sats_hbd should be 0.00001 * 100_000_000 = 1000
        assert quote.sats_hbd == Decimal("1000.000000")
        # hive_usd should come from market quote
        assert quote.hive_usd > 0

    def test_build_trade_quote_other_pair(self):
        """Test building trade quote for non-HIVE/HBD pairs returns market quote."""
        adapter = BinanceAdapter()
        avg_price = Decimal("0.05")  # Some other pair

        quote = adapter._build_trade_quote("ETH", "BTC", avg_price, {})

        assert quote.source == "binance_trade"
        # For other pairs, uses market quote values
        assert quote.btc_usd > 0
        assert quote.hive_usd > 0

    def test_build_trade_quote_testnet(self):
        """Test trade quote includes testnet in source when using testnet."""
        adapter = BinanceAdapter(testnet=True)
        avg_price = Decimal("0.0000123")

        quote = adapter._build_trade_quote("HIVE", "BTC", avg_price, {})

        assert quote.source == "binance_testnet_trade"


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
            fills=[
                {
                    "price": "0.0000123",
                    "qty": "199",
                    "commission": "0.00008532",
                    "commissionAsset": "BNB",
                    "tradeId": 12345,
                }
            ],
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
            fills=[
                {
                    "price": "0.0000123",
                    "qty": "100",
                    "commission": "0.00006730",
                    "commissionAsset": "BNB",
                    "tradeId": 54321,
                }
            ],
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
