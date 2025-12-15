"""
Unit tests for exchange_protocol module.

Tests the abstract exchange interface and data models.
"""

from decimal import Decimal

import pytest

from v4vapp_backend_v2.conversion.exchange_protocol import (
    BaseExchangeAdapter,
    ExchangeBelowMinimumError,
    ExchangeConnectionError,
    ExchangeError,
    ExchangeInsufficientBalanceError,
    ExchangeMinimums,
    ExchangeOrderResult,
    ExchangeProtocol,
)


class TestExchangeOrderResult:
    """Tests for ExchangeOrderResult model."""

    def test_create_order_result(self):
        """Test creating an order result with all fields."""
        result = ExchangeOrderResult(
            exchange="binance",
            symbol="HIVEBTC",
            order_id="12345",
            side="SELL",
            status="FILLED",
            requested_qty=Decimal("100.5"),
            executed_qty=Decimal("100.5"),
            quote_qty=Decimal("0.00123456"),
            avg_price=Decimal("0.0000123"),
            fee_msats=Decimal("0.0000001"),
            fee_original=Decimal("0.00001"),
            fee_asset="BTC",
            raw_response={"orderId": 12345},
        )

        assert result.exchange == "binance"
        assert result.symbol == "HIVEBTC"
        assert result.order_id == "12345"
        assert result.side == "SELL"
        assert result.requested_qty == Decimal("100.5")
        assert result.executed_qty == Decimal("100.5")
        assert result.quote_qty == Decimal("0.00123456")
        assert result.status == "FILLED"
        assert result.avg_price == Decimal("0.0000123")
        assert result.fee_msats == Decimal("0.0000001")
        assert result.fee_asset == "BTC"
        assert result.trade_quote is None  # Optional, defaults to None

    def test_create_order_result_buy_side(self):
        """Test creating a buy order result."""
        result = ExchangeOrderResult(
            exchange="binance",
            symbol="BTCUSDT",
            order_id="67890",
            side="BUY",
            status="FILLED",
            requested_qty=Decimal("0.001"),
            executed_qty=Decimal("0.001"),
            quote_qty=Decimal("50.25"),
            avg_price=Decimal("50250"),
            fee_msats=Decimal("0.05"),
            fee_original=Decimal("0.00005"),
            fee_asset="USDT",
            raw_response={},
        )

        assert result.side == "BUY"
        assert result.symbol == "BTCUSDT"

    def test_order_result_partial_fill(self):
        """Test order result with partial fill."""
        result = ExchangeOrderResult(
            exchange="binance",
            symbol="HIVEBTC",
            order_id="11111",
            side="SELL",
            status="PARTIALLY_FILLED",
            requested_qty=Decimal("1000"),
            executed_qty=Decimal("500"),  # Only half filled
            quote_qty=Decimal("0.005"),
            avg_price=Decimal("0.00001"),
            fee_msats=Decimal("0"),
            fee_original=Decimal("0"),
            fee_asset="BTC",
            raw_response={},
        )

        assert result.requested_qty == Decimal("1000")
        assert result.executed_qty == Decimal("500")
        assert result.status == "PARTIALLY_FILLED"

    def test_order_result_zero_values(self):
        """Test order result with zero values (edge case)."""
        result = ExchangeOrderResult(
            exchange="binance",
            symbol="HIVEBTC",
            order_id="0",
            side="SELL",
            status="CANCELED",
            requested_qty=Decimal("0"),
            executed_qty=Decimal("0"),
            quote_qty=Decimal("0"),
            avg_price=Decimal("0"),
            fee_msats=Decimal("0"),
            fee_original=Decimal("0"),
            fee_asset="BTC",
            raw_response={},
        )

        assert result.requested_qty == Decimal("0")
        assert result.executed_qty == Decimal("0")


class TestExchangeMinimums:
    """Tests for ExchangeMinimums model."""

    def test_create_exchange_minimums(self):
        """Test creating exchange minimums."""
        minimums = ExchangeMinimums(
            min_qty=Decimal("1.0"),
            min_notional=Decimal("0.0001"),
        )

        assert minimums.min_qty == Decimal("1.0")
        assert minimums.min_notional == Decimal("0.0001")

    def test_minimums_with_step_size(self):
        """Test minimums with step size."""
        minimums = ExchangeMinimums(
            min_qty=Decimal("10"),
            min_notional=Decimal("0.001"),
            step_size=Decimal("1"),
        )

        assert minimums.min_qty == Decimal("10")
        assert minimums.min_notional == Decimal("0.001")
        assert minimums.step_size == Decimal("1")

    def test_minimums_default_step_size(self):
        """Test that step_size defaults to 0."""
        minimums = ExchangeMinimums(
            min_qty=Decimal("1.0"),
            min_notional=Decimal("0.0001"),
        )

        assert minimums.step_size == Decimal("0")


class TestExchangeExceptions:
    """Tests for exchange exception classes."""

    def test_exchange_error_base(self):
        """Test base ExchangeError."""
        error = ExchangeError("Test error")
        assert str(error) == "Test error"
        assert isinstance(error, Exception)

    def test_exchange_connection_error(self):
        """Test ExchangeConnectionError."""
        error = ExchangeConnectionError("Connection failed")
        assert str(error) == "Connection failed"
        assert isinstance(error, ExchangeError)

    def test_exchange_below_minimum_error(self):
        """Test ExchangeBelowMinimumError."""
        error = ExchangeBelowMinimumError("Order too small")
        assert str(error) == "Order too small"
        assert isinstance(error, ExchangeError)

    def test_exchange_insufficient_balance_error(self):
        """Test ExchangeInsufficientBalanceError."""
        error = ExchangeInsufficientBalanceError("Not enough funds")
        assert str(error) == "Not enough funds"
        assert isinstance(error, ExchangeError)


class TestExchangeProtocol:
    """Tests for ExchangeProtocol interface."""

    def test_protocol_defines_required_methods(self):
        """Test that protocol defines expected methods."""
        # Verify the protocol has the expected method signatures
        assert hasattr(ExchangeProtocol, "exchange_name")
        assert hasattr(ExchangeProtocol, "market_sell")
        assert hasattr(ExchangeProtocol, "market_buy")
        assert hasattr(ExchangeProtocol, "get_min_order_requirements")
        assert hasattr(ExchangeProtocol, "get_current_price")
        assert hasattr(ExchangeProtocol, "get_balance")

    def test_concrete_implementation_satisfies_protocol(self):
        """Test that a concrete class can satisfy the protocol."""

        class MockExchange:
            """Mock exchange implementation."""

            @property
            def exchange_name(self) -> str:
                return "mock"

            def market_sell(
                self, base_asset: str, quote_asset: str, quantity: Decimal
            ) -> ExchangeOrderResult:
                return ExchangeOrderResult(
                    exchange="mock",
                    symbol=f"{base_asset}{quote_asset}",
                    order_id="1",
                    side="SELL",
                    status="FILLED",
                    requested_qty=quantity,
                    executed_qty=quantity,
                    quote_qty=Decimal("0.001"),
                    avg_price=Decimal("0.00001"),
                    fee_msats=Decimal("0"),
                    fee_original=Decimal("0"),
                    fee_asset=quote_asset,
                    raw_response={},
                )

            def market_buy(
                self, base_asset: str, quote_asset: str, quantity: Decimal
            ) -> ExchangeOrderResult:
                return ExchangeOrderResult(
                    exchange="mock",
                    symbol=f"{base_asset}{quote_asset}",
                    order_id="2",
                    side="BUY",
                    status="FILLED",
                    requested_qty=quantity,
                    executed_qty=quantity,
                    quote_qty=Decimal("50.0"),
                    avg_price=Decimal("0.00001"),
                    fee_msats=Decimal("0"),
                    fee_original=Decimal("0"),
                    fee_asset=quote_asset,
                    raw_response={},
                )

            def get_min_order_requirements(
                self, base_asset: str, quote_asset: str
            ) -> ExchangeMinimums:
                return ExchangeMinimums(
                    min_qty=Decimal("1.0"),
                    min_notional=Decimal("0.0001"),
                )

            def get_current_price(self, base_asset: str, quote_asset: str) -> Decimal:
                return Decimal("0.00001234")

            def get_balance(self, asset: str) -> Decimal:
                return Decimal("1000")

        # Create instance and verify it works
        exchange = MockExchange()

        # Test sell
        sell_result = exchange.market_sell("HIVE", "BTC", Decimal("100"))
        assert sell_result.symbol == "HIVEBTC"
        assert sell_result.side == "SELL"

        # Test buy
        buy_result = exchange.market_buy("HIVE", "BTC", Decimal("100"))
        assert buy_result.symbol == "HIVEBTC"
        assert buy_result.side == "BUY"

        # Test get_min_order_requirements
        minimums = exchange.get_min_order_requirements("HIVE", "BTC")
        assert minimums.min_qty == Decimal("1.0")

        # Test get_current_price
        price = exchange.get_current_price("HIVE", "BTC")
        assert price == Decimal("0.00001234")

        # Test get_balance
        balance = exchange.get_balance("HIVE")
        assert balance == Decimal("1000")

    def test_protocol_type_checking(self):
        """Test that protocol can be used for type checking."""
        from typing import Protocol

        # ExchangeProtocol should be a Protocol
        assert issubclass(type(ExchangeProtocol), type(Protocol))


class TestBaseExchangeAdapter:
    """Tests for BaseExchangeAdapter abstract class."""

    def test_cannot_instantiate_directly(self):
        """Test that BaseExchangeAdapter cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseExchangeAdapter()

    def test_concrete_implementation(self):
        """Test that a concrete implementation works."""

        class ConcreteAdapter(BaseExchangeAdapter):
            @property
            def exchange_name(self) -> str:
                return "test_exchange"

            def get_min_order_requirements(
                self, base_asset: str, quote_asset: str
            ) -> ExchangeMinimums:
                return ExchangeMinimums(
                    min_qty=Decimal("1"),
                    min_notional=Decimal("0.0001"),
                )

            def get_balance(self, asset: str) -> Decimal:
                return Decimal("100")

            def get_current_price(self, base_asset: str, quote_asset: str) -> Decimal:
                return Decimal("0.00001")

            def market_sell(
                self, base_asset: str, quote_asset: str, quantity: Decimal
            ) -> ExchangeOrderResult:
                return ExchangeOrderResult(
                    exchange=self.exchange_name,
                    symbol=f"{base_asset}{quote_asset}",
                    order_id="1",
                    side="SELL",
                    status="FILLED",
                    requested_qty=quantity,
                    executed_qty=quantity,
                    quote_qty=quantity * Decimal("0.00001"),
                    avg_price=Decimal("0.00001"),
                    fee_msats=Decimal("0"),
                    fee_original=Decimal("0"),
                    fee_asset=quote_asset,
                    raw_response={},
                )

            def market_buy(
                self, base_asset: str, quote_asset: str, quantity: Decimal
            ) -> ExchangeOrderResult:
                return ExchangeOrderResult(
                    exchange=self.exchange_name,
                    symbol=f"{base_asset}{quote_asset}",
                    order_id="2",
                    side="BUY",
                    status="FILLED",
                    requested_qty=quantity,
                    executed_qty=quantity,
                    quote_qty=quantity * Decimal("0.00001"),
                    avg_price=Decimal("0.00001"),
                    fee_msats=Decimal("0"),
                    fee_original=Decimal("0"),
                    fee_asset=quote_asset,
                    raw_response={},
                )

        adapter = ConcreteAdapter()
        assert adapter.exchange_name == "test_exchange"
        assert adapter.testnet is False

    def test_testnet_flag(self):
        """Test testnet flag in adapter."""

        class ConcreteAdapter(BaseExchangeAdapter):
            @property
            def exchange_name(self) -> str:
                return "test"

            def get_min_order_requirements(
                self, base_asset: str, quote_asset: str
            ) -> ExchangeMinimums:
                return ExchangeMinimums(min_qty=Decimal("1"), min_notional=Decimal("0.0001"))

            def get_balance(self, asset: str) -> Decimal:
                return Decimal("0")

            def get_current_price(self, base_asset: str, quote_asset: str) -> Decimal:
                return Decimal("0")

            def market_sell(
                self, base_asset: str, quote_asset: str, quantity: Decimal
            ) -> ExchangeOrderResult:
                raise NotImplementedError

            def market_buy(
                self, base_asset: str, quote_asset: str, quantity: Decimal
            ) -> ExchangeOrderResult:
                raise NotImplementedError

        adapter = ConcreteAdapter(testnet=True)
        assert adapter.testnet is True
