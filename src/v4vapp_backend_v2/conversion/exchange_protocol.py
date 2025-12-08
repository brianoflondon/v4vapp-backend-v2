"""
Abstract protocol for exchange operations.

This module defines the interface that any exchange adapter must implement,
allowing the rebalancing system to work with multiple exchanges (Binance, etc.)
without coupling to a specific implementation.
"""

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any, Dict, Protocol, runtime_checkable

from pydantic import BaseModel


class ExchangeOrderResult(BaseModel):
    """
    Standardized result from an exchange order execution.
    All monetary values use Decimal for precision.
    """

    model_config = {"arbitrary_types_allowed": True}

    exchange: str  # e.g., "binance", "kraken"
    symbol: str  # e.g., "HIVEBTC"
    order_id: str  # Exchange's order ID (as string for compatibility)
    side: str  # "BUY" or "SELL"
    status: str  # e.g., "FILLED", "PARTIALLY_FILLED"
    requested_qty: Decimal  # Original quantity requested
    executed_qty: Decimal  # Actual quantity executed
    quote_qty: Decimal  # Total quote asset received/spent
    avg_price: Decimal  # Average execution price
    fee: Decimal  # Total fees paid
    fee_asset: str  # Asset used for fees (e.g., "BTC", "BNB")
    raw_response: dict  # Original exchange response for debugging

    @property
    def log_str(self) -> str:
        """Formatted string for logging the order result."""
        return (
            f"Exchange: {self.exchange}, Symbol: {self.symbol}, Order ID: {self.order_id}, "
            f"Side: {self.side}, Status: {self.status}, Requested Qty: {self.requested_qty}, "
            f"Executed Qty: {self.executed_qty}, Quote Qty: {self.quote_qty}, "
            f"Avg Price: {self.avg_price}, Fee: {self.fee} {self.fee_asset}"
        )

    @property
    def log_extra(self) -> Dict[str, Any]:
        """Dictionary of key order details for structured logging."""
        return {"exchange_order_result": self.model_dump()}

    @property
    def notification_str(self) -> str:
        """Formatted string for user notifications."""
        return (
            f"Executed {self.side} order on {self.exchange}: {self.executed_qty} units of "
            f"{self.symbol} at avg price {self.avg_price}, total {self.quote_qty} spent/received."
        )


class ExchangeMinimums(BaseModel):
    """Minimum order requirements from an exchange."""

    model_config = {"arbitrary_types_allowed": True}

    min_qty: Decimal  # Minimum quantity (LOT_SIZE)
    min_notional: Decimal  # Minimum order value in quote asset
    step_size: Decimal = Decimal("0")  # Quantity step size (optional)


class ExchangeError(Exception):
    """Base exception for exchange operations."""

    pass


class ExchangeConnectionError(ExchangeError):
    """Raised when connection to exchange fails."""

    pass


class ExchangeBelowMinimumError(ExchangeError):
    """Raised when order is below exchange minimums."""

    pass


class ExchangeInsufficientBalanceError(ExchangeError):
    """Raised when account has insufficient balance."""

    pass


@runtime_checkable
class ExchangeProtocol(Protocol):
    """
    Protocol defining the interface for exchange adapters.

    Any exchange implementation must provide these methods to be used
    with the rebalancing system.
    """

    @property
    def exchange_name(self) -> str:
        """Return the name of the exchange (e.g., 'binance')."""
        ...

    def get_min_order_requirements(self, base_asset: str, quote_asset: str) -> ExchangeMinimums:
        """
        Get minimum order requirements for a trading pair.

        Args:
            base_asset: The base asset (e.g., 'HIVE')
            quote_asset: The quote asset (e.g., 'BTC')

        Returns:
            ExchangeMinimums with min_qty and min_notional
        """
        ...

    def get_balance(self, asset: str) -> Decimal:
        """
        Get the available balance for an asset.

        Args:
            asset: The asset symbol (e.g., 'HIVE', 'BTC')

        Returns:
            Available balance as Decimal
        """
        ...

    def get_current_price(self, base_asset: str, quote_asset: str) -> Decimal:
        """
        Get the current market price for a trading pair.

        Args:
            base_asset: The base asset (e.g., 'HIVE')
            quote_asset: The quote asset (e.g., 'BTC')

        Returns:
            Current price as Decimal
        """
        ...

    def market_sell(
        self, base_asset: str, quote_asset: str, quantity: Decimal
    ) -> ExchangeOrderResult:
        """
        Execute a market sell order.

        Args:
            base_asset: The asset to sell (e.g., 'HIVE')
            quote_asset: The asset to receive (e.g., 'BTC')
            quantity: Amount of base asset to sell

        Returns:
            ExchangeOrderResult with execution details

        Raises:
            ExchangeBelowMinimumError: If order is below minimum
            ExchangeConnectionError: If connection fails
        """
        ...

    def market_buy(
        self, base_asset: str, quote_asset: str, quantity: Decimal
    ) -> ExchangeOrderResult:
        """
        Execute a market buy order.

        Args:
            base_asset: The asset to buy (e.g., 'HIVE')
            quote_asset: The asset to spend (e.g., 'BTC')
            quantity: Amount of base asset to buy

        Returns:
            ExchangeOrderResult with execution details

        Raises:
            ExchangeBelowMinimumError: If order is below minimum
            ExchangeConnectionError: If connection fails
        """
        ...


class BaseExchangeAdapter(ABC):
    """
    Abstract base class for exchange adapters.

    Provides common functionality and enforces the interface.
    Subclasses must implement the abstract methods.
    """

    def __init__(self, testnet: bool = False):
        self.testnet = testnet

    @property
    @abstractmethod
    def exchange_name(self) -> str:
        """Return the name of the exchange."""
        pass

    @abstractmethod
    def get_min_order_requirements(self, base_asset: str, quote_asset: str) -> ExchangeMinimums:
        """Get minimum order requirements for a trading pair."""
        pass

    @abstractmethod
    def get_balance(self, asset: str) -> Decimal:
        """Get the available balance for an asset."""
        pass

    @abstractmethod
    def get_current_price(self, base_asset: str, quote_asset: str) -> Decimal:
        """Get the current market price for a trading pair."""
        pass

    @abstractmethod
    def market_sell(
        self, base_asset: str, quote_asset: str, quantity: Decimal
    ) -> ExchangeOrderResult:
        """Execute a market sell order."""
        pass

    @abstractmethod
    def market_buy(
        self, base_asset: str, quote_asset: str, quantity: Decimal
    ) -> ExchangeOrderResult:
        """Execute a market buy order."""
        pass

    def build_symbol(self, base_asset: str, quote_asset: str) -> str:
        """
        Build a trading pair symbol from base and quote assets.
        Default implementation concatenates them (e.g., 'HIVEBTC').
        Override in subclass if exchange uses different format.
        """
        return f"{base_asset}{quote_asset}"

    def can_execute_order(
        self, base_asset: str, quote_asset: str, quantity: Decimal
    ) -> tuple[bool, str]:
        """
        Check if an order can be executed (meets minimums).

        Returns:
            tuple: (can_execute: bool, reason: str)
        """
        try:
            minimums = self.get_min_order_requirements(base_asset, quote_asset)
            price = self.get_current_price(base_asset, quote_asset)
            notional = quantity * price

            if quantity < minimums.min_qty:
                return False, f"Quantity {quantity} below minimum {minimums.min_qty}"

            if notional < minimums.min_notional:
                return (
                    False,
                    f"Notional {notional} below minimum {minimums.min_notional}",
                )

            return True, "OK"
        except Exception as e:
            return False, f"Error checking order: {e}"
