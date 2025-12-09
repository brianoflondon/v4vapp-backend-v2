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

# Conversion constant: 1 BTC = 100,000,000 satoshis
SATS_PER_BTC = Decimal("100000000")


def format_base_asset(value: Decimal, asset: str) -> str:
    """
    Format a base asset value for display.

    For HIVE-like assets: 3 decimal places
    For other assets: 8 decimal places

    Args:
        value: The decimal value to format
        asset: The asset symbol (e.g., 'HIVE', 'BTC')

    Returns:
        Formatted string like "100.123 HIVE"
    """
    if asset.upper() in ("HIVE", "HBD"):
        return f"{value:.3f} {asset}"
    return f"{value:.8f} {asset}"


def format_quote_asset(value: Decimal, asset: str) -> str:
    """
    Format a quote asset value for display.

    For BTC: convert to sats and display as integer with comma separators
    For other assets: 8 decimal places

    Args:
        value: The decimal value to format (in BTC or other quote asset)
        asset: The asset symbol (e.g., 'BTC', 'USDT')

    Returns:
        Formatted string like "12,345 sats" or "100.00000000 USDT"
    """
    if asset.upper() == "BTC":
        sats = int(value * SATS_PER_BTC)
        return f"{sats:,} sats"
    return f"{value:.8f} {asset}"


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

    # Optional: base and quote assets for formatting (can be inferred from symbol)
    base_asset: str = ""  # e.g., "HIVE" - set by adapter if known
    quote_asset: str = ""  # e.g., "BTC" - set by adapter if known

    def _get_assets(self) -> tuple[str, str]:
        """Get base and quote assets, inferring from symbol if not set."""
        if self.base_asset and self.quote_asset:
            return self.base_asset, self.quote_asset
        # Try to infer from common quote assets
        for quote in ("BTC", "USDT", "BUSD", "ETH", "BNB"):
            if self.symbol.endswith(quote):
                base = self.symbol[: -len(quote)]
                return base, quote
        # Fallback: assume last 3 chars are quote
        return self.symbol[:-3], self.symbol[-3:]

    @property
    def log_str(self) -> str:
        """Formatted string for logging the order result."""
        base, quote = self._get_assets()
        qty_str = format_base_asset(self.executed_qty, base)
        quote_str = format_quote_asset(self.quote_qty, quote)
        fee_str = format_quote_asset(self.fee, self.fee_asset)
        return (
            f"Exchange: {self.exchange}, Symbol: {self.symbol}, "
            f"Order ID: {self.order_id}, Side: {self.side}, Status: {self.status}, "
            f"Executed: {qty_str}, Quote: {quote_str}, Fee: {fee_str}"
        )

    @property
    def log_extra(self) -> Dict[str, Any]:
        """Dictionary of key order details for structured logging."""
        return {"exchange_order_result": self.model_dump()}

    @property
    def notification_str(self) -> str:
        """Formatted string for user notifications."""
        base, quote = self._get_assets()
        qty_str = format_base_asset(self.executed_qty, base)
        quote_str = format_quote_asset(self.quote_qty, quote)
        return f"Executed {self.side} on {self.exchange}: {qty_str} for {quote_str}"


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
        self,
        base_asset: str,
        quote_asset: str,
        quantity: Decimal,
        client_order_id: str | None = None,
    ) -> ExchangeOrderResult:
        """
        Execute a market sell order.

        Args:
            base_asset: The asset to sell (e.g., 'HIVE')
            quote_asset: The asset to receive (e.g., 'BTC')
            quantity: Amount of base asset to sell
            client_order_id: Optional custom order ID for tracking

        Returns:
            ExchangeOrderResult with execution details

        Raises:
            ExchangeBelowMinimumError: If order is below minimum
            ExchangeConnectionError: If connection fails
        """
        ...

    def market_buy(
        self,
        base_asset: str,
        quote_asset: str,
        quantity: Decimal,
        client_order_id: str | None = None,
    ) -> ExchangeOrderResult:
        """
        Execute a market buy order.

        Args:
            base_asset: The asset to buy (e.g., 'HIVE')
            quote_asset: The asset to spend (e.g., 'BTC')
            quantity: Amount of base asset to buy
            client_order_id: Optional custom order ID for tracking

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
        self,
        base_asset: str,
        quote_asset: str,
        quantity: Decimal,
        client_order_id: str | None = None,
    ) -> ExchangeOrderResult:
        """Execute a market sell order."""
        pass

    @abstractmethod
    def market_buy(
        self,
        base_asset: str,
        quote_asset: str,
        quantity: Decimal,
        client_order_id: str | None = None,
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


def get_exchange_adapter(exchange_name: str | None = None) -> BaseExchangeAdapter:
    """
    Factory function to get the appropriate exchange adapter based on configuration.

    This function reads from the application config to determine which exchange
    to use and whether to use testnet or mainnet.

    Args:
        exchange_name: Optional exchange name override. If not provided,
                      uses default_exchange from config.

    Returns:
        BaseExchangeAdapter: The configured exchange adapter instance.

    Raises:
        ValueError: If the specified exchange is not supported.
    """
    # Import here to avoid circular imports
    from v4vapp_backend_v2.config.setup import InternalConfig

    config = InternalConfig()
    exchange_config = config.config.exchange_config

    # Use provided name or default from config
    provider_name = exchange_name or exchange_config.default_exchange

    # Get the provider config
    provider = exchange_config.get_provider(provider_name)
    testnet = provider.use_testnet

    # Return the appropriate adapter
    if provider_name == "binance":
        from v4vapp_backend_v2.conversion.binance_adapter import BinanceAdapter

        return BinanceAdapter(testnet=testnet)

    # Future exchanges can be added here:
    # elif provider_name == "vsc-exchange":
    #     from v4vapp_backend_v2.conversion.vsc_adapter import VSCAdapter
    #     return VSCAdapter(testnet=testnet)

    raise ValueError(f"Unsupported exchange: {provider_name}")
