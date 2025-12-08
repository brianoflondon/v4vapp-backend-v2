"""
Binance exchange adapter implementing the ExchangeProtocol.

This adapter wraps the binance_extras functions to provide a standardized
interface for the rebalancing system.
"""

from decimal import Decimal

from v4vapp_backend_v2.conversion.exchange_protocol import (
    BaseExchangeAdapter,
    ExchangeBelowMinimumError,
    ExchangeConnectionError,
    ExchangeMinimums,
    ExchangeOrderResult,
)
from v4vapp_backend_v2.helpers.binance_extras import (
    BinanceErrorBadConnection,
    BinanceErrorBelowMinimum,
    MarketOrderResult,
    get_balances,
    get_current_price,
    get_min_order_quantity,
    get_symbol_info,
    market_buy,
    market_sell,
)


class BinanceAdapter(BaseExchangeAdapter):
    """
    Binance exchange adapter.

    Implements the ExchangeProtocol using the binance_extras module.
    """

    @property
    def exchange_name(self) -> str:
        return "binance"

    def get_min_order_requirements(self, base_asset: str, quote_asset: str) -> ExchangeMinimums:
        """
        Get minimum order requirements from Binance.

        Args:
            base_asset: The base asset (e.g., 'HIVE')
            quote_asset: The quote asset (e.g., 'BTC')

        Returns:
            ExchangeMinimums with min_qty and min_notional
        """
        symbol = self.build_symbol(base_asset, quote_asset)
        try:
            min_qty, min_notional = get_min_order_quantity(symbol, testnet=self.testnet)

            # Try to get step_size from symbol info
            step_size = Decimal("0")
            symbol_info = get_symbol_info(symbol, testnet=self.testnet)
            for f in symbol_info.get("filters", []):
                if f["filterType"] == "LOT_SIZE":
                    step_size = Decimal(f.get("stepSize", "0"))
                    break

            return ExchangeMinimums(
                min_qty=min_qty,
                min_notional=min_notional,
                step_size=step_size,
            )
        except BinanceErrorBadConnection as e:
            raise ExchangeConnectionError(f"Failed to get Binance minimums: {e}")

    def get_balance(self, asset: str) -> Decimal:
        """
        Get available balance from Binance.

        Args:
            asset: The asset symbol (e.g., 'HIVE', 'BTC')

        Returns:
            Available balance as Decimal
        """
        try:
            balances = get_balances([asset], testnet=self.testnet)
            return Decimal(str(balances.get(asset, 0)))
        except BinanceErrorBadConnection as e:
            raise ExchangeConnectionError(f"Failed to get Binance balance: {e}")

    def get_current_price(self, base_asset: str, quote_asset: str) -> Decimal:
        """
        Get current market price from Binance.

        Args:
            base_asset: The base asset (e.g., 'HIVE')
            quote_asset: The quote asset (e.g., 'BTC')

        Returns:
            Current price as Decimal (using bid price for conservative estimate)
        """
        symbol = self.build_symbol(base_asset, quote_asset)
        try:
            price_info = get_current_price(symbol, testnet=self.testnet)
            return Decimal(price_info["bid_price"])
        except BinanceErrorBadConnection as e:
            raise ExchangeConnectionError(f"Failed to get Binance price: {e}")

    def _convert_result(
        self, result: MarketOrderResult, side: str, requested_qty: Decimal
    ) -> ExchangeOrderResult:
        """Convert Binance result to standardized ExchangeOrderResult."""
        # Extract fee info from fills
        total_fee = Decimal("0")
        fee_asset = ""
        for fill in result.fills:
            total_fee += Decimal(str(fill.get("commission", "0")))
            if not fee_asset:
                fee_asset = fill.get("commissionAsset", "")

        return ExchangeOrderResult(
            exchange=self.exchange_name,
            symbol=result.symbol,
            order_id=str(result.order_id),
            side=side,
            status=result.status,
            requested_qty=requested_qty,
            executed_qty=result.executed_qty,
            quote_qty=result.cummulative_quote_qty,
            avg_price=result.avg_price,
            fee=total_fee,
            fee_asset=fee_asset,
            raw_response=result.raw_response,
        )

    def market_sell(
        self, base_asset: str, quote_asset: str, quantity: Decimal
    ) -> ExchangeOrderResult:
        """
        Execute a market sell order on Binance.

        Args:
            base_asset: The asset to sell (e.g., 'HIVE')
            quote_asset: The asset to receive (e.g., 'BTC')
            quantity: Amount of base asset to sell

        Returns:
            ExchangeOrderResult with execution details
        """
        symbol = self.build_symbol(base_asset, quote_asset)
        try:
            result = market_sell(symbol=symbol, quantity=quantity, testnet=self.testnet)
            return self._convert_result(result, "SELL", quantity)
        except BinanceErrorBelowMinimum as e:
            raise ExchangeBelowMinimumError(f"Binance order below minimum: {e}")
        except BinanceErrorBadConnection as e:
            raise ExchangeConnectionError(f"Binance connection error: {e}")

    def market_buy(
        self, base_asset: str, quote_asset: str, quantity: Decimal
    ) -> ExchangeOrderResult:
        """
        Execute a market buy order on Binance.

        Args:
            base_asset: The asset to buy (e.g., 'HIVE')
            quote_asset: The asset to spend (e.g., 'BTC')
            quantity: Amount of base asset to buy

        Returns:
            ExchangeOrderResult with execution details
        """
        symbol = self.build_symbol(base_asset, quote_asset)
        try:
            result = market_buy(symbol=symbol, quantity=quantity, testnet=self.testnet)
            return self._convert_result(result, "BUY", quantity)
        except BinanceErrorBelowMinimum as e:
            raise ExchangeBelowMinimumError(f"Binance order below minimum: {e}")
        except BinanceErrorBadConnection as e:
            raise ExchangeConnectionError(f"Binance connection error: {e}")
