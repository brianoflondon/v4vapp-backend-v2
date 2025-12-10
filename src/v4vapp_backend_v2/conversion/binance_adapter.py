"""
Binance exchange adapter implementing the ExchangeProtocol.

This adapter wraps the binance_extras functions to provide a standardized
interface for the rebalancing system.
"""

from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import ClassVar

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.conversion.exchange_protocol import (
    SATS_PER_BTC,
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
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.currency_class import Currency


class BinanceAdapter(BaseExchangeAdapter):
    """
    Binance exchange adapter.

    Implements the ExchangeProtocol using the binance_extras module.

    Asset-specific decimal precision is configured via ASSET_DECIMALS.
    Assets not in the dict default to 8 decimal places.
    """

    # Asset-specific decimal precision for lot sizes
    # Binance requires HIVE to be traded in whole numbers (0 decimals)
    ASSET_DECIMALS: ClassVar[dict[str, int]] = {
        "HIVE": 0,  # HIVE must be traded in whole numbers
        "BTC": 8,  # BTC has high precision
        "BNB": 2,  # BNB typically 2 decimals
        "USDT": 2,  # USDT typically 2 decimals
    }

    DEFAULT_DECIMALS: ClassVar[int] = 8  # Default precision for unknown assets

    @property
    def exchange_name(self) -> str:
        if self.testnet:
            return "binance_testnet"
        return "binance"

    def get_asset_decimals(self, asset: str) -> int:
        """
        Get the number of decimal places for an asset.

        Args:
            asset: The asset symbol (e.g., 'HIVE', 'BTC')

        Returns:
            Number of decimal places allowed for the asset
        """
        return self.ASSET_DECIMALS.get(asset.upper(), self.DEFAULT_DECIMALS)

    def round_quantity(self, asset: str, quantity: Decimal) -> Decimal:
        """
        Round a quantity to the appropriate decimal places for an asset.

        Uses ROUND_DOWN to ensure we don't exceed the original quantity.

        Args:
            asset: The asset symbol (e.g., 'HIVE', 'BTC')
            quantity: The quantity to round

        Returns:
            Rounded quantity as Decimal
        """
        decimals = self.get_asset_decimals(asset)
        if decimals == 0:
            # Round down to whole number
            return quantity.quantize(Decimal("1"), rounding=ROUND_DOWN)
        else:
            # Round down to specified decimal places
            quantizer = Decimal(10) ** -decimals
            return quantity.quantize(quantizer, rounding=ROUND_DOWN)

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

    def _convert_fee_to_msats(self, fee: Decimal, fee_asset: str) -> CryptoConv | None:
        """
        Convert exchange fee to a CryptoConv object with msats as the base.

        For BNB fees, looks up the current BNBBTC price to convert:
        BNB -> BTC -> sats -> msats

        Args:
            fee: The fee amount in fee_asset units
            fee_asset: The asset the fee is denominated in (e.g., 'BNB', 'BTC')

        Returns:
            CryptoConv with fee converted to msats, or None if conversion fails
        """
        if fee <= Decimal("0"):
            return None

        try:
            fee_btc = Decimal("0")

            if fee_asset == "BTC":
                # Fee is already in BTC
                fee_btc = fee
            elif fee_asset == "BNB":
                # Get BNB/BTC price and convert
                bnb_price_info = get_current_price("BNBBTC", testnet=self.testnet)
                bnb_btc_price = Decimal(bnb_price_info["bid_price"])
                fee_btc = fee * bnb_btc_price
            else:
                # For other assets, try to get price against BTC
                try:
                    symbol = f"{fee_asset}BTC"
                    price_info = get_current_price(symbol, testnet=self.testnet)
                    asset_btc_price = Decimal(price_info["bid_price"])
                    fee_btc = fee * asset_btc_price
                except Exception:
                    logger.warning(
                        f"Could not convert fee asset {fee_asset} to BTC, fee_conv will be None"
                    )
                    return None

            # Convert BTC to sats (1 BTC = 100,000,000 sats)
            fee_sats = fee_btc * SATS_PER_BTC
            # Convert to msats (1 sat = 1000 msats)
            fee_msats = fee_sats * Decimal("1000")

            # Create CryptoConv with msats as the starting point
            # Note: We're creating a minimal conv since we don't have full quote data
            # The key values are msats, sats, and btc which we can calculate directly
            return CryptoConv(
                msats=fee_msats,
                sats=fee_sats,
                btc=fee_btc,
                conv_from=Currency.MSATS,
                value=fee_msats,
                source="Binance",
                fetch_date=datetime.now(tz=timezone.utc),
            )

        except BinanceErrorBadConnection as e:
            logger.warning(f"Failed to get price for fee conversion: {e}")
            return None
        except Exception as e:
            logger.warning(f"Unexpected error converting fee: {e}")
            return None

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

        # Convert the fee to a CryptoConv object (msats-based)
        fee_conv = self._convert_fee_to_msats(total_fee, fee_asset)

        return ExchangeOrderResult(
            exchange=self.exchange_name,
            symbol=result.symbol,
            order_id=str(result.order_id),
            client_order_id=result.client_order_id,
            side=side,
            status=result.status,
            requested_qty=requested_qty,
            executed_qty=result.executed_qty,
            quote_qty=result.cummulative_quote_qty,
            avg_price=result.avg_price,
            fee=total_fee,
            fee_asset=fee_asset,
            fee_conv=fee_conv,
            raw_response=result.raw_response,
        )

    def market_sell(
        self,
        base_asset: str,
        quote_asset: str,
        quantity: Decimal,
        client_order_id: str | None = None,
    ) -> ExchangeOrderResult:
        """
        Execute a market sell order on Binance.

        The quantity is automatically rounded to the appropriate decimal places
        for the base asset (e.g., HIVE is rounded to whole numbers).

        Args:
            base_asset: The asset to sell (e.g., 'HIVE')
            quote_asset: The asset to receive (e.g., 'BTC')
            quantity: Amount of base asset to sell
            client_order_id: Optional custom order ID for tracking (max 36 chars)

        Returns:
            ExchangeOrderResult with execution details
        """
        symbol = self.build_symbol(base_asset, quote_asset)
        # Round quantity to asset-specific decimal places
        rounded_qty = self.round_quantity(base_asset, quantity)

        if rounded_qty <= Decimal("0"):
            raise ExchangeBelowMinimumError(
                f"Quantity {quantity} rounds to {rounded_qty} for {base_asset} "
                f"(requires {self.get_asset_decimals(base_asset)} decimals)"
            )

        try:
            result = market_sell(
                symbol=symbol,
                quantity=rounded_qty,
                testnet=self.testnet,
                client_order_id=client_order_id,
            )
            return self._convert_result(result, "SELL", rounded_qty)
        except BinanceErrorBelowMinimum as e:
            raise ExchangeBelowMinimumError(f"Binance order below minimum: {e}")
        except BinanceErrorBadConnection as e:
            raise ExchangeConnectionError(f"Binance connection error: {e}")

    def market_buy(
        self,
        base_asset: str,
        quote_asset: str,
        quantity: Decimal,
        client_order_id: str | None = None,
    ) -> ExchangeOrderResult:
        """
        Execute a market buy order on Binance.

        The quantity is automatically rounded to the appropriate decimal places
        for the base asset (e.g., HIVE is rounded to whole numbers).

        Args:
            base_asset: The asset to buy (e.g., 'HIVE')
            quote_asset: The asset to spend (e.g., 'BTC')
            quantity: Amount of base asset to buy
            client_order_id: Optional custom order ID for tracking (max 36 chars)

        Returns:
            ExchangeOrderResult with execution details
        """
        symbol = self.build_symbol(base_asset, quote_asset)
        # Round quantity to asset-specific decimal places
        rounded_qty = self.round_quantity(base_asset, quantity)

        if rounded_qty <= Decimal("0"):
            raise ExchangeBelowMinimumError(
                f"Quantity {quantity} rounds to {rounded_qty} for {base_asset} "
                f"(requires {self.get_asset_decimals(base_asset)} decimals)"
            )

        try:
            result = market_buy(
                symbol=symbol,
                quantity=rounded_qty,
                testnet=self.testnet,
                client_order_id=client_order_id,
            )
            return self._convert_result(result, "BUY", rounded_qty)
        except BinanceErrorBelowMinimum as e:
            raise ExchangeBelowMinimumError(f"Binance order below minimum: {e}")
        except BinanceErrorBadConnection as e:
            raise ExchangeConnectionError(f"Binance connection error: {e}")
