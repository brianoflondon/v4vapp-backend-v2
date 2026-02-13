"""
Binance exchange adapter implementing the ExchangeProtocol.

This adapter wraps the binance_extras functions to provide a standardized
interface for the rebalancing system.

2026-02-13 - Binance removed  BTC/HIVE pair so this adapter is depreciated in favor of
BinanceSwapAdapter which uses the new swap endpoints and supports the new swap pairs.
The old BinanceAdapter code is left here for reference and potential future use if needed.

BinanceSwapAdapter cannot be used on Testnet however.

"""

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
    get_mid_price,
    get_min_order_quantity,
    get_symbol_info,
    market_buy,
    market_sell,
)
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse


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

    def get_balances(self, assets: list[str]) -> dict[str, Decimal]:
        """
        Get available balances for multiple assets from Binance in a single API call.

        Args:
            assets: List of asset symbols (e.g., ['BTC', 'HIVE', 'USDT'])

        Returns:
            Dict mapping asset symbols to balances as Decimal.
            Includes a 'SATS' key if 'BTC' is requested.
        """
        try:
            raw = get_balances(assets, testnet=self.testnet)
            result = {k: Decimal(str(v)) for k, v in raw.items()}
            return result
        except BinanceErrorBadConnection as e:
            raise ExchangeConnectionError(f"Failed to get Binance balances: {e}")

    def get_current_price(self, base_asset: str, quote_asset: str) -> Decimal:
        """
        Get current market price from Binance.

        Returns the mid-price (average of bid and ask) when both are available,
        otherwise falls back to the best non-zero price.

        Args:
            base_asset: The base asset (e.g., 'HIVE')
            quote_asset: The quote asset (e.g., 'BTC')

        Returns:
            Current mid-price as Decimal
        """
        symbol = self.build_symbol(base_asset, quote_asset)
        try:
            return get_mid_price(symbol, testnet=self.testnet)
        except BinanceErrorBadConnection as e:
            raise ExchangeConnectionError(f"Failed to get Binance price: {e}")

    def _convert_fee_to_msats(
        self,
        fee: Decimal,
        fee_asset: str,
        requested_qty: Decimal = Decimal(0),
        base_asset: str = "",
    ) -> Decimal:
        """
        Convert exchange fee to a Decimal representing msats.

        For BNB fees, looks up the current BNBBTC price to convert:
        BNB -> BTC -> sats -> msats

        Args:
            fee: The fee amount in fee_asset units
            fee_asset: The asset the fee is denominated in (e.g., 'BNB', 'BTC')

        Returns:
            Decimal representing fee in msats, or Decimal(0) if conversion fails
        """
        if fee <= Decimal("0"):
            if self.testnet:
                # if the fee is zero (this happens always on testnet) return 0.0075% of the transaction value
                fee = requested_qty * Decimal("0.000075")
                fee_asset = base_asset
            else:
                return Decimal(0)

        try:
            fee_btc = Decimal("0")

            if fee_asset == "BTC":
                # Fee is already in BTC
                fee_btc = fee
            elif fee_asset == "BNB":
                # Get BNB/BTC price and convert
                bnb_btc_price = get_mid_price("BNBBTC", testnet=self.testnet)
                fee_btc = fee * bnb_btc_price
            else:
                # For other assets, try to get price against BTC
                try:
                    symbol = f"{fee_asset}BTC"
                    asset_btc_price = get_mid_price(symbol, testnet=self.testnet)
                    fee_btc = fee * asset_btc_price
                except Exception:
                    logger.warning(
                        f"Could not convert fee asset {fee_asset} to BTC, fee_conv will be None"
                    )
                    return Decimal(0)

            # Convert BTC to sats (1 BTC = 100,000,000 sats)
            fee_sats = fee_btc * SATS_PER_BTC
            # Convert to msats (1 sat = 1000 msats)
            fee_msats = fee_sats * Decimal("1000")

            return fee_msats

        except BinanceErrorBadConnection as e:
            logger.warning(f"Failed to get price for fee conversion: {e}")
            return Decimal(0)
        except Exception as e:
            logger.exception(f"Unexpected error converting fee: {e}")
            return Decimal(0)

    def _build_trade_quote(
        self,
        base_asset: str,
        quote_asset: str,
        avg_price: Decimal,
        raw_response: dict,
    ) -> QuoteResponse:
        """
        Build a QuoteResponse that reflects the actual executed trade rate,
        while preserving accurate market prices for HBD, USD, and BTC conversions.

        This fetches the current market quote and overrides only the specific
        rate that was executed in the trade (e.g., HIVE/BTC price), ensuring
        that all other conversion calculations remain accurate.

        For HIVE/BTC trades:
            - Uses actual trade avg_price for sats_hive calculation
            - Preserves real BTC/USD, HBD/USD rates from market

        Args:
            base_asset: The base asset (e.g., 'HIVE')
            quote_asset: The quote asset (e.g., 'BTC')
            avg_price: The average execution price (base asset in terms of quote asset)
            raw_response: The raw exchange response for reference

        Returns:
            QuoteResponse with sats_hive reflecting the actual trade rate,
            and other rates from current market data
        """
        from datetime import datetime, timezone

        from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes

        # Fetch current market quote for accurate HBD/USD/BTC rates
        all_quotes = AllQuotes()
        # Use synchronous call to get Binance quote - we're already in sync context
        import asyncio

        try:
            # Try to get a running event loop (Python 3.10+)
            try:
                loop = asyncio.get_running_loop()
                # We're in an async context, use thread pool to avoid blocking
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, all_quotes.get_all_quotes())
                    future.result(timeout=5)
            except RuntimeError:
                # No running event loop, we can use asyncio.run directly
                asyncio.run(all_quotes.get_all_quotes())
        except Exception as e:
            logger.warning(f"Failed to fetch market quote for trade_quote: {e}")
            # Fall back to basic quote
            all_quotes.quote = QuoteResponse()

        market_quote = all_quotes.quote

        # Calculate the actual sats_hive from the trade
        # avg_price is HIVE in BTC, so sats_hive = avg_price * SATS_PER_BTC
        trade_sats_hive = avg_price * SATS_PER_BTC

        # For HIVE/BTC trades: override hive_usd to produce correct sats_hive
        # sats_hive = (SATS_PER_BTC / btc_usd) * hive_usd
        # We want: trade_sats_hive = (SATS_PER_BTC / btc_usd) * hive_usd
        # So: hive_usd = trade_sats_hive * btc_usd / SATS_PER_BTC = avg_price * btc_usd
        if base_asset.upper() == "HIVE" and quote_asset.upper() == "BTC":
            # Calculate hive_usd that will produce the correct sats_hive
            # given the market's btc_usd rate
            btc_usd = market_quote.btc_usd if market_quote.btc_usd > 0 else Decimal("1")
            trade_hive_usd = avg_price * btc_usd

            return QuoteResponse(
                hive_usd=trade_hive_usd,
                hbd_usd=market_quote.hbd_usd,
                btc_usd=btc_usd,
                hive_hbd=market_quote.hive_hbd,
                source=f"{self.exchange_name}_trade",
                fetch_date=datetime.now(tz=timezone.utc),
            )
        elif base_asset.upper() == "HBD" and quote_asset.upper() == "BTC":
            btc_usd = market_quote.btc_usd if market_quote.btc_usd > 0 else Decimal("1")
            trade_hbd_usd = avg_price * btc_usd

            return QuoteResponse(
                hive_usd=market_quote.hive_usd,
                hbd_usd=trade_hbd_usd,
                btc_usd=btc_usd,
                hive_hbd=market_quote.hive_hbd,
                source=f"{self.exchange_name}_trade",
                fetch_date=datetime.now(tz=timezone.utc),
            )
        else:
            # For other pairs, return market quote with trade source
            return QuoteResponse(
                hive_usd=market_quote.hive_usd,
                hbd_usd=market_quote.hbd_usd,
                btc_usd=market_quote.btc_usd,
                hive_hbd=market_quote.hive_hbd,
                source=f"{self.exchange_name}_trade",
                fetch_date=datetime.now(tz=timezone.utc),
            )

    def _convert_result(
        self,
        result: MarketOrderResult,
        side: str,
        requested_qty: Decimal,
        base_asset: str,
        quote_asset: str = "BTC",
    ) -> ExchangeOrderResult:
        """
        Convert Binance result to standardized ExchangeOrderResult.

        This method processes the raw market order result from Binance, extracts fee information
        from the fills, converts the fee to msats, and constructs a
        standardized ExchangeOrderResult object.

        Args:
            result (MarketOrderResult): The market order result returned by Binance, containing
                details like order ID, status, executed quantity, average price, and fills.
            side (str): The side of the order, typically 'buy' or 'sell'.
            requested_qty (Decimal): The originally requested quantity for the order.
            base_asset (str): The base asset of the trade (e.g., 'HIVE').
            quote_asset (str): The quote asset of the trade (e.g., 'BTC').

        Returns:
            ExchangeOrderResult: A standardized object representing the exchange order result,
                including exchange name, symbol, order details, fees, and raw response.
        """
        # Extract fee info from fills
        total_fee = Decimal("0")
        fee_asset = ""
        for fill in result.fills:
            total_fee += Decimal(str(fill.get("commission", "0")))
            if not fee_asset:
                fee_asset = fill.get("commissionAsset", "")

        # Build trade quote from the executed price
        trade_quote = self._build_trade_quote(
            base_asset=base_asset,
            quote_asset=quote_asset,
            avg_price=result.avg_price,
            raw_response=result.raw_response,
        )

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
            fee_msats=self._convert_fee_to_msats(total_fee, fee_asset, requested_qty, base_asset),
            fee_original=total_fee,
            fee_asset=fee_asset,
            raw_response=result.raw_response,
            base_asset=base_asset,
            quote_asset=quote_asset,
            trade_quote=trade_quote,
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
            return self._convert_result(result, "SELL", rounded_qty, base_asset, quote_asset)
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
            return self._convert_result(result, "BUY", rounded_qty, base_asset, quote_asset)
        except BinanceErrorBelowMinimum as e:
            raise ExchangeBelowMinimumError(f"Binance order below minimum: {e}")
        except BinanceErrorBadConnection as e:
            raise ExchangeConnectionError(f"Binance connection error: {e}")
