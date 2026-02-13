"""
Binance Convert (Swap) adapter implementing the ExchangeProtocol.

This adapter uses Binance's Convert API (send_quote_request / accept_quote)
instead of market orders. The Convert API provides:
  - Simpler flow: request a quote, then accept it
  - Potentially lower minimums than spot market orders
  - Fees hidden in the conversion rate (treated as zero explicit fees)

NOTE: The Convert API does NOT support testnet. All operations are live.
"""

import time
from decimal import ROUND_DOWN, Decimal
from typing import ClassVar

import backoff
from binance.error import ClientError  # type: ignore
from binance.spot import Spot as Client  # type: ignore
from pydantic import BaseModel, ConfigDict
from requests.exceptions import RequestException

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.conversion.exchange_protocol import (
    BaseExchangeAdapter,
    ExchangeBelowMinimumError,
    ExchangeConnectionError,
    ExchangeError,
    ExchangeMinimums,
    ExchangeOrderResult,
)
from v4vapp_backend_v2.helpers.binance_extras import (
    BinanceErrorBadConnection,
    get_balances,
    get_client,
    get_mid_price,
)
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse


class ExchangeQuoteExpiredError(ExchangeError):
    """Raised when a Convert quote has expired before acceptance."""

    pass


class ConvertQuoteResult(BaseModel):
    """
    Model for a Binance Convert quote response.

    Example response from send_quote_request:
    {
        "quoteId": "12415572564",
        "ratio": "38163.7",
        "inverseRatio": "0.0000262",
        "validTimestamp": 1623319461670,
        "toAmount": "3816.37",
        "fromAmount": "0.1"
    }
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    quote_id: str
    ratio: Decimal  # conversion ratio (toAsset/fromAsset rate)
    inverse_ratio: Decimal
    valid_timestamp: int  # ms timestamp when quote expires
    from_amount: Decimal
    to_amount: Decimal
    raw_response: dict

    @classmethod
    def from_binance_response(cls, response: dict) -> "ConvertQuoteResult":
        return cls(
            quote_id=str(response.get("quoteId", "")),
            ratio=Decimal(str(response.get("ratio", "0"))),
            inverse_ratio=Decimal(str(response.get("inverseRatio", "0"))),
            valid_timestamp=int(response.get("validTimestamp", 0)),
            from_amount=Decimal(str(response.get("fromAmount", "0"))),
            to_amount=Decimal(str(response.get("toAmount", "0"))),
            raw_response=response,
        )

    @property
    def is_expired(self) -> bool:
        """Check if the quote has expired (using current time in ms)."""
        return int(time.time() * 1000) > self.valid_timestamp

    @property
    def expires_in_seconds(self) -> float:
        """Seconds until the quote expires (negative if already expired)."""
        return (self.valid_timestamp - int(time.time() * 1000)) / 1000.0


class ConvertAcceptResult(BaseModel):
    """
    Model for a Binance Convert accept_quote response.

    Example response from accept_quote:
    {
        "orderId": "933256278426274426",
        "createTime": 1623381330472,
        "orderStatus": "PROCESS"  // PROCESS/ACCEPT_SUCCESS/SUCCESS/FAIL
    }
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    order_id: str
    create_time: int
    order_status: str
    raw_response: dict

    @classmethod
    def from_binance_response(cls, response: dict) -> "ConvertAcceptResult":
        return cls(
            order_id=str(response.get("orderId", "")),
            create_time=int(response.get("createTime", 0)),
            order_status=response.get("orderStatus", ""),
            raw_response=response,
        )


class ConvertOrderStatus(BaseModel):
    """
    Model for a Binance Convert order_status response.

    Example response from order_status:
    {
        "orderId": 933256278426274426,
        "orderStatus": "SUCCESS",
        "fromAsset": "BTC",
        "fromAmount": "0.00054414",
        "toAsset": "USDT",
        "toAmount": "20",
        "ratio": "36755",
        "inverseRatio": "0.00002721",
        "createTime": 1623381330472
    }
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    order_id: str
    order_status: str  # PROCESS / ACCEPT_SUCCESS / SUCCESS / FAIL
    from_asset: str
    from_amount: Decimal
    to_asset: str
    to_amount: Decimal
    ratio: Decimal
    inverse_ratio: Decimal
    create_time: int
    raw_response: dict

    @classmethod
    def from_binance_response(cls, response: dict) -> "ConvertOrderStatus":
        return cls(
            order_id=str(response.get("orderId", "")),
            order_status=response.get("orderStatus", ""),
            from_asset=response.get("fromAsset", ""),
            from_amount=Decimal(str(response.get("fromAmount", "0"))),
            to_asset=response.get("toAsset", ""),
            to_amount=Decimal(str(response.get("toAmount", "0"))),
            ratio=Decimal(str(response.get("ratio", "0"))),
            inverse_ratio=Decimal(str(response.get("inverseRatio", "0"))),
            create_time=int(response.get("createTime", 0)),
            raw_response=response,
        )


class BinanceSwapAdapter(BaseExchangeAdapter):
    """
    Binance Convert (swap) adapter.

    Uses the Binance Convert API (send_quote_request / accept_quote) for
    simpler, potentially lower-minimum swaps. Fees are embedded in the
    conversion rate and treated as zero explicit fees.

    NOTE: The Convert API does NOT support testnet. The testnet parameter
    is accepted for interface compatibility but will log a warning if True.
    """

    # How long to wait for order to reach SUCCESS status (seconds)
    ORDER_STATUS_TIMEOUT: ClassVar[int] = 30
    ORDER_STATUS_POLL_INTERVAL: ClassVar[float] = 0.5

    # Valid time options for quotes
    VALID_TIME_OPTIONS: ClassVar[list[str]] = ["10s", "30s", "1m", "2m"]

    def __init__(self, testnet: bool = False):
        # Convert API does not support testnet - always use mainnet
        if testnet:
            logger.warning(
                "Binance Convert API does not support testnet. Using mainnet credentials instead."
            )
        super().__init__(testnet=False)

    @property
    def exchange_name(self) -> str:
        return "binance_convert"

    # Maximum decimal places accepted by the Binance Convert API
    MAX_CONVERT_DECIMALS: ClassVar[int] = 8

    def _get_client(self) -> Client:
        """
        Get a Binance Spot client for Convert API calls.

        Always uses mainnet since Convert API doesn't support testnet.
        """
        return get_client(testnet=False)

    @classmethod
    def _format_amount(cls, amount: Decimal) -> str:
        """
        Format a Decimal amount for the Convert API.

        Binance Convert API rejects amounts with more than 8 decimal places.
        This method truncates (rounds down) to 8 decimals and strips trailing
        zeros to produce a clean string.

        Args:
            amount: The Decimal amount to format

        Returns:
            String representation with at most 8 decimal places
        """
        quantizer = Decimal(10) ** -cls.MAX_CONVERT_DECIMALS
        truncated = amount.quantize(quantizer, rounding=ROUND_DOWN)
        # Use fixed-point notation and strip trailing zeros
        fixed = f"{truncated:f}"
        if "." in fixed:
            fixed = fixed.rstrip("0").rstrip(".")
        return fixed

    def get_min_order_requirements(self, base_asset: str, quote_asset: str) -> ExchangeMinimums:
        """
        Get minimum order requirements from the Convert API.

        Uses list_all_convert_pairs to get min/max amounts for a pair.

        Args:
            base_asset: The base asset (e.g., 'HIVE')
            quote_asset: The quote asset (e.g., 'BTC')

        Returns:
            ExchangeMinimums with min_qty and min_notional from the Convert API
        """
        try:
            client = self._get_client()
            pairs = client.list_all_convert_pairs(fromAsset=base_asset, toAsset=quote_asset)

            # Find the matching pair
            for pair in pairs:
                if pair.get("fromAsset") == base_asset and pair.get("toAsset") == quote_asset:
                    return ExchangeMinimums(
                        min_qty=Decimal(str(pair.get("fromAssetMinAmount", "0"))),
                        min_notional=Decimal(str(pair.get("toAssetMinAmount", "0"))),
                        step_size=Decimal("0"),  # Convert API doesn't have step_size
                    )

            # Also check the reverse direction (toAsset -> fromAsset)
            pairs_reverse = client.list_all_convert_pairs(
                fromAsset=quote_asset, toAsset=base_asset
            )
            for pair in pairs_reverse:
                if pair.get("fromAsset") == quote_asset and pair.get("toAsset") == base_asset:
                    return ExchangeMinimums(
                        min_qty=Decimal(str(pair.get("toAssetMinAmount", "0"))),
                        min_notional=Decimal(str(pair.get("fromAssetMinAmount", "0"))),
                        step_size=Decimal("0"),
                    )

            # Pair not found - return zeros (let the API reject if invalid)
            logger.warning(
                f"Convert pair {base_asset}/{quote_asset} not found in convert pairs list"
            )
            return ExchangeMinimums(
                min_qty=Decimal("0"),
                min_notional=Decimal("0"),
                step_size=Decimal("0"),
            )

        except ClientError as e:
            raise ExchangeConnectionError(f"Failed to get Convert pair info: {e.error_message}")
        except RequestException as e:
            raise ExchangeConnectionError(f"Failed to get Convert pair info: {e}")

    def get_balance(self, asset: str) -> Decimal:
        """
        Get available balance from Binance.

        Args:
            asset: The asset symbol (e.g., 'HIVE', 'BTC')

        Returns:
            Available balance as Decimal
        """
        try:
            balances = get_balances([asset], testnet=False)
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
            raw = get_balances(assets, testnet=False)
            result = {k: Decimal(str(v)) for k, v in raw.items()}
            return result
        except BinanceErrorBadConnection as e:
            raise ExchangeConnectionError(f"Failed to get Binance balances: {e}")

    def get_current_price(self, base_asset: str, quote_asset: str) -> Decimal:
        """
        Get current market price from Binance.

        Uses the spot order book mid-price as reference.
        The actual swap rate may differ slightly (includes spread/fee).

        Args:
            base_asset: The base asset (e.g., 'HIVE')
            quote_asset: The quote asset (e.g., 'BTC')

        Returns:
            Current mid-price as Decimal
        """
        symbol = self.build_symbol(base_asset, quote_asset)
        try:
            return get_mid_price(symbol, testnet=False)
        except BinanceErrorBadConnection as e:
            raise ExchangeConnectionError(f"Failed to get Binance price: {e}")

    def send_quote_request(
        self,
        from_asset: str,
        to_asset: str,
        from_amount: Decimal | None = None,
        to_amount: Decimal | None = None,
        valid_time: str = "10s",
        wallet_type: str = "SPOT",
    ) -> ConvertQuoteResult:
        """
        Request a conversion quote from Binance.

        You must specify either from_amount or to_amount (not both).

        Args:
            from_asset: Asset to convert from (e.g., 'HIVE')
            to_asset: Asset to convert to (e.g., 'BTC')
            from_amount: Amount of from_asset to spend (mutually exclusive with to_amount)
            to_amount: Amount of to_asset to receive (mutually exclusive with from_amount)
            valid_time: Quote validity period ('10s', '30s', '1m', '2m')
            wallet_type: Wallet to use ('SPOT', 'FUNDING', 'SPOT_FUNDING', etc.)

        Returns:
            ConvertQuoteResult with quote details

        Raises:
            ExchangeConnectionError: If the API call fails
            ExchangeBelowMinimumError: If the amount is too low
            ValueError: If neither from_amount nor to_amount is specified
        """
        if from_amount is None and to_amount is None:
            raise ValueError("Either from_amount or to_amount must be specified")

        try:
            client = self._get_client()

            kwargs: dict = {
                "validTime": valid_time,
                "walletType": wallet_type,
            }
            if from_amount is not None:
                kwargs["fromAmount"] = self._format_amount(from_amount)
            if to_amount is not None:
                kwargs["toAmount"] = self._format_amount(to_amount)

            response = client.send_quote_request(
                fromAsset=from_asset,
                toAsset=to_asset,
                **kwargs,
            )

            quote = ConvertQuoteResult.from_binance_response(response)

            logger.info(
                f"Convert quote received: {from_asset} -> {to_asset}, "
                f"from={quote.from_amount}, to={quote.to_amount}, "
                f"ratio={quote.ratio}, expires_in={quote.expires_in_seconds:.1f}s"
            )

            return quote

        except ClientError as e:
            error_msg = e.error_message if hasattr(e, "error_message") else str(e)
            if "MIN" in str(error_msg).upper() or "MINIMUM" in str(error_msg).upper():
                raise ExchangeBelowMinimumError(f"Convert amount below minimum: {error_msg}")
            raise ExchangeConnectionError(f"Failed to get Convert quote: {error_msg}")
        except RequestException as e:
            raise ExchangeConnectionError(f"Failed to get Convert quote: {e}")

    def accept_quote(self, quote_id: str) -> ConvertAcceptResult:
        """
        Accept a previously received conversion quote.

        The quote must not be expired.

        Args:
            quote_id: The quote ID from send_quote_request

        Returns:
            ConvertAcceptResult with order details

        Raises:
            ExchangeConnectionError: If the API call fails
            ExchangeQuoteExpiredError: If the quote has expired
        """
        try:
            client = self._get_client()
            response = client.accept_quote(quoteId=quote_id)
            result = ConvertAcceptResult.from_binance_response(response)

            logger.info(
                f"Convert quote accepted: orderId={result.order_id}, status={result.order_status}"
            )

            return result

        except ClientError as e:
            error_msg = e.error_message if hasattr(e, "error_message") else str(e)
            if "EXPIRED" in str(error_msg).upper():
                raise ExchangeQuoteExpiredError(f"Convert quote expired: {error_msg}")
            raise ExchangeConnectionError(f"Failed to accept Convert quote: {error_msg}")
        except RequestException as e:
            raise ExchangeConnectionError(f"Failed to accept Convert quote: {e}")

    def get_order_status(self, order_id: str) -> ConvertOrderStatus:
        """
        Query the status of a Convert order.

        Args:
            order_id: The order ID returned from accept_quote

        Returns:
            ConvertOrderStatus with full order details
        """
        try:
            client = self._get_client()
            response = client.order_status(orderId=order_id)
            return ConvertOrderStatus.from_binance_response(response)
        except ClientError as e:
            raise ExchangeConnectionError(f"Failed to get Convert order status: {e.error_message}")
        except RequestException as e:
            raise ExchangeConnectionError(f"Failed to get Convert order status: {e}")

    @backoff.on_exception(
        backoff.constant,
        ExchangeConnectionError,
        max_tries=3,
        interval=1,
        logger=logger,
    )
    def _wait_for_order_completion(self, order_id: str) -> ConvertOrderStatus:
        """
        Poll order status until it reaches a terminal state (SUCCESS or FAIL).

        Args:
            order_id: The order ID to poll

        Returns:
            ConvertOrderStatus with final status

        Raises:
            ExchangeError: If the order fails or times out
        """
        deadline = time.time() + self.ORDER_STATUS_TIMEOUT

        while time.time() < deadline:
            status = self.get_order_status(order_id)

            if status.order_status == "SUCCESS":
                logger.info(
                    f"Convert order {order_id} completed: "
                    f"{status.from_amount} {status.from_asset} -> "
                    f"{status.to_amount} {status.to_asset}"
                )
                return status

            if status.order_status == "FAIL":
                raise ExchangeError(f"Convert order {order_id} failed: {status.raw_response}")

            # Still processing - wait and retry
            time.sleep(self.ORDER_STATUS_POLL_INTERVAL)

        raise ExchangeError(
            f"Convert order {order_id} timed out after {self.ORDER_STATUS_TIMEOUT}s"
        )

    def _build_trade_quote(
        self,
        from_asset: str,
        to_asset: str,
        ratio: Decimal,
    ) -> QuoteResponse:
        """
        Build a QuoteResponse reflecting the actual swap rate,
        preserving market prices for other conversions.

        For HIVE->BTC or BTC->HIVE swaps:
            - Uses actual swap ratio for sats_hive calculation
            - Preserves real BTC/USD, HBD/USD rates from market

        Args:
            from_asset: The source asset
            to_asset: The destination asset
            ratio: The conversion ratio (to_amount / from_amount)

        Returns:
            QuoteResponse with rates reflecting the actual swap
        """
        import asyncio
        from datetime import datetime, timezone

        from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes

        # Fetch current market quote for accurate HBD/USD/BTC rates
        all_quotes = AllQuotes()
        try:
            try:
                asyncio.get_running_loop()
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, all_quotes.get_all_quotes())
                    future.result(timeout=5)
            except RuntimeError:
                asyncio.run(all_quotes.get_all_quotes())
        except Exception as e:
            logger.warning(f"Failed to fetch market quote for trade_quote: {e}")
            all_quotes.quote = QuoteResponse()

        market_quote = all_quotes.quote
        btc_usd = market_quote.btc_usd if market_quote.btc_usd > 0 else Decimal("1")

        # Determine the effective HIVE/BTC price from the swap ratio
        if from_asset.upper() == "HIVE" and to_asset.upper() == "BTC":
            # Selling HIVE for BTC: ratio = BTC received per HIVE
            # avg_price (in BTC per HIVE) = ratio
            avg_price_btc = ratio
            trade_hive_usd = avg_price_btc * btc_usd
            return QuoteResponse(
                hive_usd=trade_hive_usd,
                hbd_usd=market_quote.hbd_usd,
                btc_usd=btc_usd,
                hive_hbd=market_quote.hive_hbd,
                source=f"{self.exchange_name}_trade",
                fetch_date=datetime.now(tz=timezone.utc),
            )
        elif from_asset.upper() == "BTC" and to_asset.upper() == "HIVE":
            # Buying HIVE with BTC: ratio = HIVE received per BTC
            # avg_price (BTC per HIVE) = 1 / ratio
            avg_price_btc = Decimal("1") / ratio if ratio > 0 else Decimal("0")
            trade_hive_usd = avg_price_btc * btc_usd
            return QuoteResponse(
                hive_usd=trade_hive_usd,
                hbd_usd=market_quote.hbd_usd,
                btc_usd=btc_usd,
                hive_hbd=market_quote.hive_hbd,
                source=f"{self.exchange_name}_trade",
                fetch_date=datetime.now(tz=timezone.utc),
            )
        elif from_asset.upper() == "HBD" and to_asset.upper() == "BTC":
            avg_price_btc = ratio
            trade_hbd_usd = avg_price_btc * btc_usd
            return QuoteResponse(
                hive_usd=market_quote.hive_usd,
                hbd_usd=trade_hbd_usd,
                btc_usd=btc_usd,
                hive_hbd=market_quote.hive_hbd,
                source=f"{self.exchange_name}_trade",
                fetch_date=datetime.now(tz=timezone.utc),
            )
        elif from_asset.upper() == "BTC" and to_asset.upper() == "HBD":
            avg_price_btc = Decimal("1") / ratio if ratio > 0 else Decimal("0")
            trade_hbd_usd = avg_price_btc * btc_usd
            return QuoteResponse(
                hive_usd=market_quote.hive_usd,
                hbd_usd=trade_hbd_usd,
                btc_usd=btc_usd,
                hive_hbd=market_quote.hive_hbd,
                source=f"{self.exchange_name}_trade",
                fetch_date=datetime.now(tz=timezone.utc),
            )
        else:
            return QuoteResponse(
                hive_usd=market_quote.hive_usd,
                hbd_usd=market_quote.hbd_usd,
                btc_usd=market_quote.btc_usd,
                hive_hbd=market_quote.hive_hbd,
                source=f"{self.exchange_name}_trade",
                fetch_date=datetime.now(tz=timezone.utc),
            )

    def _execute_swap(
        self,
        from_asset: str,
        to_asset: str,
        from_amount: Decimal,
        side: str,
        base_asset: str,
        quote_asset: str,
        valid_time: str = "10s",
    ) -> ExchangeOrderResult:
        """
        Execute a Convert swap: request a quote, accept it, wait for completion.

        Args:
            from_asset: Asset to convert from
            to_asset: Asset to convert to
            from_amount: Amount of from_asset to spend
            side: 'BUY' or 'SELL' (for the ExchangeOrderResult)
            base_asset: The base asset of the logical pair (e.g., 'HIVE')
            quote_asset: The quote asset of the logical pair (e.g., 'BTC')
            valid_time: Quote validity period

        Returns:
            ExchangeOrderResult with execution details
        """
        # Step 1: Request a quote
        quote = self.send_quote_request(
            from_asset=from_asset,
            to_asset=to_asset,
            from_amount=from_amount,
            valid_time=valid_time,
        )

        # Step 2: Accept the quote
        accept_result = self.accept_quote(quote.quote_id)

        # Step 3: Wait for order completion (poll status)
        order_status = self._wait_for_order_completion(accept_result.order_id)

        # Step 4: Build the result
        # Determine executed quantities in terms of the base/quote pair
        if side == "SELL":
            # Selling base_asset for quote_asset
            # from_asset=base_asset, to_asset=quote_asset
            executed_qty = order_status.from_amount
            quote_qty = order_status.to_amount
            # avg_price = quote_asset per base_asset
            avg_price = quote_qty / executed_qty if executed_qty > 0 else Decimal("0")
        else:
            # Buying base_asset with quote_asset
            # from_asset=quote_asset, to_asset=base_asset
            executed_qty = order_status.to_amount
            quote_qty = order_status.from_amount
            # avg_price = quote_asset per base_asset
            avg_price = quote_qty / executed_qty if executed_qty > 0 else Decimal("0")

        # Build trade quote from the actual conversion ratio
        # For sell: from→to means base→quote, ratio = to_amount/from_amount = avg_price
        # For buy:  from→to means quote→base, ratio = to_amount/from_amount = 1/avg_price
        trade_quote = self._build_trade_quote(
            from_asset=from_asset,
            to_asset=to_asset,
            ratio=order_status.ratio,
        )

        symbol = self.build_symbol(base_asset, quote_asset)

        return ExchangeOrderResult(
            exchange=self.exchange_name,
            symbol=symbol,
            order_id=order_status.order_id,
            client_order_id="",
            side=side,
            status=order_status.order_status,
            requested_qty=from_amount if side == "SELL" else executed_qty,
            executed_qty=executed_qty,
            quote_qty=quote_qty,
            avg_price=avg_price,
            fee_msats=Decimal("0"),  # Fees hidden in rate
            fee_original=Decimal("0"),
            fee_asset="",
            raw_response=order_status.raw_response,
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
        Execute a sell via Binance Convert.

        Sells base_asset to receive quote_asset.
        e.g., market_sell('HIVE', 'BTC', 100) sells 100 HIVE to receive BTC.

        Args:
            base_asset: The asset to sell (e.g., 'HIVE')
            quote_asset: The asset to receive (e.g., 'BTC')
            quantity: Amount of base asset to sell
            client_order_id: Not used by Convert API (accepted for interface compat)

        Returns:
            ExchangeOrderResult with execution details
        """
        if client_order_id:
            logger.debug(f"client_order_id '{client_order_id}' ignored by Convert API")

        return self._execute_swap(
            from_asset=base_asset,
            to_asset=quote_asset,
            from_amount=quantity,
            side="SELL",
            base_asset=base_asset,
            quote_asset=quote_asset,
        )

    def market_buy(
        self,
        base_asset: str,
        quote_asset: str,
        quantity: Decimal,
        client_order_id: str | None = None,
    ) -> ExchangeOrderResult:
        """
        Execute a buy via Binance Convert.

        Buys base_asset by spending quote_asset.
        Note: The Convert API works with from/to assets, so for a buy we
        need to request a quote from quote_asset -> base_asset for the
        desired amount of base_asset (using toAmount).

        Args:
            base_asset: The asset to buy (e.g., 'HIVE')
            quote_asset: The asset to spend (e.g., 'BTC')
            quantity: Amount of base asset to buy
            client_order_id: Not used by Convert API (accepted for interface compat)

        Returns:
            ExchangeOrderResult with execution details
        """
        if client_order_id:
            logger.debug(f"client_order_id '{client_order_id}' ignored by Convert API")

        # For a buy, we want to receive `quantity` of base_asset
        # So we request a quote: from=quote_asset, to=base_asset, toAmount=quantity
        quote = self.send_quote_request(
            from_asset=quote_asset,
            to_asset=base_asset,
            to_amount=quantity,
        )

        # Accept the quote
        accept_result = self.accept_quote(quote.quote_id)

        # Wait for completion
        order_status = self._wait_for_order_completion(accept_result.order_id)

        # Build result
        # order_status: from=quote_asset, to=base_asset
        executed_qty = order_status.to_amount  # base_asset received
        quote_qty = order_status.from_amount  # quote_asset spent
        avg_price = quote_qty / executed_qty if executed_qty > 0 else Decimal("0")

        trade_quote = self._build_trade_quote(
            from_asset=quote_asset,
            to_asset=base_asset,
            ratio=order_status.ratio,
        )

        symbol = self.build_symbol(base_asset, quote_asset)

        return ExchangeOrderResult(
            exchange=self.exchange_name,
            symbol=symbol,
            order_id=order_status.order_id,
            client_order_id="",
            side="BUY",
            status=order_status.order_status,
            requested_qty=quantity,
            executed_qty=executed_qty,
            quote_qty=quote_qty,
            avg_price=avg_price,
            fee_msats=Decimal("0"),  # Fees hidden in rate
            fee_original=Decimal("0"),
            fee_asset="",
            raw_response=order_status.raw_response,
            base_asset=base_asset,
            quote_asset=quote_asset,
            trade_quote=trade_quote,
        )
