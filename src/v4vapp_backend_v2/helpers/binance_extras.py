from decimal import Decimal
from typing import Dict

from binance.error import ClientError  # type: ignore
from binance.spot import Spot as Client  # type: ignore
from pydantic import BaseModel

from v4vapp_backend_v2.config.setup import InternalConfig, logger


class BinanceErrorLowBalance(Exception):
    pass


class BinanceErrorBadConnection(Exception):
    pass


class BinanceErrorBelowMinimum(Exception):
    """Raised when the order amount is below the minimum required by Binance."""

    pass


def get_client(testnet: bool = False) -> Client:
    """
    Get a Binance API client.

    Uses the exchange_config section from the configuration to get API credentials.
    The testnet parameter determines which network config (testnet/mainnet) to use,
    but if exchange_mode is set to testnet in config, testnet will be used regardless.
    """
    internal_config = InternalConfig().config
    exchange_config = internal_config.exchange_config
    binance_config = exchange_config.get_provider("binance")

    # Use testnet if either the config says testnet or the parameter says testnet
    use_testnet = binance_config.is_testnet or testnet

    try:
        if use_testnet:
            network_config = binance_config.testnet
            base_url = network_config.base_url or "https://testnet.binance.vision"
            client = Client(
                api_key=network_config.resolved_api_key,
                api_secret=network_config.resolved_api_secret,
                base_url=base_url,
            )
        else:
            network_config = binance_config.mainnet
            client = Client(
                api_key=network_config.resolved_api_key,
                api_secret=network_config.resolved_api_secret,
            )
        return client
    except Exception as e:
        logger.error(e)
        raise e


def get_balances(symbols: list, testnet: bool = False) -> Dict[str, Decimal | int]:
    """
    Get balances for a list of symbols.
    This will work always on testnet. If the IP address is not whitelisted on Binance
    then it will fail and raise BinanceErrorBadConnection

    Returns:
        dict: Balances as Decimal values for each symbol
    """
    try:
        client = get_client(testnet)
        account = client.account()
        symbols_set = set(symbols)  # Convert to set for O(1) lookups
        balances: dict[str, Decimal | int] = {
            symbol: Decimal("0") for symbol in symbols
        }  # Initialize all balances to 0
        found_count = 0
        for balance in account["balances"]:
            if balance["asset"] in symbols_set:
                balances[balance["asset"]] = Decimal(balance["free"])
                found_count += 1
                if found_count == len(symbols_set):
                    break  # Early exit once all symbols found
        if "BTC" in balances and balances["BTC"] > Decimal("0"):
            balances["SATS"] = Decimal(balances["BTC"] * Decimal("100000000"))
        return balances
    except ClientError as error:
        logger.error(
            f"Found error. status: {error.status_code}, error code: {error.error_code}, error message: {error.error_message}",
            extra={"notification": False},
        )
        raise BinanceErrorBadConnection(error.error_message)
    except Exception as error:
        logger.error(error)
        raise BinanceErrorBadConnection(error)


def get_current_price(symbol: str, testnet: bool = False) -> dict:
    """
    Retrieve the current price details for a given trading symbol from Binance.

    Args:
        symbol (str): The trading pair symbol (e.g., 'BTCUSDT') for which to fetch the price.
        testnet (bool, optional): Whether to use the Binance testnet. Defaults to False.

    Returns:
        dict: A dictionary containing the following keys:
            - "ask_price" (str): The current ask price for the symbol.
            - "bid_price" (str): The current bid price for the symbol.
            - "current_price" (str): The latest price for the symbol.
    """

    client = get_client(testnet)

    price = {}
    ticker_info = client.book_ticker(symbol)
    price["ask_price"] = ticker_info["askPrice"]
    price["bid_price"] = ticker_info["bidPrice"]
    ticker_price = client.ticker_price(symbol)
    price["current_price"] = ticker_price["price"]
    return price


class MarketOrderResult(BaseModel):
    """
    Model to store the result of a market order (buy or sell).
    All monetary values are stored as Decimal for precision.
    """

    model_config = {"arbitrary_types_allowed": True}

    symbol: str
    order_id: int
    client_order_id: str
    transact_time: int
    orig_qty: Decimal
    executed_qty: Decimal
    cummulative_quote_qty: Decimal  # Total quote asset received/spent
    status: str
    type: str
    side: str
    avg_price: Decimal  # Calculated average price (quote per base unit)
    fills: list  # List of individual fills with price, qty, commission
    raw_response: dict  # Store the full response for reference

    @classmethod
    def from_binance_response(cls, response: dict) -> "MarketOrderResult":
        """
        Create a MarketOrderResult from a Binance API response.

        Args:
            response: The raw response from Binance new_order API

        Returns:
            MarketOrderResult with calculated average price
        """
        executed_qty = Decimal(response.get("executedQty", "0"))
        cummulative_quote_qty = Decimal(response.get("cummulativeQuoteQty", "0"))

        # Calculate average price (quote asset per base unit)
        avg_price = Decimal("0")
        if executed_qty > Decimal("0"):
            avg_price = cummulative_quote_qty / executed_qty

        return cls(
            symbol=response.get("symbol", ""),
            order_id=response.get("orderId", 0),
            client_order_id=response.get("clientOrderId", ""),
            transact_time=response.get("transactTime", 0),
            orig_qty=Decimal(response.get("origQty", "0")),
            executed_qty=executed_qty,
            cummulative_quote_qty=cummulative_quote_qty,
            status=response.get("status", ""),
            type=response.get("type", ""),
            side=response.get("side", ""),
            avg_price=avg_price,
            fills=response.get("fills", []),
            raw_response=response,
        )


# Alias for backward compatibility
MarketSellResult = MarketOrderResult


def get_symbol_info(symbol: str, testnet: bool = False) -> dict:
    """
    Get exchange info for a specific symbol including minimum order requirements.

    Args:
        symbol: The trading pair symbol (e.g., 'HIVEBTC')
        testnet: Whether to use the Binance testnet

    Returns:
        dict: Symbol info including filters for LOT_SIZE and MIN_NOTIONAL
    """
    client = get_client(testnet)
    exchange_info = client.exchange_info(symbol=symbol)

    for sym in exchange_info.get("symbols", []):
        if sym["symbol"] == symbol:
            return sym

    return {}


def get_min_order_quantity(symbol: str, testnet: bool = False) -> tuple[Decimal, Decimal]:
    """
    Get the minimum order quantity and minimum notional value for a symbol.

    Args:
        symbol: The trading pair symbol (e.g., 'HIVEBTC')
        testnet: Whether to use the Binance testnet

    Returns:
        tuple: (min_qty from LOT_SIZE filter, min_notional from MIN_NOTIONAL/NOTIONAL filter)
    """
    symbol_info = get_symbol_info(symbol, testnet)
    filters = symbol_info.get("filters", [])

    min_qty = Decimal("0")
    min_notional = Decimal("0")

    for f in filters:
        if f["filterType"] == "LOT_SIZE":
            min_qty = Decimal(f.get("minQty", "0"))
        elif f["filterType"] in ("MIN_NOTIONAL", "NOTIONAL"):
            min_notional = Decimal(f.get("minNotional", "0"))

    return min_qty, min_notional


def market_order(
    symbol: str,
    side: str,
    quantity: Decimal,
    testnet: bool = False,
) -> MarketOrderResult:
    """
    Execute a market order for any trading pair supported by Binance.

    This function places a market order which executes immediately at the best
    available price in the order book.

    Args:
        symbol: The trading pair symbol (e.g., 'HIVEBTC', 'BTCUSDT', 'ETHBTC')
        side: Order side - 'BUY' or 'SELL'
        quantity: The amount of the base asset to buy/sell (as Decimal)
        testnet: Whether to use the Binance testnet

    Returns:
        MarketOrderResult: Contains order details including the realized price

    Raises:
        BinanceErrorBelowMinimum: If the order amount is below the minimum required
        BinanceErrorBadConnection: If there's a connection or API error
        ValueError: If side is not 'BUY' or 'SELL'
    """
    side = side.upper()
    if side not in ("BUY", "SELL"):
        raise ValueError(f"Invalid side '{side}'. Must be 'BUY' or 'SELL'.")

    try:
        # Get minimum order requirements
        min_qty, min_notional = get_min_order_quantity(symbol, testnet)

        # Check if quantity meets minimum lot size
        if quantity < min_qty:
            raise BinanceErrorBelowMinimum(
                f"Order quantity {quantity} is below minimum lot size {min_qty} for {symbol}"
            )

        # Estimate notional value (quantity * approximate price)
        # Get current price to estimate notional
        price_info = get_current_price(symbol, testnet)
        # Use bid price for SELL, ask price for BUY
        price_key = "bid_price" if side == "SELL" else "ask_price"
        price = Decimal(price_info[price_key])
        estimated_notional = quantity * price

        if min_notional > Decimal("0") and estimated_notional < min_notional:
            raise BinanceErrorBelowMinimum(
                f"Order notional value ~{estimated_notional:.8f} is below minimum "
                f"{min_notional:.8f}. Need at least {min_notional / price:.8f} quantity for {symbol}"
            )

        # Place market order
        client = get_client(testnet)
        response = client.new_order(
            symbol=symbol,
            side=side,
            type="MARKET",
            quantity=str(quantity),
        )

        logger.info(
            f"Market {side.lower()} order executed: {quantity} {symbol}",
            extra={"response": response},
        )

        return MarketOrderResult.from_binance_response(response)

    except BinanceErrorBelowMinimum:
        # Re-raise our custom exception
        raise
    except ClientError as error:
        logger.error(
            f"Binance API error. status: {error.status_code}, "
            f"error code: {error.error_code}, error message: {error.error_message}",
            extra={"notification": False},
        )
        raise BinanceErrorBadConnection(error.error_message)
    except ValueError:
        # Re-raise ValueError for invalid side
        raise
    except Exception as error:
        logger.error(f"Error executing market {side.lower()}: {error}")
        raise BinanceErrorBadConnection(str(error))


def market_sell(
    symbol: str,
    quantity: Decimal,
    testnet: bool = False,
) -> MarketOrderResult:
    """
    Execute a market sell order for any trading pair.

    Convenience wrapper around market_order with side='SELL'.

    Args:
        symbol: The trading pair symbol (e.g., 'HIVEBTC', 'BTCUSDT')
        quantity: The amount of the base asset to sell (as Decimal)
        testnet: Whether to use the Binance testnet

    Returns:
        MarketOrderResult: Contains order details including the realized price
    """
    return market_order(symbol=symbol, side="SELL", quantity=quantity, testnet=testnet)


def market_buy(
    symbol: str,
    quantity: Decimal,
    testnet: bool = False,
) -> MarketOrderResult:
    """
    Execute a market buy order for any trading pair.

    Convenience wrapper around market_order with side='BUY'.

    Args:
        symbol: The trading pair symbol (e.g., 'HIVEBTC', 'BTCUSDT')
        quantity: The amount of the base asset to buy (as Decimal)
        testnet: Whether to use the Binance testnet

    Returns:
        MarketOrderResult: Contains order details including the realized price
    """
    return market_order(symbol=symbol, side="BUY", quantity=quantity, testnet=testnet)


def market_sell_to_btc(
    from_asset: str,
    quantity: Decimal,
    testnet: bool = False,
) -> MarketOrderResult:
    """
    Execute a market sell order to convert an asset to BTC as quickly as possible.

    This is a convenience function that constructs the symbol as {from_asset}BTC
    and calls market_sell.

    Args:
        from_asset: The asset to sell (e.g., 'HIVE')
        quantity: The amount of the asset to sell (as Decimal)
        testnet: Whether to use the Binance testnet

    Returns:
        MarketOrderResult: Contains order details including the realized price

    Raises:
        BinanceErrorBelowMinimum: If the order amount is below the minimum required
        BinanceErrorBadConnection: If there's a connection or API error
    """
    symbol = f"{from_asset}BTC"
    return market_sell(symbol=symbol, quantity=quantity, testnet=testnet)
