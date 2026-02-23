from dataclasses import dataclass
from typing import Any, Dict

from nectar.account import Account
from nectar.amount import Amount
from nectar.hive import Hive
from nectar.market import Market
from nectar.price import Price
from nectarapi.exceptions import UnhandledRPCError

from v4vapp_backend_v2.config.setup import (
    HiveAccountConfig,
    HiveTradeDirection,
    InternalConfig,
    logger,
)
from v4vapp_backend_v2.hive.hive_extras import get_hive_client

ORDER_BOOK_CACHE: Dict[str, Any] = {}
ICON = "📈"


@dataclass
class HiveQuote:
    trade: Amount
    price: Price
    minimum_amount: Amount


def account_trade(
    hive_acc: HiveAccountConfig, set_amount_to: Amount, nobroadcast: bool = False
) -> dict:
    """
    Executes a trade for a given Hive account to reach a specified amount.

    Args:
        hive_acc (HiveAccountConfig): Configuration for the Hive account.
        set_amount_to (Amount): The target amount to set for the account.

    Returns:
        dict: An empty dictionary.

    Logs:
        - Information about the account's current balance.
        - Information about the trade to be executed if the
            account's balance exceeds the target amount.
        - Information if the account's balance is below the target amount.
    """
    if hive_acc is None:
        raise ValueError(f"Account {hive_acc.name} not found in config")
    hive_configs = InternalConfig().config.hive
    if not hive_configs or not hive_configs.hive_accs:
        message = "No Hive accounts found in config"
        logger.error(f"{ICON} {message}")
        raise ValueError(message)

    if isinstance(hive_acc, str):
        retrieved_acc = hive_configs.hive_accs.get(hive_acc)
        if retrieved_acc is None:
            raise ValueError(f"Account {hive_acc} not found in config")
        hive_acc = retrieved_acc
    elif isinstance(hive_acc, HiveAccountConfig):
        if hive_acc.name and not hive_acc.keys:
            hive_acc = hive_configs.hive_accs.get(hive_acc.name, HiveAccountConfig())

    if not hive_acc or not hive_acc.active_key:
        logger.error(f"{ICON} Account {hive_acc.name} not found in config")
        raise ValueError(f"Account {hive_acc.name} Active Keys not found in config")

    hive = get_hive_client(keys=hive_acc.keys, nobroadcast=nobroadcast)
    account = Account(hive_acc.name, blockchain_instance=hive)
    balance: Dict[str, Amount] = {}
    balance["HIVE"] = account.available_balances[0]
    balance["HBD"] = account.available_balances[1]
    delta = balance[set_amount_to.symbol] - set_amount_to
    if hive_acc.threshold_delta:
        threshold_delta_amount = Amount(hive_acc.threshold_delta)
    else:
        threshold_delta_amount = Amount("1.000 HBD")  # default to 0 if not set
    # delta.amount may be negative if we need to buy the asset instead of selling it
    if abs(delta.amount) > threshold_delta_amount.amount:
        if hive_acc.hbd_trade_direction == HiveTradeDirection.none:
            logger.info(
                f"{ICON} Account {hive_acc.name} balance is outside threshold but hbd_trade_direction is set to 'none', so no trade will be executed"
            )
            return {}
        if hive_acc.hbd_trade_direction != HiveTradeDirection.both:
            if delta.amount > 0 and hive_acc.hbd_trade_direction != HiveTradeDirection.sell:
                logger.info(
                    f"{ICON} Account {hive_acc.name} balance is above threshold but hbd_trade_direction is set to '{hive_acc.hbd_trade_direction}', so no sell trade will be executed"
                )
                return {}
            elif delta.amount < 0 and hive_acc.hbd_trade_direction != HiveTradeDirection.buy:
                logger.info(
                    f"{ICON} Account {hive_acc.name} balance is below threshold but hbd_trade_direction is set to '{hive_acc.hbd_trade_direction}', so no buy trade will be executed"
                )
                return {}
        logger.info(
            f"{ICON} "
            f"Account {hive_acc.name} has balance: {balance[set_amount_to.symbol]} "
            f"and will trade {delta} to reach {set_amount_to}"
        )
        trx = market_trade(hive_acc, delta)
        return trx
    else:
        if delta.amount == 0:
            delta_text = "the same as"
        else:
            delta_text = f"{delta} below" if delta.amount < 0 else f"{delta} above"
        logger.info(f"{ICON} Account {hive_acc.name} balance is {delta_text} {set_amount_to}")
    return {}


def market_trade(
    hive_acc: HiveAccountConfig,
    amount: Amount,
    use_cache: bool = False,
    killfill: bool = False,
    nobroadcast: bool = False,
) -> Any:
    """
    Executes a market trade on the Hive blockchain, either selling or buying the specified asset.
    A *positive* amount value means sell the asset; a *negative* amount means buy the
    asset in order to raise the account's balance to the target.

    Args:
        hive_acc (HiveAccountConfig): The Hive account configuration containing account details and keys.
        amount (Amount): The amount to trade, including the symbol (e.g., "HIVE" or "HBD").
        use_cache (bool, optional): Whether to use cached order book data. Defaults to False.
        killfill (bool, optional): Whether to enforce a kill-fill order (trade must be fully executed or canceled). Defaults to False.
        nobroadcast (bool, optional): If True, the transaction will not be broadcasted to the blockchain. Defaults to False.

    Returns:
        dict: A dictionary containing transaction details, including the transaction ID.

    Raises:
        UnhandledRPCError: If an unhandled RPC error occurs during the trade.
        Exception: For any other exceptions encountered during the trade.

    Notes:
        - The function determines the market direction based on the symbol of the `amount` parameter.
        - Logs transaction details and notifications for successful trades.
        - Generates links to the Hive block explorer for the transaction.
    """
    quote = None
    try:
        hive_configs = InternalConfig().config.hive
        if hive_acc.name in hive_configs.hive_accs:
            hive_acc = hive_configs.hive_accs[hive_acc.name]

        hive = get_hive_client(keys=hive_acc.keys, nobroadcast=nobroadcast)
        try:
            quote = check_order_book(amount, hive, use_cache=use_cache)
        except ValueError as e:
            logger.warning(
                f"{ICON} Market Trade error: {e}",
                extra={"notification": True, "quote": None, "error": e},
            )
            raise e
        if not quote:
            raise ValueError("No quote available for the trade")
        price_float = float(quote.price["price"])

        # determine direction based on sign of amount
        is_buy = amount.amount < 0
        abs_amount = Amount(abs(amount.amount), amount.symbol)

        if is_buy:
            # buying base asset
            if amount.symbol == "HIVE":
                market = Market("HIVE:HBD", blockchain_instance=hive)
                trx = market.buy(
                    price=price_float,
                    amount=str(abs_amount),
                    account=hive_acc.name,
                    killfill=killfill,
                )
            else:
                market = Market("HBD:HIVE", blockchain_instance=hive)
                trx = market.buy(
                    price=1 / price_float,
                    amount=str(abs_amount),
                    account=hive_acc.name,
                    killfill=killfill,
                )
        else:
            # selling base asset (existing behavior)
            if amount.symbol == "HIVE":
                market = Market("HIVE:HBD", blockchain_instance=hive)
                trx = market.sell(
                    price=price_float,
                    amount=str(abs_amount),
                    account=hive_acc.name,
                    killfill=killfill,
                )
            else:
                market = Market("HBD:HIVE", blockchain_instance=hive)
                trx = market.sell(
                    price=1 / price_float,
                    amount=str(abs_amount),
                    account=hive_acc.name,
                    killfill=killfill,
                )
        return trx
    except UnhandledRPCError as e:
        logger.warning(
            f"Market Trade error: {e}",
            extra={"notification": True, "quote": quote, "error": e},
        )
        raise e
    except Exception as e:
        logger.warning(
            f"{ICON} Market Trade error: {e}",
            exc_info=True,
            extra={"notification": False, "quote": quote, "error": e},
        )
        raise e


def check_order_book(
    amount: Amount,
    hive: Hive | None = None,
    use_cache: bool = False,
    order_book_limit: int = 500,
) -> HiveQuote:
    """
    Check the order book for the given amount and return the best price.

    Args:
        amount (Amount): The amount of the asset to check in the order book.
        hive (Hive, optional): The Hive client instance to use.
            If not provided, a new client will be created.
        use_cache (bool, optional): Whether to use the cached order book data. Defaults to False.

    Returns:
        HiveQuote: An object containing the amount, the best price, and the minimum amount.

    Raises:
        ValueError: If the amount is negative or if there is not enough volume in the order book.

    Note:
        The function assumes that the order book always uses HIVE as the base asset.
    """

    global ORDER_BOOK_CACHE

    # allow negative amounts to represent a buy order
    sign = 1 if amount.amount >= 0 else -1
    # construct an Amount with absolute value; let the constructor handle spacing
    abs_amount = Amount(abs(amount.amount), amount.symbol)

    if abs_amount.amount <= 0:
        raise ValueError("Amount must be non-zero")

    if not hive:
        hive = get_hive_client()

    base_asset = amount.symbol  # the asset we want to trade
    quote_asset = "HBD" if base_asset == "HIVE" else "HIVE"

    if use_cache and ORDER_BOOK_CACHE:
        order_book = ORDER_BOOK_CACHE
    else:
        # Note: The order book is the same and always uses HIVE as the base asset
        market = Market(base=base_asset, quote=quote_asset, blockchain_instance=hive)
        order_book = market.orderbook(limit=order_book_limit)
        ORDER_BOOK_CACHE = order_book

    # choose side depending on buy/sell and asset
    if sign > 0:
        # selling base asset (existing behaviour)
        if base_asset == "HIVE":
            orders_bids = order_book["bids"]
            orders_bids.sort(key=lambda x: x["price"], reverse=True)
        else:  # selling HBD
            orders_bids = order_book["asks"]
            orders_bids.sort(key=lambda x: x["price"], reverse=False)
    else:
        # buying base asset
        if base_asset == "HIVE":
            orders_bids = order_book["asks"]
            orders_bids.sort(key=lambda x: x["price"], reverse=False)
        else:  # buying HBD (sell HIVE) — take asks to hit liquidity immediately
            orders_bids = order_book["asks"]
            orders_bids.sort(key=lambda x: x["price"], reverse=False)

    cumulative_volume = 0.0
    total_received = 0.0
    final_price: None | Price = None

    # iterate until we've covered the desired amount of base asset
    for order in orders_bids:
        order_price = float(order["price"])
        quote = order["quote"]
        base = order["base"]

        if sign > 0:
            # selling path, same as before
            if base_asset == "HIVE":
                volume = float(quote)
                price = Price(str(order_price), base_asset, quote_asset)  # type: ignore[arg-type]
                total_received += volume
            else:  # Selling HBD
                volume = float(base)
                price = Price(str(order_price), quote_asset, base_asset)  # type: ignore[arg-type]
                total_received += volume
        else:
            # buying path, volume is amount of base asset we'll acquire
            if base_asset == "HIVE":
                volume = float(base)
                price = Price(str(order_price), base_asset, quote_asset)  # type: ignore[arg-type]
                total_received += volume
            else:  # buying HBD
                volume = float(quote)
                price = Price(str(order_price), quote_asset, base_asset)  # type: ignore[arg-type]
                total_received += volume

        cumulative_volume += volume
        if cumulative_volume >= abs_amount.amount:
            final_price = price
            break

    if final_price is None:
        raise ValueError("Not enough volume in the order book")

    logger.debug(f"{ICON} Best price for {amount} is {final_price}")

    # minimum amount expressed in quote asset (cost when buying, proceeds when selling)
    if base_asset == "HIVE":
        min_amt = abs_amount.amount * float(final_price["price"])
    else:
        min_amt = abs_amount.amount / float(final_price["price"])

    minimum_amount = Amount(min_amt, quote_asset)

    logger.debug(f"{ICON} Minimum amount: {minimum_amount}")

    hive_quote = HiveQuote(abs_amount, final_price, minimum_amount)
    return hive_quote


if __name__ == "__main__":
    # try:
    #     account_trade(HiveAccountConfig(name="v4vapp-test"), Amount("2.5 HBD"))
    #     account_trade(HiveAccountConfig(name="v4vapp-test"), Amount("14 HIVE"))
    # except Exception as e:
    #     logger.info(f"{icon} {e}")
    InternalConfig(config_filename="devhive.config.yaml")
    try:
        trade = Amount("0.2 HBD")
        trx = market_trade(HiveAccountConfig(name="v4vapp-test"), trade)
        logger.info(f"{ICON} trx: {trx}", extra={"notification": False})
    except Exception as e:
        logger.info(f"{ICON} {e}")

    # try:
    #     trade = Amount("1 HIVE")
    #     trx2 = market_trade(HiveAccountConfig(name="v4vapp-test"), trade)
    #     logger.info(f"{icon} " f"trx2: {trx2}", extra={"notification": False})
    # except Exception as e:
    #     logger.info(f"{icon} {e}")
