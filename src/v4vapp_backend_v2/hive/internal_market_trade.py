from dataclasses import dataclass
from typing import Any, Dict

from beem import Hive  # type: ignore
from beem.amount import Amount  # type: ignore
from beem.market import Market  # type: ignore
from beem.price import Price  # type: ignore
from beemapi.exceptions import UnhandledRPCError  # type: ignore

from v4vapp_backend_v2.config.setup import HiveAccountConfig, InternalConfig, logger
from v4vapp_backend_v2.hive.hive_extras import (
    get_hive_block_explorer_link,
    get_hive_client,
)

ORDER_BOOK_CACHE: Dict[str, Any] = {}


@dataclass
class HiveQuote:
    trade: Amount
    price: Price
    minimum_amount: Amount


def market_trade(
    hive_acc: HiveAccountConfig,
    amount: Amount,
    price: Price | None = None,
    use_cache: bool = False,
    killfill: bool = False,
    nobroadcast: bool = False,
    expiration: int = 3600,
) -> dict:
    hive_configs = InternalConfig().config.hive
    if hive_acc.name in hive_configs.hive_accs:
        hive_acc = hive_configs.hive_accs[hive_acc.name]
    hive = get_hive_client(keys=hive_acc.keys, nobroadcast=nobroadcast)
    quote = check_order_book(amount, hive, use_cache=use_cache)

    base_asset = amount.symbol
    quote_asset = "HBD" if base_asset == "HIVE" else "HIVE"
    market_symbol = f"{base_asset}:{quote_asset}"
    market = Market(market_symbol, blockchain_instance=hive)
    amount_str = str(amount)
    price_float = float(quote.price["price"])
    logger.info(
        f"Converting {amount} to {quote.minimum_amount} at {quote.price}",
        extra={"notification": True, "quote": quote},
    )
    try:
        trx = market.sell(
            price=price_float,
            amount=amount_str,
            account=hive_acc.name,
            killfill=killfill,
            expiration=expiration,
        )
        link = get_hive_block_explorer_link(trx.get("trx_id"), markdown=True)
        logger.info(
            f"Transaction {amount} {trx.get("trx_id")} completed {link}",
            extra={"notification": True, "extra": trx},
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
            f"Market Trade error: {e}",
            extra={"notification": False, "quote": quote, "error": e},
        )
        raise e


def check_order_book(
    amount: Amount,
    hive: Hive = None,
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

    if amount.amount < 0:
        raise ValueError("Amount must be positive")

    if not hive:
        hive = get_hive_client()

    base_asset = amount.symbol  # The Asset I'm selling
    quote_asset = "HBD" if base_asset == "HIVE" else "HIVE"

    if use_cache and ORDER_BOOK_CACHE:
        order_book = ORDER_BOOK_CACHE
    else:
        # Note: The order book is the same and always uses HIVE as the base asset
        market = Market(base=base_asset, quote=quote_asset, blockchain_instance=hive)
        order_book = market.orderbook(limit=order_book_limit)
        ORDER_BOOK_CACHE = order_book

    # Selling HIVE, so we want the highest bid
    if base_asset == "HIVE":
        orders_bids = order_book["bids"]
        orders_bids.sort(key=lambda x: x["price"], reverse=True)
    else:  # Selling HBD so we want the lowest ask
        orders_bids = order_book["asks"]
        orders_bids.sort(key=lambda x: x["price"], reverse=False)

    cumulative_volume = 0.0
    total_received = 0.0
    final_price: Price = None

    # This naieve calculation doesn't take into account the slightly lower
    # price of the next order in the order book, it quotes the price of the
    # last order that was needed to reach the desired amount
    for order in orders_bids:
        price = float(order["price"])
        quote = order["quote"]
        base = order["base"]

        if base_asset == "HIVE":
            volume = float(quote)
            price = Price(price, base_asset, quote_asset)
            total_received += volume
        else:  # Selling HBD
            volume = float(base)
            price = Price(price, quote_asset, base_asset)
            total_received += volume

        cumulative_volume += volume
        if cumulative_volume >= amount.amount:
            final_price = price
            break

    if final_price is None:
        raise ValueError("Not enough volume in the order book")

    logger.debug(f"Best price for {amount} is {final_price}")
    # if final_price.market["base"]["symbol"] != amount.symbol:
    #     final_price.invert()

    if base_asset == "HIVE":
        min_amt = amount.amount * float(final_price["price"])
    else:
        min_amt = amount.amount / float(final_price["price"])

    minimum_amount = Amount(min_amt, quote_asset)

    logger.debug(f"Minimum amount: {minimum_amount}")

    hive_quote = HiveQuote(amount, final_price, minimum_amount)
    return hive_quote


if __name__ == "__main__":
    # trade = Amount("1_000 HBD")
    # sell_HBD_quote = check_order_book(trade, use_cache=True)

    # trade = Amount(sell_HBD_quote.minimum_amount)
    # sell_HIVE_quote = check_order_book(trade, use_cache=True)
    # assert sell_HBD_quote.price > sell_HIVE_quote.price
    try:
        trade = Amount("0.1 HBD")
        trx = market_trade(HiveAccountConfig(name="v4vapp-test"), trade)
        logger.info(f"trx: {trx}")
    except Exception as e:
        logger.info(e)

    try:
        trade = Amount("0.1 HIVE")
        trx2 = market_trade(HiveAccountConfig(name="v4vapp-test"), trade)
        logger.info(f"trx2: {trx2}")
    except Exception as e:
        logger.info(e)
