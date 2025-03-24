from dataclasses import dataclass
from pprint import pprint
from typing import Any, Dict

from beem import Hive  # type: ignore
from beem.account import Account  # type: ignore
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
icon = "📈"


@dataclass
class HiveQuote:
    trade: Amount
    price: Price
    minimum_amount: Amount


def account_trade(hive_acc: HiveAccountConfig, set_amount_to: Amount) -> dict:
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

    if isinstance(hive_acc, str):
        hive_acc = hive_configs.hive_accs.get(hive_acc)
    elif isinstance(hive_acc, HiveAccountConfig):
        if hive_acc.name and not hive_acc.keys:
            hive_acc = hive_configs.hive_accs.get(hive_acc.name, HiveAccountConfig())

    if not hive_acc or not hive_acc.active_key:
        logger.error(f"{icon} " f"Account {hive_acc.name} not found in config")
        raise ValueError(f"Account {hive_acc.name} Active Keys not found in config")

    hive = get_hive_client(keys=hive_acc.keys, nobroadcast=True)
    account = Account(hive_acc.name, blockchain_instance=hive)
    pprint(account.available_balances, indent=2)
    balance = {}
    balance["HIVE"] = account.available_balances[0]
    balance["HBD"] = account.available_balances[1]
    delta = balance[set_amount_to.symbol] - set_amount_to
    if delta.amount > 0:
        logger.info(
            f"{icon} "
            f"Account {hive_acc.name} has balance: {balance[set_amount_to.symbol]} "
            f"and will trade {delta} to reach {set_amount_to}"
        )
        trx = market_trade(hive_acc, delta)
        return trx
    else:
        logger.info(
            f"{icon} "
            f"Account {hive_acc.name} balance is {delta} below : {set_amount_to}"
        )
    return {}


def market_trade(
    hive_acc: HiveAccountConfig,
    amount: Amount,
    use_cache: bool = False,
    killfill: bool = False,
    nobroadcast: bool = False,
) -> dict:
    try:
        hive_configs = InternalConfig().config.hive
        if hive_acc.name in hive_configs.hive_accs:
            hive_acc = hive_configs.hive_accs[hive_acc.name]

        hive = get_hive_client(keys=hive_acc.keys, nobroadcast=nobroadcast)
        quote = check_order_book(amount, hive, use_cache=use_cache)
        price_float = float(quote.price["price"])

        if amount.symbol == "HIVE":
            market = Market("HIVE:HBD", blockchain_instance=hive)
            trx = market.sell(
                price=price_float,
                amount=str(amount),
                account=hive_acc.name,
                killfill=killfill,
            )

        else:
            market = Market("HBD:HIVE", blockchain_instance=hive)
            trx = market.sell(
                price=1 / price_float,
                amount=amount,
                account=hive_acc.name,
                killfill=killfill,
            )
        link = get_hive_block_explorer_link(trx.get("trx_id"), markdown=True)
        logger.info(
            f"{icon} " f"{hive_acc.name} sold {amount} {link}",
            extra={"notification": True, "trx": trx},
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
            f"{icon} " f"Market Trade error: {e}",
            extra={"notification": False, "quote": quote, "error": e},
        )
        raise e
    return {}


# def market_trade_broken(
#     hive_acc: HiveAccountConfig,
#     amount: Amount,
#     price: Price | None = None,
#     use_cache: bool = False,
#     killfill: bool = False,
#     nobroadcast: bool = False,
#     expiration: int = 3600,
# ) -> dict:
#     hive_configs = InternalConfig().config.hive
#     if hive_acc.name in hive_configs.hive_accs:
#         hive_acc = hive_configs.hive_accs[hive_acc.name]
#     hive = get_hive_client(keys=hive_acc.keys, nobroadcast=nobroadcast)
#     quote = check_order_book(amount, hive, use_cache=use_cache)

#     base_asset = amount.symbol
#     quote_asset = "HBD" if base_asset == "HIVE" else "HIVE"
#     market_symbol = f"{base_asset}:{quote_asset}"
#     market = Market(market_symbol, blockchain_instance=hive)
#     amount_str = str(amount)
#     price_float = float(quote.price["price"])
#     logger.info(
#         f"{icon} " f"Converting {amount} to {quote.minimum_amount} at {quote.price}",
#         extra={"notification": True, "quote": quote},
#     )
#     try:
#         trx = market.sell(
#             price=price_float,
#             amount=amount_str,
#             account=hive_acc.name,
#             killfill=killfill,
#             expiration=expiration,
#         )
#         link = get_hive_block_explorer_link(trx.get("trx_id"), markdown=True)
#         logger.info(
#             f"{icon} " f"Transaction {amount} {link} {trx.get("trx_id")} completed",
#             extra={"notification": True, "extra": trx},
#         )
#         return trx
#     except UnhandledRPCError as e:
#         logger.warning(
#             f"Market Trade error: {e}",
#             extra={"notification": True, "quote": quote, "error": e},
#         )
#         raise e
#     except Exception as e:
#         logger.warning(
#             f"{icon} " f"Market Trade error: {e}",
#             extra={"notification": False, "quote": quote, "error": e},
#         )
#         raise e


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

    logger.debug(f"{icon} " f"Best price for {amount} is {final_price}")

    if base_asset == "HIVE":
        min_amt = amount.amount * float(final_price["price"])
    else:
        min_amt = amount.amount / float(final_price["price"])

    minimum_amount = Amount(min_amt, quote_asset)

    logger.debug(f"{icon} Minimum amount: {minimum_amount}")

    hive_quote = HiveQuote(amount, final_price, minimum_amount)
    return hive_quote


if __name__ == "__main__":

    # try:
    #     account_trade(HiveAccountConfig(name="v4vapp-test"), Amount("2.5 HBD"))
    #     account_trade(HiveAccountConfig(name="v4vapp-test"), Amount("14 HIVE"))
    # except Exception as e:
    #     logger.info(f"{icon} {e}")

    try:
        trade = Amount("0.2 HBD")
        trx = market_trade(HiveAccountConfig(name="v4vapp-test"), trade)
        logger.info(f"{icon} " f"trx: {trx}", extra={"notification": False})
    except Exception as e:
        logger.info(f"{icon} {e}")

    try:
        trade = Amount("1 HIVE")
        trx2 = market_trade(HiveAccountConfig(name="v4vapp-test"), trade)
        logger.info(f"{icon} " f"trx2: {trx2}", extra={"notification": False})
    except Exception as e:
        logger.info(f"{icon} {e}")
