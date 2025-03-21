from typing import Any, Tuple

from beem.amount import Amount  # Type: ignore
from beem.market import Market  # Type: ignore
from beem.price import Price  # type: ignore
from beem.transactionbuilder import TransactionBuilder  # type: ignore
from beembase.operations import Limit_order_create, Limit_order_create2

from v4vapp_backend_v2.config.setup import HiveAccountConfig, InternalConfig, logger
from v4vapp_backend_v2.hive.hive_extras import get_hive_client


def internal_market_sell(
    hive_acc: HiveAccountConfig, amount: Amount, price: Price | None = None
) -> Any:

    hive_configs = InternalConfig().config.hive
    if hive_acc.name in hive_configs.hive_accs:
        hive_acc = hive_configs.hive_accs[hive_acc.name]
    hive = get_hive_client(keys=hive_acc.keys, nobroadcast=False)

    def working():
        market = Market("HIVE:HBD", blockchain_instance=hive)
        try:
            trx = market.sell(
                price=0.2,
                amount="1 HIVE",
                account=hive_acc.name,
            )
        except Exception as e:
            logger.warning(str(e), extra={"notification": False})
            return

        logger.info(f"trx: {trx}")

        market = Market("HBD:HIVE", blockchain_instance=hive)
        try:
            trx = market.sell(
                price=(0.24),
                amount="1 HBD",
                account=hive_acc.name,
            )
        except Exception as e:
            logger.warning(str(e), extra={"notification": False})
            return

        logger.info(f"trx: {trx}")

        return trx

    if not price:
        try:
            min_to_receive, price = check_order_book_to_sell(amount, hive_client=hive)
        except ValueError as e:
            logger.warning(
                f"Failed to check order book: {e}", extra={"notification": False}
            )
            raise e
    logger.info(f"Converting {amount} to {min_to_receive} at {price}")
    sell_asset = amount.symbol
    receive_asset = "HBD" if sell_asset == "HIVE" else "HIVE"
    market = Market(f"{sell_asset}:{receive_asset}", blockchain_instance=hive)
    try:
        trx = market.sell(
            price=float(price),
            amount=amount,
            account=hive_acc.name,
            # killfill=True,
            # returnOrderId="head",
        )
        logger.info(
            f"Transaction broadcasted: {trx}", extra={"notification": False, "trx": trx}
        )
    except Exception as e:
        logger.warning(str(e), extra={"notification": False})
        return


CACHE_ORDER_BOOK = []


def check_order_book_to_sell(
    amount: Amount,
    hive_client=None,
    use_cache: bool = True,
    simulate_order_book_depletion: bool = False,
) -> Tuple[Amount, Price]:
    global CACHE_ORDER_BOOK
    if amount.amount <= 0:
        logger.info(f"Amount to sell is 0, skipping.")
        raise ValueError("Amount to sell is 0, skipping.")

    sell_asset = amount.symbol
    target_amount = float(amount.amount)

    logger.info(f"Starting to check order book for selling {amount}")

    if hive_client is None:
        hive_client = get_hive_client()

    if not use_cache:
        CACHE_ORDER_BOOK = []

    if not CACHE_ORDER_BOOK:
        market = Market(blockchain_instance=hive_client)
        ticker = market.ticker()
        logger.info(
            f"Current spread - Highest bid: {ticker['highest_bid']} | Lowest ask: {ticker['lowest_ask']}"
        )
        order_book = market.orderbook(limit=500)  # Use get_order_book() for consistency
        CACHE_ORDER_BOOK = order_book
    else:
        order_book = CACHE_ORDER_BOOK

    orders_bids = order_book["bids"]
    orders_asks = order_book["asks"]

    if sell_asset == "HBD":
        orders = orders_bids
        price_label = "HBD/HIVE"
        order_type = "bids"
        orders.sort(
            key=lambda x: x["price"], reverse=True
        )  # Highest HBD/HIVE (best for seller)
    else:  # HIVE
        orders = orders_asks
        price_label = "HBD/HIVE"
        order_type = "asks"
        orders.sort(
            key=lambda x: x["price"], reverse=False
        )  # Lowest HBD/HIVE (best for seller)

    cumulative_volume = 0.0
    total_received = 0.0
    final_price = None

    for order in orders:
        price = float(order["price"])  # HBD/HIVE
        quote = order["quote"]  # HIVE
        base = order["base"]  # HBD

        if sell_asset == "HBD":
            volume = float(base.amount)  # HBD from bids
            received_per_unit = 1 / price  # HIVE/HBD
            received_asset = "HIVE"
        else:  # HIVE
            volume = float(quote.amount)  # HIVE from asks
            received_per_unit = price  # HBD/HIVE
            received_asset = "HBD"

        remaining_to_sell = target_amount - cumulative_volume
        volume_used = min(volume, remaining_to_sell)
        received_from_order = volume_used * received_per_unit

        cumulative_volume += volume_used
        total_received += received_from_order

        logger.info(
            f"Price: {price:.4f} {price_label}, Quote: {quote.amount:.3f} HIVE, "
            f"Base: {base.amount:.3f} HBD, Used: {volume_used:.3f} {sell_asset}, "
            f"Cumulative: {cumulative_volume:.3f}"
        )

        if cumulative_volume >= target_amount:
            final_price = price
            break

    # After simulating the trade, remove the consumed orders from the order book
    if simulate_order_book_depletion:
        updated_order_book = order_book.copy()
        if sell_asset == "HBD":
            # Remove or adjust consumed bids
            for i, order in enumerate(updated_order_book["bids"]):
                if i >= len(orders):  # Only process the orders we used
                    break
                if order["base"].amount <= cumulative_volume:
                    # This order was completely consumed
                    updated_order_book["bids"].pop(i)
                else:
                    # This order was partially consumed
                    updated_order_book["bids"][i]["base"] = Amount(
                        f"{order['base'].amount - volume_used} HBD"
                    )
        else:
            for i, order in enumerate(updated_order_book["asks"]):
                if i >= len(orders):
                    break
                if order["quote"].amount <= cumulative_volume:  # Completely consumed
                    updated_order_book["asks"].pop(i)
                else:  # Partially consumed
                    updated_order_book["asks"][i]["quote"] = Amount(
                        f"{order['quote'].amount - volume_used} HIVE"
                    )
        CACHE_ORDER_BOOK = updated_order_book

    if final_price is not None:
        logger.info(
            f"To sell {target_amount:.3f} {sell_asset}, set price at or "
            f"{'below' if sell_asset == 'HBD' else 'above'} {final_price:.4f} {price_label}"
        )
        logger.info(
            f"You'll receive approximately {total_received:.3f} {received_asset}"
        )
        ans_price = Price(f"{final_price:.6f} {price_label}")
        ans_amount = Amount(f"{total_received:.6f} {received_asset}")
        return ans_amount, ans_price
    else:
        logger.info(
            f"Not enough liquidity to sell {target_amount:.3f} {sell_asset} with current {order_type}."
        )
        raise ValueError(
            f"Not enough liquidity to sell {target_amount:.3f} {sell_asset} with current {order_type}."
        )


if __name__ == "__main__":
    # Single client instance
    hive_acc = HiveAccountConfig(name="v4vapp-test")
    amount = Amount("5 HIVE")
    trx = internal_market_sell(hive_acc, amount)
    amount = Amount("0.25 HBD")
    trx = internal_market_sell(hive_acc, amount)

    # for asset in ["HBD", "HIVE"]:
    #     for value in range(200, 400, 200):

    hive_client = get_hive_client()  # Replace with get_hive_client()
    value = 10
    asset = "HBD"

    base_amount = Amount(f"{value} {asset}")
    first_result = check_order_book_to_sell(base_amount, hive_client)
    if first_result:
        second_result = check_order_book_to_sell(first_result, hive_client)
        if second_result:
            delta = second_result.amount - base_amount.amount
            logger.info(
                f"{base_amount} -> {first_result} -> {second_result} delta: "
                f"{delta:.3f} {asset} "
                f"percentage: {delta / base_amount.amount * 100:.2f}%"
            )
            logger.info(
                f"{base_amount.amount/first_result.amount:.3f} "
                f"{second_result.amount/first_result.amount}"
            )
        else:
            logger.info(f"Second check failed for {base_amount}")
    else:
        logger.info(f"First check failed for {base_amount}")

    # market = Market("HBD:HIVE", blockchain_instance=hive_client)
    # # order_book = market.orderbook(limit=25)
    # pprint(order_book, indent=2)

    # hive_acc = HiveAccountConfig(name="v4vapp.dev")
    # amount = Amount("10 HBD")
    # internal_market_hive_hbd_trade(hive_acc, amount)
    # amount = Amount("10 HBD")
    # internal_market_hive_hbd_trade(hive_acc, amount)
