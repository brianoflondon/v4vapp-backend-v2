import pytest
from nectar.amount import Amount

from v4vapp_backend_v2.hive.internal_market_trade import check_order_book


def test_check_order_book():
    originaL_trade = Amount("500 HBD")
    sell_HBD_quote = check_order_book(
        originaL_trade, use_cache=True, order_book_limit=100
    )

    trade = Amount(sell_HBD_quote.minimum_amount)
    sell_HIVE_quote = check_order_book(trade, use_cache=True)

    assert sell_HBD_quote.price > sell_HIVE_quote.price
    assert sell_HIVE_quote.minimum_amount.amount < originaL_trade.amount


def test_check_order_book_no_cache():
    originaL_trade = Amount("500 HBD", order_book_limit=100)
    sell_HBD_quote = check_order_book(originaL_trade, use_cache=False)

    trade = Amount(sell_HBD_quote.minimum_amount)
    sell_HIVE_quote = check_order_book(trade, use_cache=False)

    assert sell_HBD_quote.price > sell_HIVE_quote.price
    assert sell_HIVE_quote.minimum_amount.amount < originaL_trade.amount


def test_check_order_book_no_liquidity():
    trade = Amount("1_000_000_000 HBD")
    with pytest.raises(ValueError):
        check_order_book(trade, use_cache=True)
