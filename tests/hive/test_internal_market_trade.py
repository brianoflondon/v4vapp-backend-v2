import json
import pickle
from unittest.mock import patch

import pytest  # type: ignore
from beem.amount import Amount

from v4vapp_backend_v2.config.setup import HiveAccountConfig
from v4vapp_backend_v2.hive.hive_extras import get_hive_client
from v4vapp_backend_v2.hive.internal_market_trade import (
    ORDER_BOOK_CACHE,
    check_order_book,
)


def test_check_order_book():
    originaL_trade = Amount("1_000 HBD")
    sell_HBD_quote = check_order_book(
        originaL_trade, use_cache=True, order_book_limit=50
    )

    trade = Amount(sell_HBD_quote.minimum_amount)
    sell_HIVE_quote = check_order_book(trade, use_cache=True)

    assert sell_HBD_quote.price > sell_HIVE_quote.price
    assert sell_HIVE_quote.minimum_amount.amount < originaL_trade.amount


def test_check_order_book_no_cache():
    originaL_trade = Amount("1_000 HBD", order_book_limit=50)
    sell_HBD_quote = check_order_book(originaL_trade, use_cache=False)

    trade = Amount(sell_HBD_quote.minimum_amount)
    sell_HIVE_quote = check_order_book(trade, use_cache=False)

    assert sell_HBD_quote.price > sell_HIVE_quote.price
    assert sell_HIVE_quote.minimum_amount.amount < originaL_trade.amount


def test_check_order_book_no_liquidity():
    trade = Amount("1_000_000_000 HBD")
    with pytest.raises(ValueError):
        check_order_book(trade, use_cache=True)
