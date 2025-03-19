import pytest  # type: ignore
from beem.amount import Amount

from v4vapp_backend_v2.config.setup import HiveAccountConfig
from v4vapp_backend_v2.hive.hive_extras import get_hive_client
from v4vapp_backend_v2.hive.internal_market_trade import (
    check_order_book_to_sell,
    internal_market_sell,
)


def test_check_order_book_to_sell():
    base_amount = Amount("10 HBD")
    hive_client = get_hive_client()
    first_result = check_order_book_to_sell(base_amount, hive_client)
    assert first_result is not None
    second_result = check_order_book_to_sell(first_result, hive_client)
    assert second_result is not None
    delta = second_result.amount - base_amount.amount
    assert delta is not None


def test_check_order_book_to_sell_too_much():
    base_amount = Amount("1_000_000_000 HBD")
    hive_client = get_hive_client()
    with pytest.raises(ValueError) as ex:
        first_result = check_order_book_to_sell(base_amount, hive_client)
        assert "Not enough liquidity" in str(ex.value)


def test_internal_market_sell():
    hive_acc = HiveAccountConfig(name="v4vapp-test")
    amount = Amount("1 HIVE")
    internal_market_sell(hive_acc, amount)
    assert True
    hive_acc = HiveAccountConfig(name="v4vapp-test")
    amount = Amount("1 HBD")
    internal_market_sell(hive_acc, amount)
    assert True
