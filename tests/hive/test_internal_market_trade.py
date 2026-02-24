import pytest
from nectar.amount import Amount
from nectar.market import Market

from v4vapp_backend_v2.config.setup import HiveAccountConfig
from v4vapp_backend_v2.hive.internal_market_trade import check_order_book, market_trade


def fake_orderbook(self, limit=None):
    # simple deterministic book: bids at 1.0 price, asks at 1.2 price
    return {
        "bids": [{"price": "1.0", "base": "100", "quote": "100"}],
        "asks": [{"price": "1.2", "base": "100", "quote": "120"}],
    }


@pytest.fixture(autouse=True)
def patch_market(monkeypatch):
    monkeypatch.setattr(Market, "orderbook", fake_orderbook)


def test_check_order_book_sell_and_buy():
    # positive amount should select bid side, price 1.0
    sell_quote = check_order_book(Amount("10 HIVE"), use_cache=False)
    assert float(sell_quote.price["price"]) == 1.0

    # negative amount should select ask side, price 1.2
    buy_quote = check_order_book(Amount("-10 HIVE"), use_cache=False)
    assert float(buy_quote.price["price"]) == 1.2

    # buying HBD (negative amount) should use the bid side (we're selling
    # HIVE) and therefore the lower price from our fake book
    buy_hbd_quote = check_order_book(Amount("-5 HBD"), use_cache=False)
    assert float(buy_hbd_quote.price["price"]) == 1.0

    # selling HBD (positive amount) should hit asks (buying HIVE with HBD)
    sell_hbd_quote = check_order_book(Amount("5 HBD"), use_cache=False)
    assert float(sell_hbd_quote.price["price"]) == 1.2

    # verify minimum amounts make sense
    assert sell_quote.minimum_amount.symbol == "HBD"
    assert buy_quote.minimum_amount.symbol == "HBD"
    assert buy_hbd_quote.minimum_amount.symbol == "HIVE"
    assert sell_hbd_quote.minimum_amount.symbol == "HIVE"


def test_check_order_book_no_cache():
    originaL_trade = Amount("10 HBD")
    sell_HBD_quote = check_order_book(originaL_trade, use_cache=False)

    trade = Amount(sell_HBD_quote.minimum_amount)
    sell_HIVE_quote = check_order_book(trade, use_cache=False)

    assert sell_HBD_quote.price > sell_HIVE_quote.price
    assert sell_HIVE_quote.minimum_amount.amount < originaL_trade.amount


def test_check_order_book_no_liquidity(monkeypatch):
    # override order book to empty so we raise
    def empty_book(self, limit=0, raw_data=False):
        return {"bids": [], "asks": []}

    monkeypatch.setattr(Market, "orderbook", empty_book, raising=False)

    trade = Amount("1_000_000_000 HBD")
    with pytest.raises(ValueError):
        check_order_book(trade, use_cache=True)


def test_market_trade_direction(monkeypatch):
    # patch check_order_book so we know which price will be returned
    class DummyQuote:
        def __init__(self, price):
            self.price = {"price": str(price)}
            self.minimum_amount = Amount("0 HBD")

    def fake_quote(amount, hive=None, use_cache=False, order_book_limit=500):
        # bid price 1.0 for sells, ask price 1.2 for buys
        p = 1.2 if amount.amount < 0 else 1.0
        return DummyQuote(p)

    monkeypatch.setattr(
        "v4vapp_backend_v2.hive.internal_market_trade.check_order_book",
        fake_quote,
    )

    calls = {}

    def fake_sell(self, price, amount, account, killfill=False):
        calls["sell"] = {"price": price, "amount": amount, "account": account}
        return {"trx": "sold"}

    def fake_buy(self, price, amount, account, killfill=False):
        calls["buy"] = {"price": price, "amount": amount, "account": account}
        return {"trx": "bought"}

    monkeypatch.setattr(Market, "sell", fake_sell)
    monkeypatch.setattr(Market, "buy", fake_buy)

    # positive amount sells – expect sell call with price 1.0
    result = market_trade(HiveAccountConfig(name="foo"), Amount("5 HIVE"))
    assert calls.get("sell") is not None
    assert calls["sell"]["price"] == 1.0
    assert result == {"trx": "sold"}

    # negative amount buys – price should have been reciprocal of 1.2
    result = market_trade(HiveAccountConfig(name="foo"), Amount("-3 HBD"))
    assert calls.get("buy") is not None
    assert calls["buy"]["price"] == pytest.approx(1 / 1.2)
    assert result == {"trx": "bought"}
