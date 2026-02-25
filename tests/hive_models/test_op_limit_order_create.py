from typing import List

from nectar.amount import Amount

from tests.get_last_quote import last_quote
from tests.load_data import load_hive_events
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes


def test_model_validate_limit_order_create():
    TrackedBaseModel.last_quote = last_quote()
    LimitOrderCreate.watch_users = ["droida", "dtake"]
    # ensure clean Redis state before iterating
    LimitOrderCreate.clear_open_orders()
    for hive_event in load_hive_events(OpTypes.LIMIT_ORDER_CREATE):
        if hive_event["type"] == "limit_order_create":
            limit_order = LimitOrderCreate.model_validate(hive_event)
            assert limit_order.trx_id == hive_event["trx_id"]
            assert limit_order.amount_to_sell.amount == hive_event["amount_to_sell"]["amount"]
            assert str(limit_order.amount_to_sell) == str(Amount(hive_event["amount_to_sell"]))
            print(limit_order.log_str)
            assert limit_order.log_extra
            assert (limit_order.owner in limit_order.watch_users) == limit_order.is_watched

    assert limit_order
    # Redis-backed storage should contain the same count
    assert len(limit_order.open_order_ids) == 14
    limit_order.expire_orders()
    assert len(limit_order.open_order_ids) == 0


def test_model_validate_limit_order_create_and_fill_orders() -> None:
    TrackedBaseModel.last_quote = last_quote()
    print()
    filled_orders: List[FillOrder] = []
    for hive_event in load_hive_events(OpTypes.LIMIT_ORDER_CREATE):
        if hive_event["type"] == "limit_order_create":
            limit_order_create = LimitOrderCreate.model_validate(hive_event)
            assert (
                limit_order_create.amount_to_sell.amount == hive_event["amount_to_sell"]["amount"]
            )
        if hive_event["type"] == "fill_order":
            fill_order = FillOrder.model_validate(hive_event)
            print(fill_order.log_str)
            filled_orders.append(fill_order)

    print(f"Number of filled orders: {len(filled_orders)}")


def test_check_hive_open_orders(monkeypatch):
    """Orders in the local cache that are missing from Hive should be dropped."""
    # prepare environment with two orders
    TrackedBaseModel.last_quote = last_quote()
    LimitOrderCreate.watch_users = ["droida", "dtake"]
    LimitOrderCreate.clear_open_orders()

    events = load_hive_events(OpTypes.LIMIT_ORDER_CREATE)
    orders = []
    for hive_event in events:
        if hive_event["type"] == "limit_order_create":
            orders.append(LimitOrderCreate.model_validate(hive_event))
            if len(orders) == 2:
                break
    assert len(orders) == 2

    for o in orders:
        LimitOrderCreate.add_open_order(o)
    assert len(LimitOrderCreate.get_all_open_orders()) == 2

    # stub the Hive market call so only the first order is returned as live
    def fake_accountopenorders(self, account):
        return [{"orderid": orders[0].orderid}]

    monkeypatch.setattr("nectar.market.Market.accountopenorders", fake_accountopenorders)
    monkeypatch.setattr(InternalConfig, "server_id", "dummy", raising=False)

    LimitOrderCreate.check_hive_open_orders()
    remaining = LimitOrderCreate.get_all_open_orders()
    assert orders[0].orderid in remaining
    assert orders[1].orderid not in remaining

    # if Hive returns an empty list we treat it as an error/unknown state
    # (see implementation) and therefore we leave the cached orders alone.
    for o in orders:
        LimitOrderCreate.add_open_order(o)
    monkeypatch.setattr("nectar.market.Market.accountopenorders", lambda self, account: [])
    LimitOrderCreate.check_hive_open_orders()
    remaining2 = LimitOrderCreate.get_all_open_orders()
    assert len(remaining2) == 2
    assert orders[0].orderid in remaining2 and orders[1].orderid in remaining2
