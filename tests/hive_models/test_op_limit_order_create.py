from typing import List

from nectar.amount import Amount

from tests.load_data import load_hive_events
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes


def test_model_validate_limit_order_create():
    LimitOrderCreate.watch_users = ["droida", "dtake"]
    for hive_event in load_hive_events(OpTypes.LIMIT_ORDER_CREATE):
        if hive_event["type"] == "limit_order_create":
            limit_order = LimitOrderCreate.model_validate(hive_event)
            assert limit_order.trx_id == hive_event["trx_id"]
            assert limit_order.amount_to_sell.amount == hive_event["amount_to_sell"]["amount"]
            assert str(limit_order.amount_to_sell) == str(Amount(hive_event["amount_to_sell"]))
            print(limit_order.log_str)
            assert limit_order.log_extra
            assert (limit_order.owner in limit_order.watch_users) == limit_order.is_watched

    len(limit_order.open_order_ids) == 28
    limit_order.expire_orders()
    len(limit_order.open_order_ids) == 0


def test_model_validate_limit_order_create_and_fill_orders() -> None:
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
