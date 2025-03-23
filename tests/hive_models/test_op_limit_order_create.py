from pprint import pprint
from typing import List

from beem.amount import Amount  # type: ignore

from tests.load_data import load_hive_events
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes


def test_model_validate_limit_order_create():
    for hive_event in load_hive_events(OpTypes.LIMIT_ORDER_CREATE):
        if hive_event["type"] == "limit_order_create":
            limit_order = LimitOrderCreate.model_validate(hive_event)
            assert limit_order.trx_id == hive_event["trx_id"]
            assert (
                limit_order.amount_to_sell.amount
                == hive_event["amount_to_sell"]["amount"]
            )
            assert str(limit_order.amount_to_sell) == str(
                Amount(hive_event["amount_to_sell"])
            )
            print(limit_order.log_str)
            print(limit_order.log_extra)

    len(limit_order.open_orderids) == 28
    limit_order.expire_orders()
    len(limit_order.open_orderids) == 0


def test_model_validate_limit_order_create_and_fill_orders():
    filled_orders: List[FillOrder] = []
    for hive_event in load_hive_events(OpTypes.LIMIT_ORDER_CREATE):
        if hive_event["type"] == "limit_order_create":
            limit_order_create = LimitOrderCreate.model_validate(hive_event)
        if hive_event["type"] == "fill_order":
            filled_orders.append(FillOrder.model_validate(hive_event))

    print(f"Number of filled orders: {len(filled_orders)}")
    for fill_order in filled_orders:
        open_order = LimitOrderCreate.open_orderids.get(fill_order.open_orderid, None)
        if open_order is not None:
            print(f"{open_order.log_str} {fill_order.log_str}")
            outstanding_amount = (
                open_order.amount_to_sell.amount_decimal
                - fill_order.open_pays.amount_decimal
            )
            if outstanding_amount > 0:
                open_order.amount_remaining = (
                    open_order.amount_to_sell.beam - fill_order.open_pays.beam
                )
            else:
                LimitOrderCreate.open_orderids.pop(fill_order.open_orderid)
                print(f"Order {open_order.orderid} has been filled.")
    print(f"Number of open orders: {len(LimitOrderCreate.open_orderids)}")
    for open_order in LimitOrderCreate.open_orderids.values():
        if open_order.amount_remaining is not None:
            print(f"{str(open_order.amount_remaining):>9} {open_order.log_str}")
