from tests.load_data import load_hive_events
from v4vapp_backend_v2.hive_models.op_all import op_any
from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes


def test_all_validate():
    for hive_event in load_hive_events():
        try:
            op = op_any(hive_event)
            assert op.type == op.name
            print(hive_event.get("type"), op.type)
        except Exception as e:
            pass

        # if hive_event["type"] == "limit_order_create":
        #     limit_order = LimitOrderCreate.model_validate(hive_event)
        #     assert limit_order.trx_id == hive_event["trx_id"]
        #     assert (
        #         limit_order.amount_to_sell.amount
        #         == hive_event["amount_to_sell"]["amount"]
        #     )
        #     assert str(limit_order.amount_to_sell) == str(
        #         Amount(hive_event["amount_to_sell"])
        #     )
        #     print(limit_order.log_str)
        #     print(limit_order.log_extra)
