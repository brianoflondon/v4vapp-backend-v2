from tests.load_data import load_hive_events
from v4vapp_backend_v2.hive_models.op_limit_order_cancelled import LimitOrderCancelled


def test_model_validate_limit_order_cancelled():
    found = False
    for hive_event in load_hive_events():
        if hive_event.get("type") == "limit_order_cancelled":
            op = LimitOrderCancelled.model_validate(hive_event)
            assert op.orderid == hive_event["orderid"]
            assert op.seller == hive_event["seller"]
            assert op.amount_back.amount == hive_event["amount_back"]["amount"]
            # basic string properties should include identifying information
            assert str(op.orderid) in op.log_str
            assert op.seller in op.notification_str
            found = True
            break
    assert found, "no limit_order_cancelled events were found in test data"


def test_op_any_and_query():
    """Ensure the discriminated union and query helpers support the new op type."""
    sample = {
        "type": "limit_order_cancelled",
        "amount_back": {"amount": "1.000", "nai": "@@000000021", "precision": 3},
        "orderid": 123,
        "seller": "foo",
        # include required base fields so op_any can validate the union
        "trx_id": "0000",
        "block_num": 1,
    }
    from v4vapp_backend_v2.hive_models.op_all import op_any, op_query

    op = op_any(sample)
    assert op.__class__.__name__ == "LimitOrderCancelled"
    # query should match case-insensitively
    q = op_query(["limit_order_cancelled"])
    assert q == {"type": {"$in": ["limit_order_cancelled"]}}
