from v4vapp_backend_v2.actions.tracked_any import get_tracked_any_type
from v4vapp_backend_v2.hive_models.op_limit_order_cancelled import LimitOrderCancelled


def test_tracked_any_type_for_limit_order_cancelled():
    sample = {
        "type": "limit_order_cancelled",
        "amount_back": {"amount": "2.000", "nai": "@@000000013", "precision": 3},
        "orderid": 1772004924,
        "seller": "devser.v4vapp",
        "trx_id": "abc123",
        "block_num": 104128504,
    }
    assert get_tracked_any_type(sample) == "limit_order_cancelled"
    from v4vapp_backend_v2.actions.tracked_any import DiscriminatedTracked

    wrapped = DiscriminatedTracked.model_validate({"value": sample})
    obj = wrapped.value
    assert isinstance(obj, LimitOrderCancelled)
    # round-trip to ensure union stays stable
    assert obj.orderid == 1772004924
