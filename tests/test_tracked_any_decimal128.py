from decimal import Decimal

try:
    from bson.decimal128 import Decimal128
except Exception:  # pragma: no cover - bson may not be present
    Decimal128 = None

from v4vapp_backend_v2.actions.tracked_any import tracked_any_filter


def test_tracked_any_filter_with_decimal128():
    if Decimal128 is None:
        return

    sample = {
        "htlc_id": 5244,
        "message_type": "FORWARD",
        "amount": Decimal128("29886.435"),
        "fee": Decimal128("14.375"),
        "fee_percent": Decimal128("0.048"),
        "fee_ppm": 481,
        "htlc_event_dict": {
            "incoming_channel_id": "993059111034945537",
            "incoming_htlc_id": "5244",
            "timestamp_ns": Decimal128("1766678812771294166"),
            "final_htlc_event": {"settled": True, "offchain": True},
        },
        "notification": True,
        "silent": False,
        "timestamp": {"$date": "2025-12-25T16:06:52.904Z"},
    }

    val = tracked_any_filter(sample)
    # Expect it to be a TrackedForwardEvent-like object with decimal-converted fields
    assert hasattr(val, "amount")
    assert isinstance(val.amount, Decimal)
    assert isinstance(val.fee_percent, Decimal)
    assert val.htlc_event_dict.timestamp_ns == Decimal("1766678812771294166")


def test_tracked_any_filter_with_int_amount_and_decimal_fee():
    sample = {
        "htlc_id": 895,
        "message_type": "FORWARD",
        "amount": 999900,
        "fee": {"$numberDecimal": "115.988"},
        "fee_percent": {"$numberDecimal": "0.012"},
        "fee_ppm": 116,
        "htlc_event_dict": {
            "incoming_channel_id": "1008803018096443396",
            "incoming_htlc_id": "895",
            "timestamp_ns": {"$numberLong": "1766757287403535160"},
            "final_htlc_event": {"settled": True, "offchain": True},
        },
        "notification": True,
        "silent": False,
        "timestamp": {"$date": "2025-12-26T13:54:47.247Z"},
        "process_time": 134.328696098004,
        "included_on_ledger": True,
        "ledger_entry_id": "forward-895-1766757287403535160_r_fee",
    }

    val = tracked_any_filter(sample)
    assert isinstance(val.amount, Decimal)
    assert val.amount == Decimal("999900")
    assert isinstance(val.fee, Decimal)
    assert val.fee == Decimal("115.988")
    assert val.included_on_ledger is True
    assert val.ledger_entry_id == "forward-895-1766757287403535160_r_fee"
