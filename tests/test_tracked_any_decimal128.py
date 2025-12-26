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
