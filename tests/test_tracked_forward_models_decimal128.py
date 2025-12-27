from decimal import Decimal

try:
    from bson.decimal128 import Decimal128
except Exception:  # pragma: no cover - bson may not be present
    Decimal128 = None

from v4vapp_backend_v2.models.tracked_forward_models import TrackedForwardEvent


def test_tracked_forward_accepts_decimal128():
    if Decimal128 is None:
        return

    sample = {
        "_id": {"$oid": "694e87582b2170f660fac436"},
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

    m = TrackedForwardEvent.model_validate(sample)
    assert isinstance(m.amount, Decimal)
    assert isinstance(m.fee, Decimal)
    assert isinstance(m.fee_percent, Decimal)
    assert m.htlc_event_dict.timestamp_ns == Decimal("1766678812771294166")
