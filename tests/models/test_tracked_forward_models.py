from datetime import datetime, timezone
from decimal import Decimal

from v4vapp_backend_v2.models.tracked_forward_models import TrackedForwardEvent

SAMPLE_DOC_1 = {
    "_id": {"$oid": "694d099b1ef642d77b19c8c0"},
    "htlc_id": 5183,
    "message_type": "FORWARD",
    "message:": "💰 Attempted 60,985 ACINQ → fortuna-custody-stroom ❌ (5183)",
    "from_channel": "ACINQ",
    "to_channel": "fortuna-custody-stroom",
    "amount": {"$numberDecimal": "60985.206"},
    "fee": {"$numberDecimal": "29.333"},
    "fee_percent": {"$numberDecimal": "0.048"},
    "fee_ppm": 481,
    "htlc_event_dict": {
        "incoming_channel_id": "993059111034945537",
        "incoming_htlc_id": "5183",
        "timestamp_ns": "1766656411792420910",
        "final_htlc_event": {"offchain": True},
    },
    "notification": False,
    "silent": True,
    "timestamp": {"$date": "2025-12-25T09:53:31.557Z"},
}

SAMPLE_DOC_2 = {
    "_id": {"$oid": "694d095e1ef642d77b19c8bf"},
    "htlc_id": 5182,
    "message_type": "FORWARD",
    "message:": "💰 Attempted 60,670 ACINQ → fortuna-custody-stroom ❌ (5182)",
    "from_channel": "ACINQ",
    "to_channel": "fortuna-custody-stroom",
    "amount": {"$numberDecimal": "60670.470"},
    "fee": {"$numberDecimal": "29.182"},
    "fee_percent": {"$numberDecimal": "0.048"},
    "fee_ppm": 481,
    "htlc_event_dict": {
        "incoming_channel_id": "993059111034945537",
        "incoming_htlc_id": "5182",
        "timestamp_ns": "1766656349845728201",
        "final_htlc_event": {"offchain": True},
    },
    "notification": False,
    "silent": True,
    "timestamp": {"$date": "2025-12-25T09:52:30.778Z"},
}

SAMPLE_DOC_3 = {
    "_id": {"$oid": "694e93a7e98820bbc5da4ea8"},
    "htlc_id": 895,
    "group_id": "forward-895-1766757287403535160",
    "message_type": "FORWARD",
    "message": "💰 Forwarded 999,900 fortuna-custody-stroom → V4VAPP Hive GoPodcasting! ✅ Earned 115.988 0.01% 116 ppm (895)",
    "from_channel": "fortuna-custody-stroom",
    "to_channel": "V4VAPP Hive GoPodcasting!",
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
    "short_id": "895",
    "timestamp": {"$date": "2025-12-26T13:54:47.247Z"},
    "process_time": 134.328696098004,
    "included_on_ledger": True,
    "ledger_entry_id": "forward-895-1766757287403535160_r_fee",
}


def test_parse_sample_doc_1():
    m = TrackedForwardEvent.model_validate(SAMPLE_DOC_1)

    assert m.htlc_id == 5183
    assert "Attempted" in (m.message or "")
    assert isinstance(m.amount, Decimal)
    assert m.amount == Decimal("60985.206")
    assert m.fee == Decimal("29.333")
    assert isinstance(m.timestamp, datetime)
    assert m.timestamp.tzinfo is not None and m.timestamp.tzinfo.utcoffset(
        m.timestamp
    ) == timezone.utc.utcoffset(m.timestamp)
    assert m.htlc_event_dict is not None
    assert m.htlc_event_dict.final_htlc_event is not None
    assert m.htlc_event_dict.final_htlc_event.offchain is True


def test_parse_sample_doc_2():
    m = TrackedForwardEvent.model_validate(SAMPLE_DOC_2)

    assert m.htlc_id == 5182
    assert m.amount == Decimal("60670.470")
    assert m.fee == Decimal("29.182")
    assert m.htlc_event_dict.timestamp_ns == Decimal("1766656349845728201")
    assert m.silent is True


def test_parse_sample_doc_3():
    m = TrackedForwardEvent.model_validate(SAMPLE_DOC_3)

    assert m.htlc_id == 895
    # integer amount should be accepted and converted
    assert m.amount == Decimal("999900")
    assert m.fee == Decimal("115.988")
    assert m.fee_percent == Decimal("0.012")
    assert m.included_on_ledger is True
    assert m.ledger_entry_id == "forward-895-1766757287403535160_r_fee"
    assert isinstance(m.process_time, float)
    assert m.htlc_event_dict.timestamp_ns == Decimal("1766757287403535160")
