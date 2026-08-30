from bson import ObjectId

from v4vapp_backend_v2.actions.tracked_any import get_tracked_any_type, tracked_any_filter
from v4vapp_backend_v2.dash.models.tracked import DashInvoiceEvent


def test_get_tracked_any_type_settled_dash_invoice():
    invoice_id = ObjectId()
    value = {
        "_id": invoice_id,
        "address": "yDashAddress111",
        "duffs_quoted": 80_000_000,
        "state": "SETTLED",
        "external_id": "ext-1",
    }
    assert get_tracked_any_type(value) == "dash_invoice"


def test_get_tracked_any_type_open_dash_invoice_rejected():
    value = {
        "address": "yDashAddress111",
        "duffs_quoted": 80_000_000,
        "state": "OPEN",
    }
    try:
        get_tracked_any_type(value)
        raise AssertionError("expected ValueError")
    except ValueError as e:
        assert "not settled" in str(e)


def test_tracked_any_filter_builds_dash_event():
    invoice_id = ObjectId()
    tracked = {
        "_id": invoice_id,
        "address": "yDashAddress111",
        "duffs_quoted": 80_000_000,
        "duffs_received": 80_000_000,
        "state": "SETTLED",
        "external_id": "ext-1",
        "lightning_invoice": "lnbc250u1ptestinvoice",
        "cust_id": "alice",
        "sats_requested": 25000,
        "sats_credited": 25000,
        "network": "testnet",
    }
    event = tracked_any_filter(tracked)
    assert isinstance(event, DashInvoiceEvent)
    assert event.invoice_id == str(invoice_id)
    assert event.op_type == "dash_invoice"
    assert event.lightning_invoice == "lnbc250u1ptestinvoice"
    assert event.short_id == str(invoice_id)[:8]
    assert event.group_id_p == str(invoice_id)
