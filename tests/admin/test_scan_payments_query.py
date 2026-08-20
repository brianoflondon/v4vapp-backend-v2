"""Admin payment replay scan lists all succeeded unprocessed payments."""

from v4vapp_backend_v2.admin.routers.replay_deposit import UNPROCESSED_SUCCEEDED_PAYMENT_QUERY


def test_payment_scan_query_is_succeeded_without_process_time() -> None:
    assert UNPROCESSED_SUCCEEDED_PAYMENT_QUERY == {
        "status": "SUCCEEDED",
        "process_time": {"$exists": False},
    }
    assert "invoice_description" not in UNPROCESSED_SUCCEEDED_PAYMENT_QUERY
    assert "cust_id" not in UNPROCESSED_SUCCEEDED_PAYMENT_QUERY
    assert "$or" not in UNPROCESSED_SUCCEEDED_PAYMENT_QUERY
    assert "$regex" not in UNPROCESSED_SUCCEEDED_PAYMENT_QUERY
