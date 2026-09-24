"""Terminal payment snapshots must not be replaced by an in-flight write."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from lnd_monitor_v2 import (
    LndIndexFloors,
    db_store_payment,
    lnrpc_payment_status_name,
    should_skip_payment_backfill,
)
from v4vapp_backend_v2.models.payment_models import Payment, PaymentStatus


def _event(status: int, payment_index: int = 13046):
    return SimpleNamespace(status=status, payment_index=payment_index)


def test_lnrpc_status_name_reads_protobuf_enum():
    assert lnrpc_payment_status_name(_event(lnrpc.Payment.PaymentStatus.IN_FLIGHT)) == "IN_FLIGHT"
    assert lnrpc_payment_status_name(_event(lnrpc.Payment.PaymentStatus.SUCCEEDED)) == "SUCCEEDED"


def test_in_flight_save_refuses_terminal_rows_and_does_not_upsert():
    payment = Payment(payment_hash="abc", status=PaymentStatus.IN_FLIGHT)
    assert payment.mongo_save_upsert() is False
    assert payment.mongo_save_filter() == {
        "payment_hash": "abc",
        "status": {"$nin": ["FAILED", "SUCCEEDED"]},
    }


def test_terminal_save_upserts_on_payment_hash_only():
    payment = Payment(payment_hash="abc", status=PaymentStatus.SUCCEEDED)
    assert payment.mongo_save_upsert() is True
    assert payment.mongo_save_filter() == {"payment_hash": "abc"}
    failed = Payment(payment_hash="abc", status="FAILED")
    assert failed.mongo_save_upsert() is True


def test_backfill_skips_in_flight_source_and_replaces_stored_in_flight():
    in_flight = SimpleNamespace(status=PaymentStatus.IN_FLIGHT, payment_hash="abc")
    succeeded = SimpleNamespace(status=PaymentStatus.SUCCEEDED, payment_hash="abc")
    clobbered = {
        "status": "IN_FLIGHT",
        "route_str": "Unknown",
        "invoice_description": "Sending sats § parent",
    }
    finished = {
        "status": "SUCCEEDED",
        "route_str": "A -> B",
        "invoice_description": "Sending sats § parent",
    }
    assert should_skip_payment_backfill(in_flight, None) is True
    assert should_skip_payment_backfill(succeeded, clobbered) is False
    assert should_skip_payment_backfill(succeeded, finished) is True
    assert should_skip_payment_backfill(succeeded, None) is False


@pytest.mark.asyncio
async def test_db_store_payment_does_not_touch_an_in_flight_event(monkeypatch):
    monkeypatch.setattr("lnd_monitor_v2.get_lnd_index_floors", lambda *_a, **_k: LndIndexFloors())
    payment_cls = MagicMock()
    decode = AsyncMock()
    monkeypatch.setattr("lnd_monitor_v2.Payment", payment_cls)
    monkeypatch.setattr("lnd_monitor_v2.decode_payment_request_and_attach", decode)
    client = MagicMock()
    client.connection.name = "legion"
    client.icon = "⚡"

    await db_store_payment(_event(lnrpc.Payment.PaymentStatus.IN_FLIGHT), client)
    await db_store_payment(_event(lnrpc.Payment.PaymentStatus.INITIATED), client)

    payment_cls.assert_not_called()
    decode.assert_not_awaited()


@pytest.mark.asyncio
async def test_db_store_payment_stores_succeeded(monkeypatch):
    monkeypatch.setattr("lnd_monitor_v2.get_lnd_index_floors", lambda *_a, **_k: LndIndexFloors())
    stored = MagicMock()
    stored.node_name = ""
    stored.update_conv = AsyncMock()
    stored.save = AsyncMock(return_value=SimpleNamespace(raw_result={}))
    stored.route_str = "A -> B"
    stored.short_id = "5391c1949d"
    stored.log_extra = {}
    monkeypatch.setattr("lnd_monitor_v2.Payment", MagicMock(return_value=stored))
    decode = AsyncMock()
    alias = AsyncMock()
    monkeypatch.setattr("lnd_monitor_v2.decode_payment_request_and_attach", decode)
    monkeypatch.setattr("lnd_monitor_v2.update_payment_route_with_alias", alias)
    client = MagicMock()
    client.connection.name = "legion"
    client.icon = "⚡"

    await db_store_payment(_event(lnrpc.Payment.PaymentStatus.SUCCEEDED), client)

    decode.assert_awaited()
    alias.assert_awaited()
    stored.save.assert_awaited()
