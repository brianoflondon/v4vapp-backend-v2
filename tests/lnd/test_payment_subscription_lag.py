"""Hung TrackPayments / SubscribeInvoices detection: LND tip ahead of Mongo."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from lnd_monitor_v2 import (
    LndIndexFloors,
    _invoice_subscription_lag_message,
    _payment_subscription_lag_message,
)
from v4vapp_backend_v2.models.invoice_models import InvoiceState


class _FakeCursor:
    def __init__(self, docs: list):
        self._docs = docs

    def sort(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def __aiter__(self):
        async def _gen():
            for doc in self._docs:
                yield doc

        return _gen()


@pytest.mark.asyncio
async def test_lag_when_lnd_index_ahead_of_mongo(monkeypatch):
    lnd_client = MagicMock()
    lnd_client.call = AsyncMock(return_value=SimpleNamespace())
    monkeypatch.setattr(
        "lnd_monitor_v2.ListPaymentsResponse",
        lambda _raw: SimpleNamespace(payments=[SimpleNamespace(payment_index=11034)]),
    )
    monkeypatch.setattr(
        "lnd_monitor_v2.Payment.collection",
        lambda: SimpleNamespace(find=lambda *_a, **_k: _FakeCursor([{"payment_index": 11033}])),
    )
    msg = await _payment_subscription_lag_message(lnd_client)
    assert msg is not None
    assert "11034" in msg and "11033" in msg


@pytest.mark.asyncio
async def test_no_lag_when_indexes_match(monkeypatch):
    lnd_client = MagicMock()
    lnd_client.call = AsyncMock(return_value=SimpleNamespace())
    monkeypatch.setattr(
        "lnd_monitor_v2.ListPaymentsResponse",
        lambda _raw: SimpleNamespace(payments=[SimpleNamespace(payment_index=11033)]),
    )
    monkeypatch.setattr(
        "lnd_monitor_v2.Payment.collection",
        lambda: SimpleNamespace(find=lambda *_a, **_k: _FakeCursor([{"payment_index": 11033}])),
    )
    assert await _payment_subscription_lag_message(lnd_client) is None


@pytest.mark.asyncio
async def test_rpc_error_is_not_treated_as_lag():
    lnd_client = MagicMock()
    lnd_client.call = AsyncMock(side_effect=RuntimeError("unavailable"))
    assert await _payment_subscription_lag_message(lnd_client) is None


def _invoice(r_hash: str, *, add_index: int = 118, settle_index: int = 0, state=InvoiceState.OPEN):
    return SimpleNamespace(
        r_hash=r_hash,
        add_index=add_index,
        settle_index=settle_index,
        state=state,
    )


@pytest.mark.asyncio
async def test_invoice_lag_when_newest_r_hash_missing(monkeypatch):
    lnd_client = MagicMock()
    lnd_client.call = AsyncMock(return_value=SimpleNamespace())
    newest = _invoice("abc123newest00", add_index=119)
    monkeypatch.setattr(
        "lnd_monitor_v2.ListInvoiceResponse",
        lambda _raw: SimpleNamespace(invoices=[newest]),
    )
    monkeypatch.setattr("lnd_monitor_v2.get_lnd_index_floors", lambda *_a, **_k: LndIndexFloors())
    collection = MagicMock()
    collection.find_one = AsyncMock(return_value=None)
    monkeypatch.setattr("lnd_monitor_v2.Invoice.collection", lambda: collection)
    msg = await _invoice_subscription_lag_message(lnd_client)
    assert msg is not None
    assert "abc123newest" in msg
    assert "missing from Mongo" in msg


@pytest.mark.asyncio
async def test_invoice_ok_when_newest_r_hash_present(monkeypatch):
    lnd_client = MagicMock()
    lnd_client.call = AsyncMock(return_value=SimpleNamespace())
    newest = _invoice("abc123newest00", add_index=119)
    monkeypatch.setattr(
        "lnd_monitor_v2.ListInvoiceResponse",
        lambda _raw: SimpleNamespace(invoices=[newest]),
    )
    monkeypatch.setattr("lnd_monitor_v2.get_lnd_index_floors", lambda *_a, **_k: LndIndexFloors())
    collection = MagicMock()
    collection.find_one = AsyncMock(return_value={"r_hash": "abc123newest00", "state": "OPEN"})
    monkeypatch.setattr("lnd_monitor_v2.Invoice.collection", lambda: collection)
    assert await _invoice_subscription_lag_message(lnd_client) is None


@pytest.mark.asyncio
async def test_invoice_lag_when_settled_r_hash_still_open_in_mongo(monkeypatch):
    lnd_client = MagicMock()
    lnd_client.call = AsyncMock(return_value=SimpleNamespace())
    settled = _invoice(
        "def456settled0", add_index=118, settle_index=40, state=InvoiceState.SETTLED
    )
    monkeypatch.setattr(
        "lnd_monitor_v2.ListInvoiceResponse",
        lambda _raw: SimpleNamespace(invoices=[settled]),
    )
    monkeypatch.setattr("lnd_monitor_v2.get_lnd_index_floors", lambda *_a, **_k: LndIndexFloors())
    collection = MagicMock()
    collection.find_one = AsyncMock(return_value={"r_hash": "def456settled0", "state": "OPEN"})
    monkeypatch.setattr("lnd_monitor_v2.Invoice.collection", lambda: collection)
    msg = await _invoice_subscription_lag_message(lnd_client)
    assert msg is not None
    assert "def456settle" in msg
    assert "SETTLED" in msg


@pytest.mark.asyncio
async def test_invoice_rpc_error_is_not_treated_as_lag():
    lnd_client = MagicMock()
    lnd_client.call = AsyncMock(side_effect=RuntimeError("unavailable"))
    assert await _invoice_subscription_lag_message(lnd_client) is None
