"""Hung TrackPayments / SubscribeInvoices detection: LND tip ahead of Mongo."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from lnd_monitor_v2 import (
    TIMEDELTA_RETAIN_AFTER_EXPIRY,
    LndIndexFloors,
    _invoice_subscription_lag_message,
    _lnd_invoice_is_prunable,
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


def _patch_payments(monkeypatch, *, lnd_index: int, mongo_docs: list, floors=None):
    monkeypatch.setattr(
        "lnd_monitor_v2.ListPaymentsResponse",
        lambda _raw: SimpleNamespace(payments=[SimpleNamespace(payment_index=lnd_index)]),
    )
    monkeypatch.setattr(
        "lnd_monitor_v2.Payment.collection",
        lambda: SimpleNamespace(find=lambda *_a, **_k: _FakeCursor(mongo_docs)),
    )
    monkeypatch.setattr(
        "lnd_monitor_v2.get_lnd_index_floors",
        lambda *_a, **_k: floors if floors is not None else LndIndexFloors(),
    )


@pytest.mark.asyncio
async def test_lag_when_lnd_index_ahead_of_mongo(monkeypatch):
    lnd_client = MagicMock()
    lnd_client.call = AsyncMock(return_value=SimpleNamespace())
    _patch_payments(monkeypatch, lnd_index=11034, mongo_docs=[{"payment_index": 11033}])
    msg = await _payment_subscription_lag_message(lnd_client)
    assert msg is not None
    assert "11034" in msg and "11033" in msg


@pytest.mark.asyncio
async def test_no_lag_when_indexes_match(monkeypatch):
    lnd_client = MagicMock()
    lnd_client.call = AsyncMock(return_value=SimpleNamespace())
    _patch_payments(monkeypatch, lnd_index=11033, mongo_docs=[{"payment_index": 11033}])
    assert await _payment_subscription_lag_message(lnd_client) is None


@pytest.mark.asyncio
async def test_rpc_error_is_not_treated_as_lag():
    lnd_client = MagicMock()
    lnd_client.call = AsyncMock(side_effect=RuntimeError("unavailable"))
    assert await _payment_subscription_lag_message(lnd_client) is None


@pytest.mark.asyncio
async def test_payment_no_lag_when_lnd_tip_at_or_below_start_payment_index(monkeypatch):
    """Fresh DB + start_payment_index: Mongo never stores index <= floor."""
    lnd_client = MagicMock()
    lnd_client.call = AsyncMock(return_value=SimpleNamespace())
    # production.fromhome / devlegion: start_payment_index: 1
    floors = LndIndexFloors(payment_index=1)
    _patch_payments(monkeypatch, lnd_index=1, mongo_docs=[], floors=floors)
    assert await _payment_subscription_lag_message(lnd_client) is None


@pytest.mark.asyncio
async def test_payment_lag_when_lnd_tip_above_start_payment_index_and_mongo_empty(
    monkeypatch,
):
    lnd_client = MagicMock()
    lnd_client.call = AsyncMock(return_value=SimpleNamespace())
    floors = LndIndexFloors(payment_index=1)
    _patch_payments(monkeypatch, lnd_index=2, mongo_docs=[], floors=floors)
    msg = await _payment_subscription_lag_message(lnd_client)
    assert msg is not None
    assert "2" in msg


def _invoice(
    r_hash: str,
    *,
    add_index: int = 118,
    settle_index: int = 0,
    state=InvoiceState.OPEN,
    expiry_date=None,
    settled: bool = False,
):
    return SimpleNamespace(
        r_hash=r_hash,
        add_index=add_index,
        settle_index=settle_index,
        state=state,
        expiry_date=expiry_date,
        settled=settled,
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


@pytest.mark.asyncio
async def test_invoice_no_lag_when_newest_add_index_at_start_add_index(monkeypatch):
    """start_add_index invoices are never stored, so missing r_hash is expected."""
    lnd_client = MagicMock()
    lnd_client.call = AsyncMock(return_value=SimpleNamespace())
    newest = _invoice("floorhash0000", add_index=25)
    monkeypatch.setattr(
        "lnd_monitor_v2.ListInvoiceResponse",
        lambda _raw: SimpleNamespace(invoices=[newest]),
    )
    # production.fromhome / devlegion: start_add_index: 25
    monkeypatch.setattr(
        "lnd_monitor_v2.get_lnd_index_floors",
        lambda *_a, **_k: LndIndexFloors(add_index=25),
    )
    collection = MagicMock()
    collection.find_one = AsyncMock(return_value=None)
    monkeypatch.setattr("lnd_monitor_v2.Invoice.collection", lambda: collection)
    assert await _invoice_subscription_lag_message(lnd_client) is None
    collection.find_one.assert_not_awaited()


@pytest.mark.asyncio
async def test_invoice_no_lag_when_newest_is_pruned_expired_unsettled(monkeypatch):
    lnd_client = MagicMock()
    lnd_client.call = AsyncMock(return_value=SimpleNamespace())
    pruned_expiry = datetime.now(tz=UTC) - TIMEDELTA_RETAIN_AFTER_EXPIRY - timedelta(hours=1)
    newest = _invoice("oldopenhash00", add_index=119, expiry_date=pruned_expiry)
    monkeypatch.setattr(
        "lnd_monitor_v2.ListInvoiceResponse",
        lambda _raw: SimpleNamespace(invoices=[newest]),
    )
    monkeypatch.setattr("lnd_monitor_v2.get_lnd_index_floors", lambda *_a, **_k: LndIndexFloors())
    collection = MagicMock()
    collection.find_one = AsyncMock(return_value=None)
    monkeypatch.setattr("lnd_monitor_v2.Invoice.collection", lambda: collection)
    assert await _invoice_subscription_lag_message(lnd_client) is None


@pytest.mark.asyncio
async def test_invoice_lag_when_newest_missing_and_still_within_retention(monkeypatch):
    lnd_client = MagicMock()
    lnd_client.call = AsyncMock(return_value=SimpleNamespace())
    recent_expiry = datetime.now(tz=UTC) + timedelta(hours=1)
    newest = _invoice("freshopen0000", add_index=119, expiry_date=recent_expiry)
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
    assert "missing from Mongo" in msg


@pytest.mark.asyncio
async def test_invoice_no_lag_when_settled_add_index_at_or_below_floor(monkeypatch):
    """A SETTLED invoice below start_add_index was never stored."""
    lnd_client = MagicMock()
    lnd_client.call = AsyncMock(return_value=SimpleNamespace())
    settled = _invoice("oldsettle0000", add_index=20, settle_index=3, state=InvoiceState.SETTLED)
    monkeypatch.setattr(
        "lnd_monitor_v2.ListInvoiceResponse",
        lambda _raw: SimpleNamespace(invoices=[settled]),
    )
    monkeypatch.setattr(
        "lnd_monitor_v2.get_lnd_index_floors",
        lambda *_a, **_k: LndIndexFloors(add_index=25),
    )
    collection = MagicMock()
    collection.find_one = AsyncMock(return_value=None)
    monkeypatch.setattr("lnd_monitor_v2.Invoice.collection", lambda: collection)
    assert await _invoice_subscription_lag_message(lnd_client) is None
    collection.find_one.assert_not_awaited()


def test_lnd_invoice_is_prunable_matches_retention_window():
    now = datetime(2026, 8, 15, 12, 0, tzinfo=UTC)
    cutoff = now - TIMEDELTA_RETAIN_AFTER_EXPIRY
    assert _lnd_invoice_is_prunable(
        _invoice("x", expiry_date=cutoff - timedelta(seconds=1)), now=now
    )
    assert not _lnd_invoice_is_prunable(_invoice("x", expiry_date=cutoff), now=now)
    assert not _lnd_invoice_is_prunable(
        _invoice(
            "x",
            expiry_date=cutoff - timedelta(days=2),
            state=InvoiceState.SETTLED,
            settle_index=9,
            settled=True,
        ),
        now=now,
    )
