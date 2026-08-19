from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from bson import ObjectId

from v4vapp_backend_v2.config.setup import DashConnectionConfig
from v4vapp_backend_v2.dash.collections import COL_INVOICES
from v4vapp_backend_v2.dash.models.invoice import DashInvoiceState
from v4vapp_backend_v2.dash.watcher.loop import WatcherState, tick

NOW = datetime(2026, 8, 13, 12, 2, tzinfo=UTC)


class _Cursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def sort(self, _spec: object) -> "_Cursor":
        return self

    def limit(self, _n: int) -> "_Cursor":
        return self

    def __aiter__(self) -> "_Cursor":
        self._iter = iter(self._rows)
        return self

    async def __anext__(self) -> dict[str, Any]:
        try:
            return next(self._iter)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class _Coll:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self.docs = docs
        self.updates: list[tuple[dict[str, Any], dict[str, Any]]] = []

    def find(self, _query: dict[str, Any]) -> _Cursor:
        return _Cursor(list(self.docs))

    async def find_one_and_update(
        self, query: dict[str, Any], update: dict[str, Any], return_document: Any = None
    ) -> dict[str, Any] | None:
        self.updates.append((query, update))
        return update.get("$set")


class _Db:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self.col = _Coll(docs)

    def __getitem__(self, name: str) -> _Coll:
        assert name == COL_INVOICES
        return self.col


class _Dashd:
    def __init__(self, utxos: list[dict[str, Any]], txs: dict[str, dict[str, Any]]) -> None:
        self.utxos = utxos
        self.txs = txs

    async def listunspent(self, *args: object) -> list[dict[str, Any]]:
        return self.utxos

    async def gettransaction(self, txid: str) -> dict[str, Any]:
        return self.txs[txid]


def _invoice() -> dict[str, Any]:
    return {
        "_id": ObjectId(),
        "address": "yAddr1",
        "state": DashInvoiceState.OPEN.value,
        "expires_at": NOW + timedelta(minutes=10),
        "settle_deadline_at": NOW + timedelta(hours=1),
        "duffs_quoted": 50_000_000,
        "sats_requested": 25_000,
        "duffs_received": 0,
        "txids": [],
        "swept_at": None,
        "policy": {
            "settle_policy": "instantsend_or_chainlock",
            "underpay_tolerance_bps": 100,
            "underpay_tolerance_duffs": 50_000,
        },
    }


def _conn() -> DashConnectionConfig:
    return DashConnectionConfig(poll_interval_s=10, watch_batch=500, dust_duffs=5460)


@pytest.mark.asyncio
async def test_tick_settles_instantsend(monkeypatch: pytest.MonkeyPatch) -> None:
    inv = _invoice()
    db = _Db([inv])
    dashd = _Dashd(
        utxos=[{"txid": "aa", "vout": 0, "address": "yAddr1", "amount": 0.5, "confirmations": 0}],
        txs={"aa": {"instantlock": True, "chainlock": False, "confirmations": 0}},
    )
    state = WatcherState()

    class _FrozenDateTime:
        @staticmethod
        def now(tz: object = None) -> datetime:
            return NOW

    monkeypatch.setattr("v4vapp_backend_v2.dash.watcher.loop.datetime", _FrozenDateTime)
    await tick(db=db, dashd=dashd, conn=_conn(), state=state)
    assert db.col.updates
    _filt, update = db.col.updates[-1]
    assert update["$set"]["state"] == DashInvoiceState.SETTLED.value
    assert update["$set"]["sats_credited"] == 25_000
    assert state.last_tick_at == NOW


@pytest.mark.asyncio
async def test_tick_settle_logs_invoice_extras(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    inv = _invoice()
    inv["external_id"] = "hive:watch:1"
    db = _Db([inv])
    dashd = _Dashd(
        utxos=[{"txid": "aa", "vout": 0, "address": "yAddr1", "amount": 0.5, "confirmations": 0}],
        txs={"aa": {"instantlock": True, "chainlock": False, "confirmations": 0}},
    )
    state = WatcherState()

    class _FrozenDateTime:
        @staticmethod
        def now(tz: object = None) -> datetime:
            return NOW

    monkeypatch.setattr("v4vapp_backend_v2.dash.watcher.loop.datetime", _FrozenDateTime)
    caplog.set_level("INFO", logger="backend")
    await tick(db=db, dashd=dashd, conn=_conn(), state=state)
    settled = [r for r in caplog.records if "invoice settled" in r.getMessage()]
    assert settled
    rec = settled[-1]
    assert rec.invoice_id == str(inv["_id"])
    assert rec.external_id == "hive:watch:1"
    assert rec.state == DashInvoiceState.SETTLED.value
    assert rec.address == "yAddr1"
    assert rec.duffs_received == 50_000_000
    assert rec.txid == "aa"
