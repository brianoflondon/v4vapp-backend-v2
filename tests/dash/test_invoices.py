from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from bson import ObjectId
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pymongo.errors import DuplicateKeyError

from v4vapp_backend_v2.config.setup import DashConnectionConfig
from v4vapp_backend_v2.dash.collections import COL_INVOICES, COL_PAYOUTS, COL_WALLET_STATE
from v4vapp_backend_v2.dash.errors import register_dash_exception_handlers
from v4vapp_backend_v2.dash.limits.hive_config import DEFAULT_CONFIG
from v4vapp_backend_v2.dash.models.invoice import DashInvoiceState
from v4vapp_backend_v2.dash.models.quote import Quote
from v4vapp_backend_v2.dash.quotes.service import quote_for_sats
from v4vapp_backend_v2.dash.routers import router as dash_router
from v4vapp_backend_v2.dash.watcher.loop import WatcherState

TESTNET_XPUB = (
    "tpubDC5FSnBiZDMmhiuCmWAYsLwgLYrrT9rAqvTySfuCCrgsWz8wxMXUS9Tb9iVMvcRbv"
    "FcAHGkMD5Kx8koh4GquNGNTfohfk7pgjhaPCdXpoba"
)
FP = "73c5da0a"


class _InsertOne:
    def __init__(self, inserted_id: ObjectId) -> None:
        self.inserted_id = inserted_id


class _Coll:
    def __init__(self) -> None:
        self.docs: list[dict[str, Any]] = []

    def _match(self, doc: dict[str, Any], query: dict[str, Any]) -> bool:
        for key, value in query.items():
            if key == "$or":
                if not any(self._match(doc, part) for part in value):
                    return False
                continue
            if isinstance(value, dict):
                actual = doc.get(key)
                if "$gt" in value and not (actual is not None and actual > value["$gt"]):
                    return False
                if "$gte" in value and not (actual is not None and actual >= value["$gte"]):
                    return False
                if "$eq" in value and actual != value["$eq"]:
                    return False
                if "$in" in value and actual not in value["$in"]:
                    return False
                continue
            if doc.get(key) != value:
                return False
        return True

    async def find_one(self, query: dict[str, Any]) -> dict[str, Any] | None:
        for doc in self.docs:
            if self._match(doc, query):
                return dict(doc)
        return None

    async def insert_one(self, doc: dict[str, Any]) -> _InsertOne:
        if any(existing.get("external_id") == doc.get("external_id") for existing in self.docs):
            raise DuplicateKeyError("E11000 duplicate external_id")
        stored = dict(doc)
        stored["_id"] = ObjectId()
        self.docs.append(stored)
        return _InsertOne(stored["_id"])

    async def update_one(self, query: dict[str, Any], update: dict[str, Any]) -> None:
        for doc in self.docs:
            if self._match(doc, query):
                doc.update(update.get("$set", {}))
                return

    async def find_one_and_update(
        self,
        query: dict[str, Any],
        update: dict[str, Any],
        return_document: Any = None,
    ) -> dict[str, Any] | None:
        for doc in self.docs:
            if not self._match(doc, query):
                continue
            before = dict(doc)
            for key, value in update.get("$inc", {}).items():
                doc[key] = int(doc.get(key, 0)) + int(value)
            doc.update(update.get("$set", {}))
            return dict(doc) if return_document else before
        return None

    def find(self, query: dict[str, Any]) -> "_Cursor":
        matched = [dict(doc) for doc in self.docs if self._match(doc, query)]
        return _Cursor(matched)


class _Cursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def sort(self, spec: list[tuple[str, int]]) -> "_Cursor":
        for key, direction in reversed(spec):
            self._rows.sort(key=lambda row: row[key], reverse=direction < 0)
        return self

    def limit(self, n: int) -> "_Cursor":
        self._rows = self._rows[:n]
        return self

    def __aiter__(self) -> "_Cursor":
        self._iter = iter(self._rows)
        return self

    async def __anext__(self) -> dict[str, Any]:
        try:
            return next(self._iter)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class _Db:
    def __init__(self) -> None:
        self.cols = {
            COL_INVOICES: _Coll(),
            COL_WALLET_STATE: _Coll(),
            COL_PAYOUTS: _Coll(),
        }

    def __getitem__(self, name: str) -> _Coll:
        return self.cols[name]


def _quote() -> Quote:
    return Quote(
        source="coingecko",
        fetched_at="2026-08-13T12:00:00Z",
        btc_usd=Decimal(65000),
        dash_usd=Decimal("32.5"),
        dash_btc=Decimal("0.0005"),
        sats_per_dash=Decimal(50000),
        ttl_s=60,
        duffs_quoted=2000,
        dash_quoted="0.00002000",
    )


def _conn() -> DashConnectionConfig:
    return DashConnectionConfig(
        rpc_url="http://127.0.0.1:19998",
        rpc_user="u",
        rpc_password="p",
        network="testnet",
        xpub=TESTNET_XPUB,
        master_fingerprint=FP,
        settle_policy="instantsend_or_chainlock",
        routing_fee_sats=300,
    )


@pytest.fixture
def invoice_client(monkeypatch: pytest.MonkeyPatch) -> tuple[TestClient, _Db]:
    async def _fake_fetch_quote():
        return _quote()

    monkeypatch.setattr("v4vapp_backend_v2.dash.routers.fetch_quote", _fake_fetch_quote)
    monkeypatch.setattr(
        "v4vapp_backend_v2.dash.routers.fetch_hive_config",
        lambda: DEFAULT_CONFIG,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.dash.limits.check.fetch_rate_windows",
        lambda: list(DEFAULT_CONFIG.windows),
    )

    db = _Db()
    db[COL_WALLET_STATE].docs.append(
        {
            "_id": "testnet",
            "network": "testnet",
            "fingerprint": FP,
            "account_xpub": TESTNET_XPUB,
            "next_receive_index": 0,
            "next_change_index": 0,
            "descriptor_range_end": 100000,
            "updated_at": datetime.now(UTC),
        }
    )
    app = FastAPI()
    register_dash_exception_handlers(app)
    app.include_router(dash_router)
    app.state.dash_db = db
    app.state.dash_conn = _conn()
    app.state.dashd = None
    app.state.watcher = WatcherState()
    client = TestClient(app)
    return client, db


def test_create_invoice_returns_y_address_and_quoted_duffs(
    invoice_client: tuple[TestClient, _Db],
) -> None:
    client, db = invoice_client
    response = client.post(
        "/v2/dash/invoices",
        json={
            "external_id": "hive:test:1",
            "sats": 25000,
            "expires_in_s": 900,
            "cust_id": "v4vapp-test",
        },
    )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["state"] == "OPEN"
    assert body["address"].startswith("y")
    assert body["sats_requested"] == 25_000
    assert body["sats_collect"] == 26_075
    assert body["fees"]["total_fee_sats"] == 1_075
    assert body["fees"]["routing_fee_sats"] == 300
    assert body["duffs_quoted"] == 52_150_000
    assert body["uri"].startswith("dash:y")
    assert body["derivation"]["path"] == "m/44'/1'/0'/0/0"
    assert body["policy"]["settle_policy"] == "instantsend_or_chainlock"
    assert body["policy"]["accept_instantsend"] is True
    assert db[COL_WALLET_STATE].docs[0]["next_receive_index"] == 1


def test_create_stores_lightning_invoice(invoice_client: tuple[TestClient, _Db]) -> None:
    client, db = invoice_client
    bolt11 = "lnbc250u1ptestinvoice"
    response = client.post(
        "/v2/dash/invoices",
        json={
            "external_id": "hive:ln:1",
            "sats": 25000,
            "expires_in_s": 900,
            "lightning_invoice": bolt11,
        },
    )
    assert response.status_code == 201, response.text
    assert response.json()["lightning_invoice"] == bolt11
    stored = db[COL_INVOICES].docs[0]
    assert stored["lightning_invoice"] == bolt11


def test_create_is_idempotent_for_same_payload(invoice_client: tuple[TestClient, _Db]) -> None:
    client, _db = invoice_client
    payload = {"external_id": "hive:test:dup", "sats": 1000, "expires_in_s": 120}
    first = client.post("/v2/dash/invoices", json=payload)
    second = client.post("/v2/dash/invoices", json=payload)
    assert first.status_code == 201
    assert second.status_code == 200
    assert first.json()["invoice_id"] == second.json()["invoice_id"]
    assert first.json()["address"] == second.json()["address"]


def test_create_conflict_on_different_payload(invoice_client: tuple[TestClient, _Db]) -> None:
    client, _db = invoice_client
    client.post(
        "/v2/dash/invoices",
        json={"external_id": "hive:test:x", "sats": 1000, "expires_in_s": 120},
    )
    response = client.post(
        "/v2/dash/invoices",
        json={"external_id": "hive:test:x", "sats": 2000, "expires_in_s": 120},
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "duplicate_external_id"


def test_get_and_cancel(invoice_client: tuple[TestClient, _Db]) -> None:
    client, _db = invoice_client
    created = client.post(
        "/v2/dash/invoices",
        json={"external_id": "hive:test:cancel", "sats": 1000, "expires_in_s": 120},
    ).json()
    fetched = client.get(f"/v2/dash/invoices/{created['invoice_id']}")
    assert fetched.status_code == 200
    by_ext = client.get("/v2/dash/invoices/by-external/hive:test:cancel")
    assert by_ext.json()["address"] == created["address"]
    canceled = client.post(f"/v2/dash/invoices/{created['invoice_id']}/cancel")
    assert canceled.status_code == 200
    assert canceled.json()["state"] == "CANCELED"
    again = client.post(f"/v2/dash/invoices/{created['invoice_id']}/cancel")
    assert again.status_code == 409
    assert again.json()["error"]["code"] == "invoice_not_cancelable"


def test_list_invoices(invoice_client: tuple[TestClient, _Db]) -> None:
    client, _db = invoice_client
    client.post("/v2/dash/invoices", json={"external_id": "a", "sats": 1000, "expires_in_s": 120})
    listed = client.get("/v2/dash/invoices?state=OPEN")
    assert listed.status_code == 200
    assert len(listed.json()["items"]) == 1


def test_payouts_disabled_without_flag(invoice_client: tuple[TestClient, _Db]) -> None:
    client, _db = invoice_client
    response = client.post(
        "/v2/dash/payouts",
        json={
            "external_id": "payout:1",
            "address": "yRd4FhXfVGHXpsuZXPNkMrfD9GVj46pnjt",
            "duffs": 10_000_000,
        },
    )
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "payouts_disabled"


def test_quote_math_still_matches_create() -> None:
    priced = quote_for_sats(25_000, _quote())
    assert priced.duffs_quoted == 50_000_000


def test_rejects_above_hive_maximum(invoice_client: tuple[TestClient, _Db]) -> None:
    client, _db = invoice_client
    response = client.post(
        "/v2/dash/invoices",
        json={"external_id": "hive:test:max", "sats": 180_001, "expires_in_s": 120},
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "amount_too_large"


def test_rate_limit_rejects_when_paid_sats_exceed_window(
    invoice_client: tuple[TestClient, _Db],
) -> None:
    client, db = invoice_client
    db[COL_INVOICES].docs.append(
        {
            "_id": ObjectId(),
            "cust_id": "wallet-app-1",
            "state": DashInvoiceState.SETTLED.value,
            "sats_requested": 500_000,
            "sats_credited": 500_000,
            "settled_at": datetime.now(UTC),
        }
    )
    response = client.post(
        "/v2/dash/invoices",
        json={
            "external_id": "hive:test:over",
            "sats": 150_000,
            "expires_in_s": 120,
            "cust_id": "wallet-app-1",
        },
    )
    assert response.status_code == 422, response.text
    body = response.json()
    assert body["error"]["code"] == "rate_limit_exceeded"
    assert "exceeded" in body["error"]["message"]
    check = body["limit_check"]
    assert check["cust_id"] == "wallet-app-1"
    assert check["limit_ok"] is False
    assert check["periods"]["4"]["sats"] == 650_000
    assert check["periods"]["4"]["limit_sats"] == 600_000
    assert check["periods"]["4"]["limit_ok"] is False


def test_rate_limit_allows_under_window(invoice_client: tuple[TestClient, _Db]) -> None:
    client, _db = invoice_client
    response = client.post(
        "/v2/dash/invoices",
        json={
            "external_id": "hive:test:under",
            "sats": 25_000,
            "expires_in_s": 120,
            "cust_id": "wallet-app-2",
        },
    )
    assert response.status_code == 201, response.text


def test_dash_health_disabled_without_conn() -> None:
    app = FastAPI()
    register_dash_exception_handlers(app)
    app.include_router(dash_router)
    app.state.dash_conn = None
    app.state.dash_db = None
    app.state.dashd = None
    app.state.watcher = WatcherState()
    client = TestClient(app)
    response = client.get("/v2/dash/health")
    assert response.status_code == 200
    assert response.json()["status"] == "disabled"


def test_dash_health_reads_monitor_heartbeat(invoice_client: tuple[TestClient, _Db]) -> None:
    client, db = invoice_client
    now = datetime.now(UTC)
    db[COL_WALLET_STATE].docs[0]["watcher"] = {
        "last_tick_at": now,
        "last_error": None,
        "open_invoices": 1,
        "ticks": 3,
        "stuck": 0,
    }
    db[COL_WALLET_STATE].docs[0]["dashd"] = {
        "synced": True,
        "initialblockdownload": False,
        "blocks": 100,
    }
    response = client.get("/v2/dash/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["watcher"]["open_invoices"] == 1
    assert body["dashd"]["synced"] is True
