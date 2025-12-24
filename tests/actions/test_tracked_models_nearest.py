from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from bson import Decimal128

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig


class FakeCursor:
    def __init__(self, docs):
        self.docs = list(docs)

    def sort(self, field, direction):
        reverse = direction < 0
        self.docs.sort(key=lambda d: d.get(field), reverse=reverse)
        return self

    def limit(self, _n):
        self.docs = self.docs[:_n]
        return self

    async def to_list(self, length=None):
        return self.docs


class FakeCollection:
    def __init__(self, docs, ts_field="timestamp"):
        self.docs = docs
        self.ts_field = ts_field

    def find(self, flt):
        key = self.ts_field
        matched = []
        cond_ts = flt.get(key, {})
        for d in self.docs:
            ts = d.get(key)
            ok = True
            if "$lte" in cond_ts and not (ts <= cond_ts["$lte"]):
                ok = False
            if "$gte" in cond_ts and not (ts >= cond_ts["$gte"]):
                ok = False

            # support simple equality checks for extra filters
            for k, v in flt.items():
                if k == key:
                    continue
                if isinstance(v, dict):
                    if "$in" in v:
                        if d.get(k) not in v["$in"]:
                            ok = False
                            break
                    else:
                        if d.get(k) != v:
                            ok = False
                            break
                else:
                    if d.get(k) != v:
                        ok = False
                        break

            if ok:
                matched.append(d)
        return FakeCursor(matched)


class FakeDB:
    def __init__(self, collection):
        self._collection = collection

    def __getitem__(self, name):
        return self._collection


@pytest.mark.asyncio
async def test_trackedbase_nearest_quote_uses_window_and_fallback(monkeypatch):
    now = datetime.now(timezone.utc)
    # target is 2 hours ago to avoid update_quote early-return (<600s)
    target = now - timedelta(hours=2)

    # doc is 2.5 hours ago (outside 1 hour window), but exists for unbounded fallback
    doc = {
        "timestamp": target - timedelta(minutes=30),
        "hive_usd": Decimal128("1.50"),
        "hbd_usd": Decimal128("1.00"),
        "btc_usd": Decimal128("30000"),
        "hive_hbd": Decimal128("1.50"),
        "sats_hive": Decimal128("0.0001"),
        "sats_usd": Decimal128("0.0001"),
        "sats_hbd": Decimal128("0.0001"),
    }

    coll = FakeCollection([doc])
    fake_db = FakeDB(coll)
    monkeypatch.setattr(InternalConfig, "db", fake_db)

    q = await TrackedBaseModel.nearest_quote(timestamp=target)

    assert q is not None
    assert hasattr(q, "fetch_date")
    assert q.fetch_date == doc["timestamp"]
    assert isinstance(q.hive_usd, Decimal)
    assert q.hive_usd == Decimal("1.50")


@pytest.mark.asyncio
async def test_trackedbase_nearest_quote_prefers_before_on_tie(monkeypatch):
    now = datetime.now(timezone.utc)
    target = now - timedelta(hours=2)

    before = {
        "timestamp": target - timedelta(minutes=5),
        "hive_usd": Decimal128("1.0"),
        "hbd_usd": Decimal128("1.0"),
        "btc_usd": Decimal128("30000"),
        "hive_hbd": Decimal128("1.0"),
        "sats_hive": Decimal128("0.0001"),
        "sats_usd": Decimal128("0.0001"),
        "sats_hbd": Decimal128("0.0001"),
    }
    after = {
        "timestamp": target + timedelta(minutes=5),
        "hive_usd": Decimal128("2.0"),
        "hbd_usd": Decimal128("1.0"),
        "btc_usd": Decimal128("30000"),
        "hive_hbd": Decimal128("2.0"),
        "sats_hive": Decimal128("0.0001"),
        "sats_usd": Decimal128("0.0001"),
        "sats_hbd": Decimal128("0.0001"),
    }

    coll = FakeCollection([before, after])
    fake_db = FakeDB(coll)
    monkeypatch.setattr(InternalConfig, "db", fake_db)

    q = await TrackedBaseModel.nearest_quote(timestamp=target)

    assert q is not None
    assert q.hive_usd == Decimal("1.0")
