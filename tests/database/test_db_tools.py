from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from bson import Decimal128

from v4vapp_backend_v2.database.db_tools import find_nearest_by_timestamp


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
        # Minimal filter interpreter supporting $lte and $gte on the timestamp field
        # and simple equality (or $in) checks for other fields supplied via filter_extra.
        key = self.ts_field
        matched = []
        cond_ts = flt.get(key, {})
        for d in self.docs:
            ts = d.get(key)
            ok = True
            # timestamp bounds
            if "$lte" in cond_ts and not (ts <= cond_ts["$lte"]):
                ok = False
            if "$gte" in cond_ts and not (ts >= cond_ts["$gte"]):
                ok = False

            # additional exact-match filters
            for k, v in flt.items():
                if k == key:
                    continue
                if isinstance(v, dict):
                    # support simple $in
                    if "$in" in v:
                        if d.get(k) not in v["$in"]:
                            ok = False
                            break
                    else:
                        # unsupported operator, conservatively fail
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


@pytest.mark.asyncio
async def test_find_nearest_prefers_before_on_tie_and_converts_decimal128():
    target = datetime(2025, 12, 24, 12, 0, 0, tzinfo=timezone.utc)
    before = {
        "timestamp": target - timedelta(minutes=5),
        "hive_usd": Decimal128("1.23"),
        "hbd_usd": Decimal128("1.00"),
        "btc_usd": Decimal128("30000"),
        "hive_hbd": Decimal128("1.23"),
        "sats_hive": Decimal128("0.0001"),
    }
    after = {
        "timestamp": target + timedelta(minutes=5),
        "hive_usd": Decimal128("2.34"),
        "hbd_usd": Decimal128("1.00"),
        "btc_usd": Decimal128("30000"),
        "hive_hbd": Decimal128("2.34"),
        "sats_hive": Decimal128("0.0001"),
    }

    coll = FakeCollection([before, after])

    res = await find_nearest_by_timestamp(coll, target, max_window=timedelta(hours=1))
    assert res is not None
    assert isinstance(res["hive_usd"], Decimal)
    # tie -> prefer before
    assert res["hive_usd"] == Decimal("1.23")


@pytest.mark.asyncio
async def test_find_nearest_with_filter_extra():
    target = datetime(2025, 12, 24, 12, 0, 0, tzinfo=timezone.utc)
    before_btc = {
        "timestamp": target - timedelta(minutes=5),
        "pair": "btc_usd",
        "hive_usd": Decimal128("1.0"),
    }
    before_eth = {
        "timestamp": target - timedelta(minutes=6),
        "pair": "eth_usd",
        "hive_usd": Decimal128("2.0"),
    }
    coll = FakeCollection([before_eth, before_btc])

    res = await find_nearest_by_timestamp(
        coll, target, max_window=timedelta(hours=1), filter_extra={"pair": "btc_usd"}
    )
    assert res is not None
    assert res["pair"] == "btc_usd"
    assert res["hive_usd"] == Decimal("1.0")


@pytest.mark.asyncio
async def test_find_nearest_respects_window_and_returns_none_if_no_match():
    target = datetime.now(timezone.utc)
    docs = [
        {"timestamp": target - timedelta(days=1), "hive_usd": Decimal128("1.0")},
        {"timestamp": target + timedelta(days=1), "hive_usd": Decimal128("1.0")},
    ]
    coll = FakeCollection(docs)

    # small window -> no match
    res = await find_nearest_by_timestamp(coll, target, max_window=timedelta(minutes=10))
    assert res is None


@pytest.mark.asyncio
async def test_find_nearest_only_before_or_after():
    target = datetime(2025, 12, 24, 12, 0, 0, tzinfo=timezone.utc)
    only_before = [{"timestamp": target - timedelta(minutes=30), "hive_usd": Decimal128("1.0")}]
    coll = FakeCollection(only_before)
    res = await find_nearest_by_timestamp(coll, target)
    assert res is not None
    assert res["hive_usd"] == Decimal("1.0")

    only_after = [{"timestamp": target + timedelta(minutes=30), "hive_usd": Decimal128("2.0")}]
    coll2 = FakeCollection(only_after)
    res2 = await find_nearest_by_timestamp(coll2, target)
    assert res2 is not None
    assert res2["hive_usd"] == Decimal("2.0")
