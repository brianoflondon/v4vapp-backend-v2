from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from bson import Decimal128

from v4vapp_backend_v2.database.db_tools import (
    find_nearest_by_timestamp,
    find_nearest_by_timestamp_server_side,
)


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

    def aggregate(self, pipeline):
        """A minimal interpreter for a small subset of the aggregation pipeline used
        by `find_nearest_by_timestamp_server_side` in tests. Supports an initial
        $match, $addFields (delta computation with $subtract and $abs), $sort, and
        $limit stages.
        """
        docs = [d.copy() for d in self.docs]
        for stage in pipeline:
            if "$match" in stage:
                flt = stage["$match"]
                matched = []
                for d in docs:
                    ts = d.get(self.ts_field)
                    ok = True
                    # timestamp conditions
                    ts_cond = flt.get(self.ts_field, {})
                    if isinstance(ts_cond, dict):
                        if "$lte" in ts_cond and not (ts is not None and ts <= ts_cond["$lte"]):
                            ok = False
                        if "$gte" in ts_cond and not (ts is not None and ts >= ts_cond["$gte"]):
                            ok = False
                        if "$ne" in ts_cond and ts == ts_cond["$ne"]:
                            ok = False
                    # other exact-match filters
                    for k, v in flt.items():
                        if k == self.ts_field:
                            continue
                        if isinstance(v, dict) and "$in" in v:
                            if d.get(k) not in v["$in"]:
                                ok = False
                                break
                        else:
                            if d.get(k) != v:
                                ok = False
                                break
                    if ok:
                        matched.append(d)
                docs = matched
            elif "$addFields" in stage:
                add = stage["$addFields"]
                # expecting delta: { $abs: { $subtract: ["$timestamp", <datetime>] } }
                for k, expr in add.items():
                    if "$abs" in expr and "$subtract" in expr["$abs"]:
                        left, right = expr["$abs"]["$subtract"]
                        # left expected to be a string like "$timestamp"
                        if isinstance(left, str) and left.startswith("$"):
                            fld = left[1:]
                            for d in docs:
                                ts = d.get(fld)
                                if ts is None:
                                    d[k] = None
                                else:
                                    # compute milliseconds like MongoDB
                                    d[k] = abs((ts - right).total_seconds() * 1000)
            elif "$sort" in stage:
                spec = stage["$sort"]
                # sort by multiple fields; Python's sort is stable so apply reversed
                # order of sort keys to emulate MongoDB compound sort
                items = list(spec.items())

                def keyfunc(doc):
                    vals = []
                    for fld, direction in items:
                        v = doc.get(fld)
                        # None sorts last for ascending, first for descending
                        if v is None:
                            vals.append(float("inf") if direction > 0 else float("-inf"))
                        else:
                            vals.append(v)
                    return tuple(vals)

                docs.sort(key=keyfunc)
            elif "$limit" in stage:
                n = stage["$limit"]
                docs = docs[:n]
        return FakeCursor(docs)


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


# ---- Server-side aggregation variant tests ----
@pytest.mark.asyncio
async def test_find_nearest_prefers_before_on_tie_and_converts_decimal128_server_side():
    target = datetime(2025, 12, 24, 12, 0, 0, tzinfo=timezone.utc)
    before = {
        "timestamp": target - timedelta(minutes=5),
        "hive_usd": Decimal128("1.23"),
    }
    after = {
        "timestamp": target + timedelta(minutes=5),
        "hive_usd": Decimal128("2.34"),
    }
    coll = FakeCollection([before, after])

    res = await find_nearest_by_timestamp_server_side(coll, target, max_window=timedelta(hours=1))
    assert res is not None
    assert isinstance(res["hive_usd"], Decimal)
    # tie -> prefer before
    assert res["hive_usd"] == Decimal("1.23")


@pytest.mark.asyncio
async def test_find_nearest_with_filter_extra_server_side():
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

    res = await find_nearest_by_timestamp_server_side(
        coll, target, max_window=timedelta(hours=1), filter_extra={"pair": "btc_usd"}
    )
    assert res is not None
    assert res["pair"] == "btc_usd"
    assert res["hive_usd"] == Decimal("1.0")


@pytest.mark.asyncio
async def test_find_nearest_respects_window_and_returns_none_if_no_match_server_side():
    target = datetime.now(timezone.utc)
    docs = [
        {"timestamp": target - timedelta(days=1), "hive_usd": Decimal128("1.0")},
        {"timestamp": target + timedelta(days=1), "hive_usd": Decimal128("1.0")},
    ]
    coll = FakeCollection(docs)

    # small window -> no match
    res = await find_nearest_by_timestamp_server_side(
        coll, target, max_window=timedelta(minutes=10)
    )
    assert res is None


@pytest.mark.asyncio
async def test_find_nearest_only_before_or_after_server_side():
    target = datetime(2025, 12, 24, 12, 0, 0, tzinfo=timezone.utc)
    only_before = [{"timestamp": target - timedelta(minutes=30), "hive_usd": Decimal128("1.0")}]
    coll = FakeCollection(only_before)
    res = await find_nearest_by_timestamp_server_side(coll, target)
    assert res is not None
    assert res["hive_usd"] == Decimal("1.0")

    only_after = [{"timestamp": target + timedelta(minutes=30), "hive_usd": Decimal128("2.0")}]
    coll2 = FakeCollection(only_after)
    res2 = await find_nearest_by_timestamp_server_side(coll2, target)
    assert res2 is not None
    assert res2["hive_usd"] == Decimal("2.0")
