"""Ledger replay guard for Hive operations that share a trx id.

The existing process_hive module test is skipped, so this file stays on the
group_id check and does not run the transfer stack.
"""

import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from v4vapp_backend_v2.accounting.ledger_entry_class import (
    LedgerEntry,
    LedgerEntryCreationException,
    LedgerEntryDuplicateException,
)
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.process.process_hive import (
    HiveLedgerOrderMismatch,
    booked_op_in_trx,
    find_existing_hive_ledger_entry,
    hive_ledger_trx_filter,
    process_hive_op,
)
from v4vapp_backend_v2.process.process_tracked_events import process_tracked_event

TRX = "8444006ec7b5a545b198fb5f5b051e3a4055e159"
ZERO_TRX = "0000000000000000000000000000000000000000"


class _LedgerOp(FillOrder):
    """FillOrder shape without running FillOrder.__init__."""

    @property
    def log_extra(self) -> dict:
        return {"op": "test"}


class _LimitOp(LimitOrderCreate):
    """Limit-order shape for the non-fill order gate. Skips LimitOrderCreate.__init__."""

    @property
    def log_extra(self) -> dict:
        return {"op": "test"}

    @property
    def log_str(self) -> str:
        return f"limit {self.op_in_trx}"


def _limit_op(op_in_trx: int, *, realm: str = "virtual"):
    return _LimitOp.model_construct(
        realm=realm,
        trx_id=TRX,
        op_in_trx=op_in_trx,
        op_type="limit_order_create",
        block_num=95887860,
        trx_num=0,
        timestamp=datetime.now(tz=UTC),
    )


def _gated_op(
    op_in_trx: int, *, block_num: int = 95887860, trx_id: str = TRX, realm: str = "real"
):
    """An operation process_hive_op accepts at its type gate."""
    spec = _op(op_in_trx, block_num=block_num, trx_id=trx_id, realm=realm)
    return _LedgerOp.model_construct(
        realm=spec.realm,
        trx_id=spec.trx_id,
        op_in_trx=spec.op_in_trx,
        op_type="fill_order",
        block_num=spec.block_num,
        trx_num=0,
    )


def _op(op_in_trx: int, *, block_num: int = 95887860, trx_id: str = TRX, realm: str = "real"):
    return SimpleNamespace(
        block_num=block_num,
        trx_id=trx_id,
        op_in_trx=op_in_trx,
        realm=realm,
        group_id=f"{block_num}_{trx_id}_{op_in_trx}_{realm}",
        group_id_query={
            "block_num": block_num,
            "trx_id": trx_id,
            "op_in_trx": op_in_trx,
            "realm": realm,
        },
        log_extra={"op": "test"},
    )


def _row(op_in_trx: int, suffix: str = "", **kwargs) -> dict:
    op = _op(op_in_trx, **kwargs)
    group_id = op.group_id if not suffix else f"{op.group_id}_{suffix}"
    return {"group_id": group_id}


class _Cursor:
    def __init__(self, docs: list[dict]):
        self.docs = docs

    async def to_list(self, length=None):
        return list(self.docs)


class _Ledger:
    def __init__(self, docs: list[dict]):
        self.docs = docs
        self.filters: list[dict] = []
        self.projections: list[dict | None] = []

    def find(self, filter=None, projection=None):
        self.filters.append(filter)
        self.projections.append(projection)
        return _Cursor(self.docs)

    async def find_one(self, filter=None, projection=None):
        self.filters.append(filter)
        group_id = (filter or {}).get("group_id")
        for doc in self.docs:
            if doc.get("group_id") == group_id:
                return doc
        return None


def _patch_ledger(monkeypatch: pytest.MonkeyPatch, docs: list[dict]) -> _Ledger:
    ledger = _Ledger(docs)
    monkeypatch.setattr(LedgerEntry, "collection", lambda: ledger)
    return ledger


class _HiveOps:
    def __init__(self, docs: list[dict]):
        self.docs = docs

    def find(self, filter=None, projection=None):
        return _Cursor(self.docs)


def _patch_hive(monkeypatch: pytest.MonkeyPatch, docs: list[dict]) -> _HiveOps:
    hive = _HiveOps(docs)
    monkeypatch.setattr(FillOrder, "collection", lambda: hive)
    return hive


def test_fill_suffix_is_the_same_op_index():
    op = _op(2, realm="virtual")
    buy = f"{op.group_id}_fill_or_b"
    sell = f"{op.group_id}_fill_or_s"
    assert booked_op_in_trx(buy, op) == 2
    assert booked_op_in_trx(sell, op) == 2
    assert booked_op_in_trx(f"{op.group_id}", op) == 2


def test_other_block_realm_and_trx_do_not_count():
    op = _op(1, realm="virtual")
    other_block = _row(1, block_num=op.block_num + 1, trx_id=ZERO_TRX, realm="virtual")
    other_realm = _row(1, realm="real")
    assert booked_op_in_trx(other_block["group_id"], op) is None
    assert booked_op_in_trx(other_realm["group_id"], op) is None


def test_filter_is_anchored_on_group_id_not_the_hive_field_query():
    op = _op(1, realm="virtual")
    query = hive_ledger_trx_filter(op)
    pattern = query["group_id"]["$regex"]
    assert pattern.startswith(f"^{op.block_num}_{op.trx_id}_")
    assert pattern.endswith(r"\d+_virtual($|_)")
    assert "block_num" not in query
    assert query != op.group_id_query


async def test_fresh_op_is_processed(monkeypatch: pytest.MonkeyPatch):
    _patch_ledger(monkeypatch, [])
    assert await find_existing_hive_ledger_entry(_op(1)) is None


async def test_op_2_with_one_prior_op_is_processed(monkeypatch: pytest.MonkeyPatch):
    _patch_ledger(monkeypatch, [_row(1)])
    assert await find_existing_hive_ledger_entry(_op(2)) is None


async def test_gap_when_earlier_ops_created_no_ledger_row(monkeypatch: pytest.MonkeyPatch):
    _patch_ledger(monkeypatch, [_row(1, realm="virtual")])
    assert await find_existing_hive_ledger_entry(_op(4, realm="virtual")) is None


async def test_two_fill_rows_count_as_one_op(monkeypatch: pytest.MonkeyPatch):
    docs = [_row(1, "fill_or_b", realm="virtual"), _row(1, "fill_or_s", realm="virtual")]
    _patch_ledger(monkeypatch, docs)
    assert await find_existing_hive_ledger_entry(_op(2, realm="virtual")) is None


async def test_same_op_index_is_a_duplicate_including_a_fill_suffix(
    monkeypatch: pytest.MonkeyPatch,
):
    op = _op(1, realm="virtual")
    doc = _row(1, "fill_or_b", realm="virtual")
    _patch_ledger(monkeypatch, [doc, _row(1, "fill_or_s", realm="virtual")])
    found = await find_existing_hive_ledger_entry(op)
    assert found == doc

    with pytest.raises(LedgerEntryDuplicateException):
        await process_hive_op(_gated_op(1, realm="virtual"))


async def test_exact_group_id_is_preferred_over_a_suffixed_row(monkeypatch: pytest.MonkeyPatch):
    op = _op(1)
    exact = _row(1)
    _patch_ledger(monkeypatch, [_row(1, "susp"), exact])
    assert await find_existing_hive_ledger_entry(op) == exact


async def test_op_1_when_four_other_indexes_are_booked_is_dropped(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    caplog.set_level(logging.WARNING, logger="backend")
    docs = [_row(index, realm="virtual") for index in (2, 3, 4, 5)]
    ledger = _patch_ledger(monkeypatch, docs)
    op = _op(1, realm="virtual")

    with pytest.raises(HiveLedgerOrderMismatch):
        await find_existing_hive_ledger_entry(op)

    with pytest.raises(HiveLedgerOrderMismatch):
        await process_hive_op(_limit_op(1))
    assert ledger.filters
    assert ledger.filters[0] == hive_ledger_trx_filter(op)
    assert ledger.projections[0] == {"group_id": 1}
    warning = caplog.text
    assert op.trx_id in warning
    assert "op_in_trx is 1" in warning
    assert "[2, 3, 4, 5]" in warning


async def test_replay_of_the_stored_first_op_is_still_a_duplicate(
    monkeypatch: pytest.MonkeyPatch,
):
    """A changed read order that reuses index 1 hits the existing row."""
    docs = [_row(index) for index in (1, 2, 3, 4)]
    _patch_ledger(monkeypatch, docs)
    with pytest.raises(LedgerEntryDuplicateException):
        await process_hive_op(_gated_op(1))


async def test_zero_trx_does_not_collide_across_blocks(monkeypatch: pytest.MonkeyPatch):
    other_block = [
        _row(index, block_num=10, trx_id=ZERO_TRX, realm="virtual") for index in (1, 2, 3, 4)
    ]
    _patch_ledger(monkeypatch, other_block)
    incoming = _op(1, block_num=11, trx_id=ZERO_TRX, realm="virtual")
    assert await find_existing_hive_ledger_entry(incoming) is None


async def test_real_op_does_not_count_virtual_ops_in_the_same_trx(
    monkeypatch: pytest.MonkeyPatch,
):
    _patch_ledger(monkeypatch, [_row(1, realm="real")])
    assert await find_existing_hive_ledger_entry(_op(1, realm="virtual")) is None


async def test_arrival_behind_a_later_booked_op_is_dropped(monkeypatch: pytest.MonkeyPatch):
    """A batch already contains op 4, so op 3 is not the next live operation."""
    _patch_ledger(monkeypatch, [_row(1, realm="virtual"), _row(4, realm="virtual")])
    with pytest.raises(HiveLedgerOrderMismatch):
        await find_existing_hive_ledger_entry(_op(3, realm="virtual"))


async def test_historical_op_zero_is_the_same_first_slot(monkeypatch: pytest.MonkeyPatch):
    stored = _row(0)
    _patch_ledger(monkeypatch, [stored])
    found = await find_existing_hive_ledger_entry(_op(1))
    assert found == stored
    with pytest.raises(LedgerEntryDuplicateException):
        await process_hive_op(_gated_op(1))


async def test_invalid_existing_document_raises_creation_exception(
    monkeypatch: pytest.MonkeyPatch,
):
    doc = _row(1)
    doc["timestamp"] = "not-a-timestamp"
    _patch_ledger(monkeypatch, [doc])
    with pytest.raises(LedgerEntryCreationException):
        await process_hive_op(_gated_op(1))


async def test_sequential_op_does_not_warn(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    caplog.set_level(logging.WARNING, logger="backend")
    seen: list[int] = []

    async def _record_fill(limit_fill_order, nobroadcast: bool = False):
        seen.append(limit_fill_order.op_in_trx)
        return []

    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_hive.process_create_fill_order_op",
        _record_fill,
    )
    _patch_ledger(monkeypatch, [_row(1)])
    _patch_hive(monkeypatch, [])
    assert await process_hive_op(_gated_op(2)) == []
    assert seen == [2]
    assert "already processed past this operation" not in caplog.text


# Block 111471342, the HBD→HIVE rebalance whose fills were handled out of order.
# Amounts are Hive integer strings (precision 3): 335 = 0.335 HBD, 7665 = 7.665 HIVE.
BLOCK = 111471342
TRX_FILL = "74fa4174a4bd8f69d4a5af41dadc62962722dc75"
HBD_NAI = "@@000000013"
HIVE_NAI = "@@000000021"
SEVEN_FILLS = (
    (1, "puncakbukit", "335", "7665", 101),
    (2, "honeybot", "585", "13370", 102),
    (3, "quicktrades", "1090", "24899", 103),
    (4, "econo", "500", "11417", 104),
    (5, "droida", "1074", "24517", 105),
    (6, "dtake", "936", "21340", 106),
    (7, "konvik-hbd", "35175", "801914", 107),
)


def _fill(
    op_in_trx: int, owner: str, hbd: str, hive: str, open_orderid: int
) -> _LedgerOp:
    return _LedgerOp.model_construct(
        realm="virtual",
        trx_id=TRX_FILL,
        op_in_trx=op_in_trx,
        op_type="fill_order",
        block_num=BLOCK,
        trx_num=1,
        current_orderid=2548986856,
        open_orderid=open_orderid,
        current_owner="v4vapp",
        open_owner=owner,
        current_pays=AmountPyd.model_construct(amount=hbd, nai=HBD_NAI, precision=3),
        open_pays=AmountPyd.model_construct(amount=hive, nai=HIVE_NAI, precision=3),
    )


def _hive_doc(op: _LedgerOp) -> dict:
    return {
        "op_in_trx": op.op_in_trx,
        "current_orderid": op.current_orderid,
        "open_orderid": op.open_orderid,
        "current_owner": op.current_owner,
        "open_owner": op.open_owner,
        "current_pays": {"amount": op.current_pays.amount, "nai": op.current_pays.nai},
        "open_pays": {"amount": op.open_pays.amount, "nai": op.open_pays.nai},
        "type": "fill_order",
    }


def _net_row(op: _LedgerOp) -> dict:
    return _row(
        op.op_in_trx,
        "fill_or_n",
        block_num=BLOCK,
        trx_id=TRX_FILL,
        realm="virtual",
    )


async def test_fill_behind_a_later_index_is_booked(monkeypatch: pytest.MonkeyPatch):
    """Op 2 is already stored. Op 1 is a different fill and must not be dropped."""
    booked = _fill(2, "honeybot", "585", "13370", 102)
    incoming = _fill(1, "puncakbukit", "335", "7665", 101)
    _patch_ledger(monkeypatch, [_net_row(booked)])
    _patch_hive(monkeypatch, [_hive_doc(booked)])

    assert await find_existing_hive_ledger_entry(incoming) is None


async def test_rescan_of_the_same_fill_under_a_new_index_is_a_duplicate(
    monkeypatch: pytest.MonkeyPatch,
):
    booked = _fill(2, "honeybot", "585", "13370", 102)
    shifted = _fill(64, "honeybot", "585", "13370", 102)
    doc = _net_row(booked)
    _patch_ledger(monkeypatch, [doc])
    _patch_hive(monkeypatch, [_hive_doc(booked)])

    assert await find_existing_hive_ledger_entry(shifted) == doc
    with pytest.raises(LedgerEntryDuplicateException):
        await process_hive_op(shifted)


async def test_seven_fills_book_the_skipped_first_op(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    """Op 2 is booked first, then op 1 arrives, then ops 3–7.

    Ledger totals are the seven on-chain fills: 39.695 HBD and 905.122 HIVE.
    """
    caplog.set_level(logging.WARNING, logger="backend")
    fills = [_fill(*spec) for spec in SEVEN_FILLS]
    by_index = {op.op_in_trx: op for op in fills}
    ledger_docs = [_net_row(by_index[2])]
    hive_docs = [_hive_doc(by_index[2])]
    ledger = _patch_ledger(monkeypatch, ledger_docs)
    _patch_hive(monkeypatch, hive_docs)
    seen: list[int] = []

    async def _book(limit_fill_order, nobroadcast: bool = False):
        seen.append(limit_fill_order.op_in_trx)
        hive_docs.append(_hive_doc(limit_fill_order))
        ledger.docs.append(_net_row(limit_fill_order))
        return []

    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_hive.process_create_fill_order_op",
        _book,
    )

    for index in (1, 3, 4, 5, 6, 7):
        assert await process_hive_op(by_index[index]) == []

    assert seen == [1, 3, 4, 5, 6, 7]
    assert "Dropping" not in caplog.text
    assert len(ledger.docs) == 7

    shifted = _fill(11, "puncakbukit", "335", "7665", 101)
    with pytest.raises(LedgerEntryDuplicateException):
        await process_hive_op(shifted)
    assert seen == [1, 3, 4, 5, 6, 7]
    assert len(ledger.docs) == 7

    hbd = sum(int(op.current_pays.amount) for op in fills)
    hive = sum(int(op.open_pays.amount) for op in fills)
    assert hbd == 39_695
    assert hive == 905_122
    first = by_index[1]
    assert first.open_owner == "puncakbukit"
    assert first.current_pays.amount == "335"
    assert first.open_pays.amount == "7665"


@asynccontextmanager
async def _dummy_locked(self, timeout=None, blocking_timeout=None, request_details=None):
    yield


async def test_dropped_non_fill_does_not_log_success(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    caplog.set_level(logging.INFO)
    _patch_ledger(monkeypatch, [_row(index, realm="virtual") for index in (2, 3, 4, 5)])
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_tracked_events.LedgerEntry.load",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_tracked_events.load_tracked_object",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_tracked_events.LockStr.locked",
        _dummy_locked,
    )

    async def _save(self, *args, **kwargs):
        return None

    monkeypatch.setattr(_LimitOp, "save", _save)
    await process_tracked_event(_limit_op(1))

    messages = [record.getMessage() for record in caplog.records]
    assert any(message.startswith("❌ dropped ") for message in messages)
    assert not any(message.startswith("✅") for message in messages)
