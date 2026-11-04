"""Ledger replay guard for Hive operations that share a trx id.

The existing process_hive module test is skipped, so this file stays on the
group_id check and does not run the transfer stack.
"""

import logging
from types import SimpleNamespace

import pytest

from v4vapp_backend_v2.accounting.ledger_entry_class import (
    LedgerEntry,
    LedgerEntryCreationException,
    LedgerEntryDuplicateException,
)
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.process.process_hive import (
    HiveLedgerOrderMismatch,
    booked_op_in_trx,
    find_existing_hive_ledger_entry,
    hive_ledger_trx_filter,
    process_hive_op,
)

TRX = "8444006ec7b5a545b198fb5f5b051e3a4055e159"
ZERO_TRX = "0000000000000000000000000000000000000000"


class _LedgerOp(FillOrder):
    """FillOrder shape without running FillOrder.__init__."""

    @property
    def log_extra(self) -> dict:
        return {"op": "test"}


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

    assert await process_hive_op(_gated_op(1, realm="virtual")) == []
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
    assert await process_hive_op(_gated_op(2)) == []
    assert seen == [2]
    assert "already processed past this operation" not in caplog.text
