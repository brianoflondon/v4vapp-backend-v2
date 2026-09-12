"""PR1 Magi identity: hash keeper, field-policy save, no double MAGI_* posting."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from mongomock_motor import AsyncMongoMockClient

from magi_monitor import get_last_indexer_id
from tests.magi.test_stream_magi import SAMPLE_EVENT
from v4vapp_backend_v2.accounting.pipelines.simple_pipelines import IGNORED_UPDATE_FIELDS
from v4vapp_backend_v2.actions.tracked_any import load_tracked_object
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.magi.magi_classes import (
    DB_MAGI_BTC_COLLECTION,
    MagiBTCTransferEvent,
    MagiIdentityInconsistency,
    assert_magi_identity_ready,
    find_magi_docs,
    identity_key_from_hash,
    magi_hash_aliases,
    persist_watched_magi_event,
    select_keeper,
)
from v4vapp_backend_v2.process.process_tracked_events import process_tracked_event

LEDGER_COL = "ledger"


@pytest.fixture
def mock_db(mocker):
    client = AsyncMongoMockClient()
    db = client["test_db"]
    mocker.patch.object(InternalConfig, "db", db)
    return db


@pytest.fixture
def noop_locks(mocker):
    @asynccontextmanager
    async def _noop(self, *args, **kwargs):
        yield

    mocker.patch(
        "v4vapp_backend_v2.process.process_tracked_events.LockStr.locked",
        _noop,
    )


def _magi_doc(**overrides) -> dict:
    tx = overrides.get("indexer_tx_hash", SAMPLE_EVENT["indexer_tx_hash"])
    indexer_id = overrides.get("indexer_id", SAMPLE_EVENT["indexer_id"])
    trx = tx.rsplit("-", 1)[0] if "-" in tx else tx
    doc = {
        **SAMPLE_EVENT,
        "group_id": f"{indexer_id}_{trx}_magi",
        "from_account": "alice",
        "to_account": "bob",
        "all_accounts": ["alice", "bob"],
        "op_in_trx": MagiBTCTransferEvent.model_construct(indexer_tx_hash=tx).op_in_trx,
        "short_id": f"{trx[-8:]}_m",
        "op_type": "magi_btc_transfer_event",
        "amount": 5000,
    }
    doc.update(overrides)
    return doc


# ---------------------------------------------------------------------------
# Hash aliases / identity_key
# ---------------------------------------------------------------------------


def test_magi_hash_aliases_op1_no_suffix():
    assert magi_hash_aliases("abc123") == ["abc123", "abc123-0"]


def test_magi_hash_aliases_op1_zero_suffix():
    assert magi_hash_aliases("abc123-0") == ["abc123", "abc123-0"]


def test_magi_hash_aliases_op2_does_not_include_stripped_trx():
    assert magi_hash_aliases("abc123-1") == ["abc123-1"]
    assert "abc123" not in magi_hash_aliases("abc123-1")
    assert "abc123-0" not in magi_hash_aliases("abc123-1")


def test_identity_key_from_hash_no_suffix_and_zero_share_key():
    assert identity_key_from_hash("abc123") == "abc123_1"
    assert identity_key_from_hash("abc123-0") == "abc123_1"
    assert identity_key_from_hash("abc123-1") == "abc123_2"


def test_ignored_update_fields_include_identity():
    for field in ("group_id", "legacy_group_id", "identity_key"):
        assert field in IGNORED_UPDATE_FIELDS


# ---------------------------------------------------------------------------
# find / select
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_find_magi_docs_by_hash_finds_old_format_without_identity_key(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(_magi_doc())
    docs = await find_magi_docs(indexer_tx_hash="abc123", identity_key="abc123_1")
    assert len(docs) == 1
    assert docs[0]["indexer_id"] == 42


@pytest.mark.asyncio
async def test_find_magi_docs_old_format_string_is_exact_only(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(_magi_doc())
    found = await find_magi_docs(group_id="42_abc123_magi")
    assert len(found) == 1
    missed = await find_magi_docs(group_id="99_abc123_magi")
    assert missed == []


@pytest.mark.asyncio
async def test_find_magi_docs_op2_hash_does_not_match_op1(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(_magi_doc())
    docs = await find_magi_docs(indexer_tx_hash="abc123-1", identity_key="abc123_2")
    assert docs == []


@pytest.mark.asyncio
async def test_select_keeper_prefers_process_time_over_higher_indexer_id(mock_db):
    processed = _magi_doc(indexer_id=42, process_time=1.0)
    empty_higher = _magi_doc(indexer_id=99)
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_many([processed, empty_higher])
    docs = await mock_db[DB_MAGI_BTC_COLLECTION].find({}).to_list(length=10)
    keeper = await select_keeper(docs)
    assert keeper["indexer_id"] == 42
    assert keeper.get("process_time") == 1.0


# ---------------------------------------------------------------------------
# save field policy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_save_rescan_does_not_overwrite_indexer_id_or_group_id(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _magi_doc(process_time=1.0, identity_key="abc123_1")
    )
    incoming = MagiBTCTransferEvent(**{**SAMPLE_EVENT, "indexer_id": 99})
    await incoming.save()
    count = await mock_db[DB_MAGI_BTC_COLLECTION].count_documents({})
    assert count == 1
    doc = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({})
    assert doc["indexer_id"] == 42
    assert doc["group_id"] == "abc123_1_magi"
    assert doc["process_time"] == 1.0
    assert doc["identity_key"] == "abc123_1"
    assert doc["legacy_group_id"] == "42_abc123_magi"


@pytest.mark.asyncio
async def test_save_insert_sets_identity_key_omits_legacy(mock_db):
    event = MagiBTCTransferEvent(**SAMPLE_EVENT)
    await event.save()
    doc = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({})
    assert doc["identity_key"] == "abc123_1"
    assert "legacy_group_id" not in doc or doc["legacy_group_id"] is None
    assert doc["group_id"] == "abc123_1_magi"


# ---------------------------------------------------------------------------
# load_tracked_object
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_tracked_object_instance_rescan_hits_keeper(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _magi_doc(process_time=1.0, identity_key="abc123_1")
    )
    incoming = MagiBTCTransferEvent(**{**SAMPLE_EVENT, "indexer_id": 99})
    loaded = await load_tracked_object(incoming)
    assert loaded is not None
    assert loaded.indexer_id == 42
    assert loaded.group_id_p == "abc123_1_magi"


@pytest.mark.asyncio
async def test_load_tracked_object_string_legacy_hits_keeper(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _magi_doc(process_time=1.0, identity_key="abc123_1", legacy_group_id="42_abc123_magi")
    )
    loaded = await load_tracked_object("42_abc123_magi")
    assert loaded is not None
    assert loaded.indexer_id == 42


@pytest.mark.asyncio
async def test_load_tracked_object_string_unstored_old_format_is_exact_miss(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(_magi_doc(process_time=1.0))
    loaded = await load_tracked_object("99_abc123_magi")
    assert loaded is None


@pytest.mark.asyncio
async def test_load_tracked_object_skips_quarantined_loser(mock_db):
    keeper = _magi_doc(process_time=1.0, identity_key="abc123_1")
    loser = _magi_doc(
        indexer_id=99,
        identity_key="__dup_abc123_1",
        group_id="__dup_99_abc123_magi",
        legacy_group_id="__dup_99_abc123_magi",
    )
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_many([keeper, loser])
    loaded = await load_tracked_object("42_abc123_magi")
    assert loaded is not None
    assert loaded.indexer_id == 42
    loaded_new = await load_tracked_object(MagiBTCTransferEvent(**{**SAMPLE_EVENT, "indexer_id": 99}))
    assert loaded_new is not None
    assert loaded_new.indexer_id == 42


# ---------------------------------------------------------------------------
# magi-monitor persist no-op
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_persist_watched_noop_when_processed(mock_db, mocker):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _magi_doc(process_time=1.0, identity_key="abc123_1")
    )
    hive = mocker.patch.object(
        MagiBTCTransferEvent, "hive_custom_json", new_callable=AsyncMock
    )
    incoming = MagiBTCTransferEvent(**{**SAMPLE_EVENT, "indexer_id": 99})
    path = await persist_watched_magi_event(incoming)
    assert path in ("keeper_hit_noop", "keeper_hit_backfill")
    hive.assert_not_called()
    count = await mock_db[DB_MAGI_BTC_COLLECTION].count_documents({})
    assert count == 1
    doc = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({})
    assert doc["indexer_id"] == 42
    assert doc["group_id"] == "abc123_1_magi"
    assert doc["legacy_group_id"] == "42_abc123_magi"


# ---------------------------------------------------------------------------
# process_tracked_event: do not double-post
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_tracked_event_rescan_zero_new_ledger(mock_db, noop_locks, mocker):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _magi_doc(process_time=1.0, identity_key="abc123_1")
    )
    await mock_db[LEDGER_COL].insert_one({
        "group_id": "42_abc123_magi_magi_out",
        "ledger_type": "magi_out",
    })
    mocker.patch(
        "v4vapp_backend_v2.process.process_tracked_events.process_magi_btc_transfer_event",
        side_effect=AssertionError("must not re-run magi accounting"),
    )
    incoming = MagiBTCTransferEvent(**{**SAMPLE_EVENT, "indexer_id": 99})
    await process_tracked_event(incoming)
    assert await mock_db[DB_MAGI_BTC_COLLECTION].count_documents({}) == 1
    assert await mock_db[LEDGER_COL].count_documents({}) == 1
    doc = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({})
    assert doc["indexer_id"] == 42
    assert doc["group_id"] == "abc123_1_magi"
    assert doc["legacy_group_id"] == "42_abc123_magi"
    assert doc["process_time"] == 1.0


@pytest.mark.asyncio
async def test_process_tracked_event_op2_not_skipped_by_op1_ledger(mock_db, noop_locks, mocker):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _magi_doc(process_time=1.0, identity_key="abc123_1")
    )
    await mock_db[LEDGER_COL].insert_one({
        "group_id": "42_abc123_magi_magi_out",
        "ledger_type": "magi_out",
    })
    called = []

    async def _mark(*, magi_transfer):
        called.append(magi_transfer.indexer_tx_hash)
        return []

    mocker.patch(
        "v4vapp_backend_v2.process.process_tracked_events.process_magi_btc_transfer_event",
        side_effect=_mark,
    )
    op2 = MagiBTCTransferEvent(**{
        **SAMPLE_EVENT,
        "indexer_id": 43,
        "indexer_tx_hash": "abc123-1",
    })
    await process_tracked_event(op2)
    assert called == ["abc123-1"]
    assert await mock_db[DB_MAGI_BTC_COLLECTION].count_documents({}) == 2
    op1 = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({"indexer_tx_hash": "abc123"})
    assert op1["indexer_id"] == 42
    assert float(op1["amount"]) == 5000.0


@pytest.mark.asyncio
async def test_rescan_of_txid_minus_zero_attaches_to_op1(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _magi_doc(process_time=1.0, identity_key="abc123_1")
    )
    incoming = MagiBTCTransferEvent(**{
        **SAMPLE_EVENT,
        "indexer_id": 99,
        "indexer_tx_hash": "abc123-0",
    })
    await incoming.save()
    assert await mock_db[DB_MAGI_BTC_COLLECTION].count_documents({}) == 1
    doc = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({})
    assert doc["indexer_id"] == 42


# ---------------------------------------------------------------------------
# get_last_indexer_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_find_magi_docs_hive_trx_hits_txid_minus_zero_not_minus_one(mock_db):
    """Hive custom_json.trx_id is unsuffixed → op 1 aliases {txid, txid-0} only."""
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_many([
        _magi_doc(indexer_tx_hash="abc123-0", identity_key="abc123_1", process_time=1.0),
        _magi_doc(
            indexer_id=43,
            indexer_tx_hash="abc123-1",
            identity_key="abc123_2",
            group_id="43_abc123_magi",
        ),
    ])
    docs = await find_magi_docs(indexer_tx_hash="abc123")
    keeper = await select_keeper(docs)
    assert keeper is not None
    assert keeper["indexer_tx_hash"] == "abc123-0"
    assert keeper["identity_key"] == "abc123_1"


@pytest.mark.asyncio
async def test_get_last_indexer_id_unchanged_after_lower_id_save(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _magi_doc(indexer_id=80, process_time=1.0, identity_key="abc123_1")
    )
    incoming = MagiBTCTransferEvent(**{**SAMPLE_EVENT, "indexer_id": 10})
    await incoming.save()
    assert await get_last_indexer_id() == 80
    doc = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({})
    assert doc["indexer_id"] == 80


@pytest.mark.asyncio
async def test_magi_identity_gate_passes_new_format(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _magi_doc(group_id="abc123_1_magi", identity_key="abc123_1", process_time=1.0)
    )
    await mock_db[LEDGER_COL].insert_one({
        "group_id": "abc123_1_magi_magi_out",
        "ledger_type": "magi_out",
    })
    await assert_magi_identity_ready(notify_delay=0)


@pytest.mark.asyncio
async def test_magi_identity_gate_passes_empty_db(mock_db):
    await assert_magi_identity_ready(notify_delay=0)


@pytest.mark.asyncio
async def test_magi_identity_gate_fails_old_magi_btc(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _magi_doc(group_id="42_abc123_magi", identity_key="abc123_1")
    )
    with pytest.raises(MagiIdentityInconsistency, match="old_format_magi_btc=1"):
        await assert_magi_identity_ready(notify_delay=0)


@pytest.mark.asyncio
async def test_magi_identity_gate_fails_hyphen_magi_btc(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _magi_doc(group_id="42-abc123-magi", identity_key="abc123_1")
    )
    with pytest.raises(MagiIdentityInconsistency, match="old_format_magi_btc=1"):
        await assert_magi_identity_ready(notify_delay=0)


@pytest.mark.asyncio
async def test_magi_identity_gate_fails_missing_identity_key(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _magi_doc(group_id="abc123_1_magi")
    )
    with pytest.raises(MagiIdentityInconsistency, match="missing_identity_key=1"):
        await assert_magi_identity_ready(notify_delay=0)


@pytest.mark.asyncio
async def test_magi_identity_gate_fails_old_magi_ledger(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _magi_doc(group_id="abc123_1_magi", identity_key="abc123_1")
    )
    await mock_db[LEDGER_COL].insert_one({
        "group_id": "42_abc123_magi_magi_out",
        "ledger_type": "magi_out",
    })
    with pytest.raises(MagiIdentityInconsistency, match="old_format_MAGI_ledger=1"):
        await assert_magi_identity_ready(notify_delay=0)


@pytest.mark.asyncio
async def test_magi_identity_gate_ignores_quarantined_dup(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _magi_doc(
            group_id="abc123_1_magi",
            identity_key="abc123_1",
            process_time=1.0,
        )
    )
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _magi_doc(
            indexer_id=99,
            group_id="99_abc123_magi",
            identity_key="__dup_abc123_1",
        )
    )
    await assert_magi_identity_ready(notify_delay=0)
