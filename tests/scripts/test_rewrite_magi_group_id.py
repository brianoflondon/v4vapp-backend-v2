"""Magi identity-backfill (PR1) and group_id rewrite (PR3)."""

import pytest
from mongomock_motor import AsyncMongoMockClient
from pymongo.errors import DuplicateKeyError, OperationFailure

from scripts.rewrite_magi_group_id import (
    INCIDENT_TRX,
    apply_group_id_rewrite,
    apply_identity_backfill,
    plan_group_id_rewrite,
    plan_identity_backfill,
    rollback_group_id_rewrite,
    run_identity_backfill,
    scan_identity_groups,
    verify_group_id_rewrite,
)
from tests.magi.test_stream_magi import SAMPLE_EVENT
from v4vapp_backend_v2.actions.tracked_any import load_tracked_object
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.magi.magi_classes import _DUP_PREFIX, DB_MAGI_BTC_COLLECTION

LEDGER_COL = "ledger"


@pytest.fixture
def mock_db(mocker):
    client = AsyncMongoMockClient()
    db = client["test_db"]
    mocker.patch.object(InternalConfig, "db", db)
    return db


def _doc(**overrides) -> dict:
    base = {
        **SAMPLE_EVENT,
        "group_id": f"{SAMPLE_EVENT['indexer_id']}_abc123_magi",
        "amount": 5000,
    }
    base.update(overrides)
    return base


@pytest.mark.asyncio
async def test_identity_backfill_dry_run_does_not_write(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(_doc())
    plan = await run_identity_backfill(
        commit=False, limit=None, report_path=None, create_index=False
    )
    assert plan["dry_run"] is True
    assert plan["counts"]["missing_identity_key"] == 1
    doc = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({})
    assert "identity_key" not in doc
    assert "legacy_group_id" not in doc


@pytest.mark.asyncio
async def test_identity_backfill_commit_sets_key_and_legacy(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(_doc(process_time=1.0))
    plan = await run_identity_backfill(
        commit=True, limit=None, report_path=None, create_index=True
    )
    assert plan["apply_result"]["index_created"] is True
    doc = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({})
    assert doc["identity_key"] == "abc123_1"
    assert doc["legacy_group_id"] == "42_abc123_magi"
    assert doc["group_id"] == "42_abc123_magi"
    assert doc["indexer_id"] == 42


@pytest.mark.asyncio
async def test_unique_index_fails_before_backfill_succeeds_after(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_many([
        _doc(),
        _doc(indexer_id=7, indexer_tx_hash="zzz999", group_id="7_zzz999_magi"),
    ])
    with pytest.raises((DuplicateKeyError, OperationFailure)):
        await mock_db[DB_MAGI_BTC_COLLECTION].create_index("identity_key", unique=True)

    await run_identity_backfill(commit=True, limit=None, report_path=None, create_index=True)
    # Second create_index with same name is ok (idempotent) or already exists.
    count = await mock_db[DB_MAGI_BTC_COLLECTION].count_documents({
        "identity_key": {"$exists": True}
    })
    assert count == 2


@pytest.mark.asyncio
async def test_identity_backfill_quarantines_same_key_duplicate(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_many([
        _doc(process_time=1.0, indexer_id=42),
        _doc(indexer_id=99, group_id="99_abc123_magi"),
    ])
    groups = await scan_identity_groups()
    plan = await plan_identity_backfill(groups)
    assert plan["counts"]["quarantine"] == 1
    await apply_identity_backfill(plan, create_index=True)
    keeper = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({"indexer_id": 42})
    loser = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({"indexer_id": 99})
    assert keeper["identity_key"] == "abc123_1"
    assert keeper["group_id"] == "42_abc123_magi"
    assert str(loser["identity_key"]).startswith(_DUP_PREFIX)
    assert _DUP_PREFIX in str(loser["group_id"])
    assert str(loser["legacy_group_id"]).startswith(_DUP_PREFIX)
    assert loser.get("duplicate_of") == keeper["_id"]


@pytest.mark.asyncio
async def test_rewrite_dry_run_does_not_write(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(_doc(process_time=1.0))
    await mock_db[LEDGER_COL].insert_one({
        "group_id": "42_abc123_magi_magi_out",
        "ledger_type": "magi_out",
    })
    plan = await plan_group_id_rewrite()
    assert plan["counts"]["magi_renames"] == 1
    assert plan["counts"]["ledger_renames"] == 1
    doc = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({})
    assert doc["group_id"] == "42_abc123_magi"
    led = await mock_db[LEDGER_COL].find_one({})
    assert led["group_id"] == "42_abc123_magi_magi_out"


@pytest.mark.asyncio
async def test_apply_rewrites_magi_and_ledger_count_unchanged(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _doc(
            process_time=1.0,
            indexer_id=802,
            indexer_tx_hash=INCIDENT_TRX,
            group_id=f"802_{INCIDENT_TRX}_magi",
        )
    )
    await mock_db[LEDGER_COL].insert_one({
        "group_id": f"802_{INCIDENT_TRX}_magi_magi_out",
        "ledger_type": "magi_out",
    })
    await mock_db[LEDGER_COL].insert_one({
        "group_id": f"802_{INCIDENT_TRX}_magi_fee_inc",
        "ledger_type": "fee_inc",
    })
    plan = await plan_group_id_rewrite()
    assert plan["counts"]["list_b"] == 0
    plan = await apply_group_id_rewrite(plan)
    plan = await verify_group_id_rewrite(plan)
    assert plan["verify_result"]["counts_unchanged"] is True
    assert plan["verify_result"]["ok"] is True
    magi = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({})
    assert magi["group_id"] == f"{INCIDENT_TRX}_1_magi"
    assert magi["legacy_group_id"] == f"802_{INCIDENT_TRX}_magi"
    assert magi["indexer_id"] == 802
    assert magi["identity_key"] == f"{INCIDENT_TRX}_1"
    assert await mock_db[LEDGER_COL].count_documents({"ledger_type": "magi_out"}) == 1
    assert await mock_db[LEDGER_COL].find_one({
        "group_id": f"{INCIDENT_TRX}_1_magi_magi_out"
    })


@pytest.mark.asyncio
async def test_apply_skips_duplicate_outbound_books(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _doc(process_time=1.0, indexer_id=802, group_id="802_abc123_magi")
    )
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(
        _doc(indexer_id=807, group_id="807_abc123_magi")
    )
    await mock_db[LEDGER_COL].insert_one({
        "group_id": "802_abc123_magi_magi_out",
        "ledger_type": "magi_out",
    })
    await mock_db[LEDGER_COL].insert_one({
        "group_id": "807_abc123_magi_magi_out",
        "ledger_type": "magi_out",
    })
    plan = await plan_group_id_rewrite()
    assert plan["counts"]["list_b"] == 1
    plan = await apply_group_id_rewrite(plan)
    assert await mock_db[LEDGER_COL].count_documents({"ledger_type": "magi_out"}) == 2
    assert await mock_db[LEDGER_COL].find_one({"group_id": "802_abc123_magi_magi_out"})
    assert await mock_db[LEDGER_COL].find_one({"group_id": "807_abc123_magi_magi_out"})
    assert not await mock_db[LEDGER_COL].find_one({"group_id": "abc123_1_magi_magi_out"})
    keeper = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({"indexer_id": 802})
    assert keeper["group_id"] == "802_abc123_magi"


@pytest.mark.asyncio
async def test_apply_quarantine_not_returned_by_load(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_many([
        _doc(process_time=1.0, indexer_id=42),
        _doc(indexer_id=99, group_id="99_abc123_magi"),
    ])
    await mock_db[LEDGER_COL].insert_one({
        "group_id": "42_abc123_magi_magi_out",
        "ledger_type": "magi_out",
    })
    plan = await plan_group_id_rewrite()
    plan = await apply_group_id_rewrite(plan)
    loaded_legacy = await load_tracked_object("42_abc123_magi")
    loaded_new = await load_tracked_object("abc123_1_magi")
    assert loaded_legacy is not None
    assert loaded_new is not None
    assert loaded_legacy.indexer_id == 42
    assert loaded_new.indexer_id == 42


@pytest.mark.asyncio
async def test_rollback_restores_old_group_id(mock_db):
    await mock_db[DB_MAGI_BTC_COLLECTION].insert_one(_doc(process_time=1.0))
    await mock_db[LEDGER_COL].insert_one({
        "group_id": "42_abc123_magi_magi_out",
        "ledger_type": "magi_out",
    })
    plan = await plan_group_id_rewrite()
    plan = await apply_group_id_rewrite(plan)
    magi = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({})
    assert magi["group_id"] == "abc123_1_magi"
    plan = await rollback_group_id_rewrite(plan)
    magi = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({})
    led = await mock_db[LEDGER_COL].find_one({})
    assert magi["group_id"] == "42_abc123_magi"
    assert magi["identity_key"] == "abc123_1"
    assert magi["legacy_group_id"] == "42_abc123_magi"
    assert led["group_id"] == "42_abc123_magi_magi_out"
