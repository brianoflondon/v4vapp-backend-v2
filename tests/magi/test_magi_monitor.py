"""Tests for magi_monitor helper functions.

Covers:
- get_last_good_block_magi  — mirrors get_last_good_block from hive_monitor
- get_indexer_id_for_block  — translates a Hive block number to a MAGI indexer_id
"""

# Import via sys-path manipulation is not required; the conftest autouse
# fixture sets up the test config paths and InternalConfig singleton.
# We import the functions directly from magi_monitor (a top-level src module).
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure src/ is on sys.path for the magi_monitor module
_SRC = Path(__file__).resolve().parent.parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from magi_monitor import get_indexer_id_for_block, get_last_good_block_magi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAMPLE_DOC = {
    "indexer_id": 42,
    "indexer_block_height": 105000000,
    "indexer_ts": "2026-04-25T09:00:00",
}


def _mock_db_collection(find_one_return):
    """Return a fake InternalConfig.db[...] object whose find_one resolves to *find_one_return*."""
    collection = MagicMock()
    collection.find_one = AsyncMock(return_value=find_one_return)
    fake_db = MagicMock()
    fake_db.__getitem__ = MagicMock(return_value=collection)
    return fake_db, collection


# ---------------------------------------------------------------------------
# get_last_good_block_magi
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_last_good_block_magi_returns_indexer_id():
    """Returns the indexer_id from the latest document in the collection."""
    fake_db, collection = _mock_db_collection(_SAMPLE_DOC)
    with patch("magi_monitor.InternalConfig") as MockIC:
        MockIC.db = fake_db
        result = await get_last_good_block_magi()

    collection.find_one.assert_awaited_once_with(filter={}, sort=[("indexer_id", -1)])
    assert result == 42


@pytest.mark.asyncio
async def test_get_last_good_block_magi_empty_db_returns_zero():
    """Returns 0 when the collection is empty."""
    fake_db, _ = _mock_db_collection(None)
    with patch("magi_monitor.InternalConfig") as MockIC:
        MockIC.db = fake_db
        result = await get_last_good_block_magi()

    assert result == 0


@pytest.mark.asyncio
async def test_get_last_good_block_magi_exception_returns_zero():
    """Returns 0 gracefully when the DB raises an exception."""
    collection = MagicMock()
    collection.find_one = AsyncMock(side_effect=RuntimeError("db down"))
    fake_db = MagicMock()
    fake_db.__getitem__ = MagicMock(return_value=collection)

    with patch("magi_monitor.InternalConfig") as MockIC:
        MockIC.db = fake_db
        result = await get_last_good_block_magi()

    assert result == 0


# ---------------------------------------------------------------------------
# get_indexer_id_for_block
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_indexer_id_for_block_found():
    """Returns the indexer_id of the last event at or before the given block."""
    fake_db, collection = _mock_db_collection(_SAMPLE_DOC)
    with patch("magi_monitor.InternalConfig") as MockIC:
        MockIC.db = fake_db
        result = await get_indexer_id_for_block(105000000)

    collection.find_one.assert_awaited_once_with(
        filter={"indexer_block_height": {"$lte": 105000000}},
        sort=[("indexer_id", -1)],
    )
    assert result == 42


@pytest.mark.asyncio
async def test_get_indexer_id_for_block_not_found_returns_zero():
    """Returns 0 when no events exist at or before the given block."""
    fake_db, _ = _mock_db_collection(None)
    with patch("magi_monitor.InternalConfig") as MockIC:
        MockIC.db = fake_db
        result = await get_indexer_id_for_block(100)

    assert result == 0


@pytest.mark.asyncio
async def test_get_indexer_id_for_block_exception_returns_zero():
    """Returns 0 gracefully when the DB raises an exception."""
    collection = MagicMock()
    collection.find_one = AsyncMock(side_effect=RuntimeError("db down"))
    fake_db = MagicMock()
    fake_db.__getitem__ = MagicMock(return_value=collection)

    with patch("magi_monitor.InternalConfig") as MockIC:
        MockIC.db = fake_db
        result = await get_indexer_id_for_block(105000000)

    assert result == 0
