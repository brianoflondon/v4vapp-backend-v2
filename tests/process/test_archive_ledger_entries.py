"""
Tests for archive_old_hold_release_keepsats_entries().

These tests use AsyncMock/MagicMock to patch the collection methods so that
no real MongoDB connection is required, and to work around the fact that
mongomock-motor does not support the ``$merge`` aggregation stage.

Coverage:
  - No matching entries (count == 0): function returns 0 immediately.
  - Odd HOLD/RELEASE pair count (even-pair guard): function returns 0 without
    calling aggregate or delete_many.
  - Successful forward archive: aggregate is invoked with a pipeline containing
    ``$merge`` into ``archived_ledger``, then delete_many cleans up originals.
  - reverse_archive=True (restore): aggregate merges from archived_ledger back
    into ledger; delete_many is NOT called.
  - Aggregate exception: function handles the error and returns 0 without
    calling delete_many.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.process.hold_release_keepsats import (
    archive_old_hold_release_keepsats_entries,
)


@pytest.fixture(autouse=True)
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


def _make_mock_collection(total_count: int = 0, ledger_type_count: int = 0) -> MagicMock:
    """Return a mock async collection with configurable count results."""
    col = MagicMock()

    # count_documents is called twice: first for total count, then for ledger_type count
    col.count_documents = AsyncMock(side_effect=[total_count, ledger_type_count])

    # aggregate returns a cursor-like object; close() is synchronous in the current
    # implementation (called without await).
    cursor = MagicMock()
    cursor.close = MagicMock()
    col.aggregate = MagicMock(return_value=cursor)

    delete_result = MagicMock()
    delete_result.deleted_count = total_count
    col.delete_many = AsyncMock(return_value=delete_result)

    return col


@pytest.mark.asyncio
async def test_archive_returns_zero_when_no_entries():
    """When no matching entries exist the function returns 0 without touching the DB."""
    mock_col = _make_mock_collection(total_count=0, ledger_type_count=0)

    with patch.object(LedgerEntry, "collection", return_value=mock_col):
        result = await archive_old_hold_release_keepsats_entries(older_than_days=30)

    assert result == 0
    mock_col.aggregate.assert_not_called()
    mock_col.delete_many.assert_not_called()


@pytest.mark.asyncio
async def test_archive_aborts_on_odd_ledger_type_count():
    """When HOLD/RELEASE count is odd the even-pair guard fires and returns 0."""
    # total_count > 0 so we get past the first check, but ledger_type_count is odd
    mock_col = _make_mock_collection(total_count=3, ledger_type_count=3)

    with patch.object(LedgerEntry, "collection", return_value=mock_col):
        result = await archive_old_hold_release_keepsats_entries(older_than_days=30)

    assert result == 0
    mock_col.aggregate.assert_not_called()
    mock_col.delete_many.assert_not_called()


@pytest.mark.asyncio
async def test_archive_successful_flow():
    """
    Normal forward archive:
    - aggregate is called with a ``$merge`` pipeline targeting ``archived_ledger``
    - delete_many is called afterwards to remove the originals
    """
    total_count = 4
    ledger_type_count = 4  # even → guard passes
    mock_col = _make_mock_collection(total_count=total_count, ledger_type_count=ledger_type_count)

    with patch.object(LedgerEntry, "collection", return_value=mock_col):
        result = await archive_old_hold_release_keepsats_entries(older_than_days=30)

    assert result == total_count

    # Verify aggregate was called once and the pipeline contains a $merge stage
    mock_col.aggregate.assert_called_once()
    call_kwargs = mock_col.aggregate.call_args.kwargs
    pipeline_arg = call_kwargs.get("pipeline") or mock_col.aggregate.call_args.args[0]
    merge_stages = [stage for stage in pipeline_arg if "$merge" in stage]
    assert len(merge_stages) == 1, "Pipeline must contain exactly one $merge stage"
    assert merge_stages[0]["$merge"]["into"] == LedgerEntry.archived_collection_name()

    # Verify delete_many was called to clean up originals
    mock_col.delete_many.assert_called_once()


@pytest.mark.asyncio
async def test_reverse_archive_no_delete():
    """
    Restore path (reverse_archive=True):
    - aggregate is called using the archived collection as source
    - the ``$merge`` target is the main ledger collection
    - delete_many is NOT called (restore is non-destructive)
    """
    total_count = 2
    ledger_type_count = 2
    mock_archived_col = _make_mock_collection(
        total_count=total_count, ledger_type_count=ledger_type_count
    )

    with patch.object(LedgerEntry, "archived_collection", return_value=mock_archived_col):
        result = await archive_old_hold_release_keepsats_entries(
            older_than_days=30, reverse_archive=True
        )

    assert result == total_count

    # Verify aggregate was called once and the pipeline targets the main ledger
    mock_archived_col.aggregate.assert_called_once()
    call_kwargs = mock_archived_col.aggregate.call_args.kwargs
    pipeline_arg = (
        call_kwargs.get("pipeline") or mock_archived_col.aggregate.call_args.args[0]
    )
    merge_stages = [stage for stage in pipeline_arg if "$merge" in stage]
    assert len(merge_stages) == 1
    assert merge_stages[0]["$merge"]["into"] == LedgerEntry.collection_name()

    # Restore must NOT delete from the archive
    mock_archived_col.delete_many.assert_not_called()


@pytest.mark.asyncio
async def test_archive_aggregate_exception_returns_zero():
    """If the aggregate call raises an exception the function returns 0 without deleting."""
    total_count = 2
    ledger_type_count = 2
    mock_col = _make_mock_collection(total_count=total_count, ledger_type_count=ledger_type_count)
    mock_col.aggregate.side_effect = Exception("simulated aggregate failure")

    with patch.object(LedgerEntry, "collection", return_value=mock_col):
        result = await archive_old_hold_release_keepsats_entries(older_than_days=30)

    assert result == 0
    mock_col.delete_many.assert_not_called()


@pytest.mark.asyncio
async def test_archive_uses_threshold_date_filter():
    """
    The match filter must use ``$lt threshold_date`` so that only entries
    *older* than the requested number of days are selected.
    """
    total_count = 6
    ledger_type_count = 6
    mock_col = _make_mock_collection(total_count=total_count, ledger_type_count=ledger_type_count)

    before = datetime.now(tz=timezone.utc) - timedelta(days=30)

    with patch.object(LedgerEntry, "collection", return_value=mock_col):
        await archive_old_hold_release_keepsats_entries(older_than_days=30)

    after = datetime.now(tz=timezone.utc) - timedelta(days=30)

    # Inspect the filter passed to the first count_documents call
    first_call_args = mock_col.count_documents.call_args_list[0]
    match_filter = first_call_args[0][0]
    threshold = match_filter["timestamp"]["$lt"]
    # The threshold should be between `before` and `after` (i.e. roughly 30 days ago)
    assert before <= threshold <= after
