"""
Tests for ErrorCodeManager class.

Tests the in-memory dict-like interface and MongoDB persistence behavior.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from v4vapp_backend_v2.config.error_code_class import ErrorCode
from v4vapp_backend_v2.config.error_code_manager import ErrorCodeManager


@pytest.fixture(autouse=True)
def reset_manager_singleton(monkeypatch: pytest.MonkeyPatch):
    """Reset the ErrorCodeManager singleton before and after each test."""
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.error_code_manager.ErrorCodeManager._instance", None
    )
    yield
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.error_code_manager.ErrorCodeManager._instance", None
    )


class TestErrorCodeManagerBasic:
    """Test basic dict-like interface."""

    def test_singleton_pattern(self):
        """Test that ErrorCodeManager is a singleton."""
        manager1 = ErrorCodeManager(db_enabled=False)
        manager2 = ErrorCodeManager(db_enabled=False)
        assert manager1 is manager2

    def test_add_and_contains(self):
        """Test adding an error code and checking containment."""
        manager = ErrorCodeManager(db_enabled=False)
        error = ErrorCode(code="E001", message="Test error")
        manager.add(error)
        assert "E001" in manager
        assert "E999" not in manager

    def test_getitem(self):
        """Test getting an error code by key."""
        manager = ErrorCodeManager(db_enabled=False)
        error = ErrorCode(code="E002", message="Test error 2")
        manager.add(error)
        retrieved = manager["E002"]
        assert retrieved.code == "E002"
        assert retrieved.message == "Test error 2"

    def test_getitem_raises_keyerror(self):
        """Test that getting a non-existent key raises KeyError."""
        manager = ErrorCodeManager(db_enabled=False)
        with pytest.raises(KeyError):
            _ = manager["nonexistent"]

    def test_setitem(self):
        """Test setting an error code via dict syntax."""
        manager = ErrorCodeManager(db_enabled=False)
        error = ErrorCode(code="E003", message="Test error 3")
        manager["E003"] = error
        assert "E003" in manager

    def test_delitem(self):
        """Test deleting an error code via dict syntax."""
        manager = ErrorCodeManager(db_enabled=False)
        error = ErrorCode(code="E004", message="Test error 4")
        manager.add(error)
        del manager["E004"]
        assert "E004" not in manager

    def test_get_with_default(self):
        """Test get method with default value."""
        manager = ErrorCodeManager(db_enabled=False)
        result = manager.get("nonexistent", None)
        assert result is None

        error = ErrorCode(code="E005", message="Test")
        manager.add(error)
        result = manager.get("E005")
        assert result is not None
        assert result.code == "E005"

    def test_pop(self):
        """Test pop method removes and returns error code."""
        manager = ErrorCodeManager(db_enabled=False)
        error = ErrorCode(code="E006", message="Test error 6")
        manager.add(error)

        popped = manager.pop("E006")
        assert popped is not None
        assert popped.code == "E006"
        assert "E006" not in manager

    def test_pop_with_default(self):
        """Test pop with default value for missing key."""
        manager = ErrorCodeManager(db_enabled=False)
        result = manager.pop("nonexistent", None)
        assert result is None

    def test_pop_raises_keyerror(self):
        """Test pop raises KeyError when no default provided."""
        manager = ErrorCodeManager(db_enabled=False)
        with pytest.raises(KeyError):
            manager.pop("nonexistent")

    def test_clear(self):
        """Test clearing all error codes."""
        manager = ErrorCodeManager(db_enabled=False)
        manager.add(ErrorCode(code="E007", message="Test 7"))
        manager.add(ErrorCode(code="E008", message="Test 8"))
        assert len(manager) == 2

        manager.clear()
        assert len(manager) == 0

    def test_len(self):
        """Test length of manager."""
        manager = ErrorCodeManager(db_enabled=False)
        assert len(manager) == 0

        manager.add(ErrorCode(code="E009", message="Test"))
        assert len(manager) == 1

        manager.add(ErrorCode(code="E010", message="Test"))
        assert len(manager) == 2

    def test_iter(self):
        """Test iteration over error codes."""
        manager = ErrorCodeManager(db_enabled=False)
        manager.add(ErrorCode(code="E011", message="Test"))
        manager.add(ErrorCode(code="E012", message="Test"))

        codes = list(manager)
        assert "E011" in codes
        assert "E012" in codes

    def test_items(self):
        """Test items method."""
        manager = ErrorCodeManager(db_enabled=False)
        manager.add(ErrorCode(code="E013", message="Test"))

        items = manager.items()
        assert len(items) == 1
        assert items[0][0] == "E013"

    def test_keys(self):
        """Test keys method."""
        manager = ErrorCodeManager(db_enabled=False)
        manager.add(ErrorCode(code="E014", message="Test"))
        manager.add(ErrorCode(code="E015", message="Test"))

        keys = manager.keys()
        assert "E014" in keys
        assert "E015" in keys

    def test_values(self):
        """Test values method."""
        manager = ErrorCodeManager(db_enabled=False)
        manager.add(ErrorCode(code="E016", message="Test 16"))

        values = manager.values()
        assert len(values) == 1
        assert values[0].code == "E016"

    def test_to_dict(self):
        """Test to_dict method."""
        manager = ErrorCodeManager(db_enabled=False)
        manager.add(ErrorCode(code="E017", message="Test 17"))

        result = manager.to_dict()
        assert "E017" in result
        assert result["E017"]["code"] == "E017"
        assert result["E017"]["message"] == "Test 17"


class TestErrorCodeManagerConfiguration:
    """Test configuration methods."""

    def test_configure(self):
        """Test configure method updates settings."""
        manager = ErrorCodeManager(db_enabled=False)
        manager.configure(server_id="test-server", node_name="test-node", db_enabled=True)

        assert manager._server_id == "test-server"
        assert manager._node_name == "test-node"
        assert manager._db_enabled is True

    def test_configure_partial(self):
        """Test configure with partial updates."""
        manager = ErrorCodeManager(server_id="original", node_name="original", db_enabled=False)
        manager.configure(server_id="new-server")

        assert manager._server_id == "new-server"
        assert manager._node_name == "original"


class TestErrorCodeClass:
    """Test ErrorCode class additions."""

    def test_to_mongo_doc(self):
        """Test ErrorCode.to_mongo_doc creates proper document."""
        error = ErrorCode(code="E100", message="Test error message")
        doc = error.to_mongo_doc(
            server_id="test-server",
            node_name="test-node",
            active=True,
            cleared_at=None,
        )

        assert doc["code"] == "E100"
        assert doc["message"] == "Test error message"
        assert doc["server_id"] == "test-server"
        assert doc["node_name"] == "test-node"
        assert doc["active"] is True
        assert doc["cleared_at"] is None
        assert "start_time" in doc
        assert "last_log_time" in doc
        assert "created_at" in doc
        assert "updated_at" in doc

    def test_to_mongo_doc_cleared(self):
        """Test to_mongo_doc with cleared error."""
        error = ErrorCode(code="E101", message="Cleared error")
        cleared_time = datetime.now(tz=timezone.utc)
        doc = error.to_mongo_doc(
            server_id="server",
            node_name="node",
            active=False,
            cleared_at=cleared_time,
        )

        assert doc["active"] is False
        assert doc["cleared_at"] == cleared_time

    def test_from_mongo_doc(self):
        """Test ErrorCode.from_mongo_doc restores ErrorCode."""
        now = datetime.now(tz=timezone.utc)
        doc = {
            "code": "E102",
            "message": "Restored error",
            "start_time": now - timedelta(hours=1),
            "last_log_time": now,
        }

        error = ErrorCode.from_mongo_doc(doc)
        assert error.code == "E102"
        assert error.message == "Restored error"
        assert error.start_time == now - timedelta(hours=1)
        assert error.last_log_time == now

    def test_from_mongo_doc_minimal(self):
        """Test from_mongo_doc with minimal document."""
        doc = {"code": "E103"}

        error = ErrorCode.from_mongo_doc(doc)
        assert error.code == "E103"
        assert error.message == ""


class TestErrorCodeManagerPersistence:
    """Test MongoDB persistence (mocked)."""

    @pytest.mark.asyncio
    async def test_async_persist_add(self):
        """Test async persistence of new error codes."""
        manager = ErrorCodeManager(db_enabled=True, server_id="test", node_name="node")

        # Mock the database
        mock_collection = AsyncMock()
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)

        with patch("v4vapp_backend_v2.config.setup.InternalConfig.db", mock_db):
            error = ErrorCode(code="E200", message="Persist test")
            await manager._async_persist_add(error)

            # Verify insert_one was called
            mock_collection.insert_one.assert_called_once()
            call_args = mock_collection.insert_one.call_args[0][0]
            assert call_args["code"] == "E200"
            assert call_args["active"] is True

    @pytest.mark.asyncio
    async def test_async_persist_clear(self):
        """Test async persistence of error clear events."""
        manager = ErrorCodeManager(db_enabled=True, server_id="test", node_name="node")

        mock_collection = AsyncMock()
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)

        with patch("v4vapp_backend_v2.config.setup.InternalConfig.db", mock_db):
            error = ErrorCode(code="E201", message="Clear test")
            await manager._async_persist_clear(error)

            mock_collection.insert_one.assert_called_once()
            call_args = mock_collection.insert_one.call_args[0][0]
            assert call_args["code"] == "E201"
            assert call_args["active"] is False
            assert call_args["cleared_at"] is not None

    @pytest.mark.asyncio
    async def test_get_error_history(self):
        """Test querying error history."""
        manager = ErrorCodeManager(db_enabled=True)

        mock_cursor = AsyncMock()
        mock_cursor.to_list = AsyncMock(return_value=[{"code": "E300", "active": True}])

        mock_collection = MagicMock()
        mock_collection.find = MagicMock(return_value=mock_cursor)
        mock_cursor.sort = MagicMock(return_value=mock_cursor)
        mock_cursor.limit = MagicMock(return_value=mock_cursor)

        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)

        with patch("v4vapp_backend_v2.config.setup.InternalConfig.db", mock_db):
            result = await manager.get_error_history(code="E300", limit=10)

            assert len(result) == 1
            assert result[0]["code"] == "E300"

    def test_persist_disabled(self):
        """Test that persistence is skipped when db_enabled=False."""
        manager = ErrorCodeManager(db_enabled=False)

        # This should not raise any errors even without mocking
        error = ErrorCode(code="E400", message="No persist")
        manager._persist_add(error)  # Should silently skip
        manager._persist_clear(error)  # Should silently skip
