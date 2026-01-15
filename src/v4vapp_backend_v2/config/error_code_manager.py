"""
ErrorCodeManager - Manages error codes with in-memory tracking and MongoDB persistence.

This module provides a singleton class that:
1. Maintains fast in-memory error code lookups (existing behavior)
2. Persists error events to MongoDB asynchronously for historical tracking
3. Provides dict-like interface for backward compatibility
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, ClassVar, Iterator

from v4vapp_backend_v2.config.error_code_class import ErrorCode

# Use module-level logger to avoid circular import with setup.py
logger = logging.getLogger(__name__)

ERROR_CODE_ICON = "🔴"
ERROR_CODE_CLEAR_ICON = "✅"

# Collection name for error codes in MongoDB
ERROR_CODES_COLLECTION = "error_codes"


class ErrorCodeManager:
    """
    Singleton class that manages error codes with in-memory tracking and MongoDB persistence.

    The manager maintains a dict-like interface for backward compatibility while
    adding async MongoDB persistence for error tracking history.

    Usage:
        manager = ErrorCodeManager()
        manager.add(ErrorCode(code="E123", message="Something failed"))
        if "E123" in manager:
            error = manager.get("E123")
        manager.remove("E123")
    """

    _instance: ClassVar["ErrorCodeManager | None"] = None
    _codes: dict[Any, ErrorCode]
    _server_id: str
    _node_name: str
    _local_machine_name: str
    _db_enabled: bool
    _initialized: bool = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ErrorCodeManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(
        self,
        server_id: str = "",
        node_name: str = "",
        local_machine_name: str = "",
        db_enabled: bool = True,
    ):
        """
        Initialize the ErrorCodeManager.

        Args:
            server_id: Server identifier for MongoDB documents
            node_name: Node name for MongoDB documents
            local_machine_name: Local machine name for MongoDB documents
            db_enabled: Whether to persist to MongoDB (can be disabled for testing)
        """
        if not self._initialized:
            self._codes = {}
            self._server_id = server_id
            self._node_name = node_name
            self._local_machine_name = local_machine_name
            self._db_enabled = db_enabled
            self._initialized = True

    def configure(
        self,
        server_id: str = "",
        node_name: str = "",
        local_machine_name: str = "",
        db_enabled: bool = True,
    ) -> None:
        """
        Configure the manager after initialization.
        Useful when InternalConfig isn't fully initialized yet.

        Args:
            server_id: Server identifier for MongoDB documents
            node_name: Node name for MongoDB documents
            local_machine_name: Local machine name for MongoDB documents
            db_enabled: Whether to persist to MongoDB
        """
        if server_id:
            self._server_id = server_id
        if node_name:
            self._node_name = node_name
        if local_machine_name:
            self._local_machine_name = local_machine_name
        self._db_enabled = db_enabled

    # MARK: Dict-like interface for backward compatibility

    def __contains__(self, code: Any) -> bool:
        """Check if an error code exists in the manager."""
        return code in self._codes

    def __getitem__(self, code: Any) -> ErrorCode:
        """Get an error code by key (raises KeyError if not found)."""
        return self._codes[code]

    def __setitem__(self, code: Any, error_code: ErrorCode) -> None:
        """Set an error code (also triggers async persistence)."""
        self.add(error_code)

    def __delitem__(self, code: Any) -> None:
        """Remove an error code by key."""
        self.remove(code)

    def __iter__(self) -> Iterator[Any]:
        """Iterate over error codes."""
        return iter(self._codes)

    def __len__(self) -> int:
        """Return the number of active error codes."""
        return len(self._codes)

    def get(self, code: Any, default: ErrorCode | None = None) -> ErrorCode | None:
        """Get an error code, returning default if not found."""
        return self._codes.get(code, default)

    def items(self) -> list[tuple[Any, ErrorCode]]:
        """Return all error code items as a list of tuples."""
        return list(self._codes.items())

    def keys(self) -> list[Any]:
        """Return all error code keys."""
        return list(self._codes.keys())

    def values(self) -> list[ErrorCode]:
        """Return all error code values."""
        return list(self._codes.values())

    def pop(self, code: Any, *args, **kwargs) -> ErrorCode | None:
        """Remove and return an error code (also triggers async persistence)."""
        clear_message = ""
        if "clear_message" in kwargs:
            clear_message = kwargs.pop("clear_message")
            logger.info(
                f"{ERROR_CODE_CLEAR_ICON} {clear_message}",
                extra={"notification": True, "error_code_obj": self._codes.get(code)},
            )
        if code in self._codes:
            error_code = self._codes.pop(code)
            self._persist_clear(error_code, clear_message=clear_message)
            return error_code
        if args:
            return args[0]
        raise KeyError(code)

    def clear(self) -> None:
        """Clear all error codes from memory (does not persist clears to DB)."""
        self._codes.clear()

    # MARK: Core methods

    def add(self, error_code: ErrorCode) -> None:
        """
        Add an error code to the manager and persist to MongoDB.

        Args:
            error_code: The ErrorCode instance to add
        """
        self._codes[error_code.code] = error_code
        self._persist_add(error_code)

    def remove(self, code: Any, clear_message: str = "") -> ErrorCode | None:
        """
        Remove an error code from the manager and persist the clear event to MongoDB.

        Args:
            code: The error code to remove
            clear_message: Message describing why/how the error was cleared

        Returns:
            The removed ErrorCode or None if not found
        """
        if code in self._codes:
            error_code = self._codes.pop(code)
            self._persist_clear(error_code, clear_message=clear_message)
            return error_code
        return None

    def to_dict(self) -> dict[Any, dict[str, Any]]:
        """
        Convert all error codes to a dictionary of dictionaries.

        Returns:
            dict: A dictionary where each key is an error code and
            each value is a dictionary representation of the ErrorCode.
        """
        return {code: error_code.to_dict() for code, error_code in self._codes.items()}

    # MARK: MongoDB persistence (async, fire-and-forget)

    def _persist_add(self, error_code: ErrorCode) -> None:
        """
        Persist a new/updated error code to MongoDB asynchronously.

        This method fires and forgets - MongoDB failures won't break logging.
        Only persists when called from the same event loop where the MongoDB
        client was created to avoid "different event loop" errors.
        """
        if not self._db_enabled:
            return

        try:
            # Only use the current running loop - don't try to use notification_loop
            # because InternalConfig.db is bound to the main event loop
            loop = asyncio.get_running_loop()
            loop.create_task(self._async_persist_add(error_code))
        except RuntimeError:
            # No running loop - skip persistence (common during startup/shutdown)
            logger.debug(
                f"{ERROR_CODE_ICON} No running event loop for error code persistence: "
                f"{error_code.code}"
            )
        except Exception as e:
            # Never let persistence errors break the logging system
            logger.debug(f"{ERROR_CODE_ICON} Error scheduling persistence: {e}")

    def _persist_clear(self, error_code: ErrorCode, clear_message: str = "") -> None:
        """
        Persist an error code clear event to MongoDB asynchronously.

        This method fires and forgets - MongoDB failures won't break logging.
        Only persists when called from the same event loop where the MongoDB
        client was created to avoid "different event loop" errors.

        Args:
            error_code: The ErrorCode being cleared
            clear_message: Message describing why/how the error was cleared
        """
        if not self._db_enabled:
            return

        try:
            # Only use the current running loop - don't try to use notification_loop
            loop = asyncio.get_running_loop()
            loop.create_task(self._async_persist_clear(error_code, clear_message=clear_message))
        except RuntimeError:
            logger.debug(
                f"{ERROR_CODE_CLEAR_ICON} No running event loop for error clear persistence: "
                f"{error_code.code}"
            )
        except Exception as e:
            logger.debug(f"{ERROR_CODE_CLEAR_ICON} Error scheduling clear persistence: {e}")

    async def _async_persist_add(self, error_code: ErrorCode) -> None:
        """
        Async method to persist error code to MongoDB.

        Inserts a new document for each error occurrence.
        Uses mongo_call for consistent retry/error handling.
        """
        try:
            # Import here to avoid circular imports
            from v4vapp_backend_v2.config.setup import InternalConfig
            from v4vapp_backend_v2.database.db_retry import mongo_call

            db = InternalConfig.db
            if db is None:
                return

            doc = error_code.to_mongo_doc(
                server_id=self._server_id or getattr(InternalConfig(), "server_id", ""),
                node_name=self._node_name or getattr(InternalConfig(), "node_name", ""),
                local_machine_name=self._local_machine_name
                or getattr(InternalConfig(), "local_machine_name", ""),
                active=True,
                cleared_at=None,
            )

            await mongo_call(
                lambda: db[ERROR_CODES_COLLECTION].insert_one(doc),
                max_retries=1,  # Don't retry much - this is fire-and-forget
                notify_on_error=False,  # Don't send notifications for error code persistence
                context=f"error_code_persist:{error_code.code}",
            )
            logger.debug(f"{ERROR_CODE_ICON} Persisted error code to MongoDB: {error_code.code}")
        except Exception as e:
            # Log but don't raise - persistence failures shouldn't break logging
            logger.warning(f"{ERROR_CODE_ICON} Failed to persist error code to MongoDB: {e}")

    async def _async_persist_clear(self, error_code: ErrorCode, clear_message: str = "") -> None:
        """
        Async method to persist error code clear event to MongoDB.

        Inserts a new document marking the error as cleared.
        Uses mongo_call for consistent retry/error handling.

        Args:
            error_code: The ErrorCode being cleared
            clear_message: Message describing why/how the error was cleared
        """
        try:
            from v4vapp_backend_v2.config.setup import InternalConfig
            from v4vapp_backend_v2.database.db_retry import mongo_call

            db = InternalConfig.db
            if db is None:
                return

            now = datetime.now(tz=timezone.utc)
            doc = error_code.to_mongo_doc(
                server_id=self._server_id or getattr(InternalConfig(), "server_id", ""),
                node_name=self._node_name or getattr(InternalConfig(), "node_name", ""),
                local_machine_name=self._local_machine_name
                or getattr(InternalConfig(), "local_machine_name", ""),
                active=False,
                cleared_at=now,
                clear_message=clear_message,
            )

            await mongo_call(
                lambda: db[ERROR_CODES_COLLECTION].insert_one(doc),
                max_retries=1,  # Don't retry much - this is fire-and-forget
                notify_on_error=False,  # Don't send notifications for error code persistence
                context=f"error_code_clear:{error_code.code}",
            )
            logger.debug(
                f"{ERROR_CODE_CLEAR_ICON} Persisted error clear to MongoDB: {error_code.code}"
            )
        except Exception as e:
            logger.warning(
                f"{ERROR_CODE_CLEAR_ICON} Failed to persist error clear to MongoDB: {e}"
            )

    # MARK: Query methods for retrieving historical data

    async def get_error_history(
        self,
        code: Any | None = None,
        active_only: bool = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Query error code history from MongoDB.

        Args:
            code: Filter by specific error code (None for all)
            active_only: Only return active (uncleared) errors
            limit: Maximum number of documents to return

        Returns:
            List of error code documents from MongoDB
        """
        try:
            from v4vapp_backend_v2.config.setup import InternalConfig

            db = InternalConfig.db
            if db is None:
                return []

            query: dict[str, Any] = {}
            if code is not None:
                query["code"] = code
            if active_only:
                query["active"] = True

            cursor = db[ERROR_CODES_COLLECTION].find(query).sort("created_at", -1).limit(limit)
            return await cursor.to_list(length=limit)
        except Exception as e:
            logger.warning(f"Failed to query error history from MongoDB: {e}")
            return []

    async def get_active_errors_from_db(self) -> list[ErrorCode]:
        """
        Load active errors from MongoDB.

        Useful for restoring state after restart.

        Returns:
            List of active ErrorCode instances
        """
        try:
            docs = await self.get_error_history(active_only=True, limit=1000)
            return [ErrorCode.from_mongo_doc(doc) for doc in docs]
        except Exception as e:
            logger.warning(f"Failed to load active errors from MongoDB: {e}")
            return []
