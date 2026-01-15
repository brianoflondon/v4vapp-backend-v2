# Claude Context: ErrorCodeManager Feature

## Feature Summary

This PR (`feature/record-error_code-events-in-database-logs`) adds MongoDB persistence to the error code tracking system. Previously, error codes were only tracked in-memory via a simple dict in `InternalConfig`. Now they persist to a MongoDB collection for historical analysis.

## Key Files

### Core Implementation
- `src/v4vapp_backend_v2/config/error_code_manager.py` - Singleton manager with dict-like interface + MongoDB persistence
- `src/v4vapp_backend_v2/config/error_code_class.py` - `ErrorCode` dataclass with `to_mongo_doc()` / `from_mongo_doc()` methods
- `src/v4vapp_backend_v2/config/setup.py` - `InternalConfig` integration (see `error_code_manager` class var and `error_codes` property)
- `src/v4vapp_backend_v2/config/mylogger.py` - `ErrorTrackingFilter` uses manager's `add()` method

### Tests
- `tests/mylogger/test_error_code_manager.py` - 27 comprehensive tests
- `tests/mylogger/test_custom_notification_handler.py` - Tests error tracking filter integration

### Documentation
- `docs/error_code_manager.md` - Full feature documentation

## Architecture

```
Log event with error_code extra
        │
        ▼
ErrorTrackingFilter (mylogger.py)
        │
        ▼
ErrorCodeManager.add(ErrorCode)
        │
        ├──► In-memory dict (fast suppression)
        │
        └──► MongoDB persistence (fire-and-forget async)
```

## MongoDB Collection: `error_codes`

Documents contain:
- `code`, `message`, `start_time`, `last_log_time` - from ErrorCode
- `server_id`, `node_name`, `local_machine_name` - from InternalConfig
- `active` (bool), `cleared_at` (datetime) - tracks error lifecycle
- `created_at`, `updated_at` - timestamps

## Key Design Decisions

1. **Singleton pattern** - `ErrorCodeManager._instance` ensures single source of truth
2. **Dict-like interface** - Backward compatibility with `__contains__`, `__getitem__`, `pop`, etc.
3. **Fire-and-forget persistence** - Uses `asyncio.get_running_loop().create_task()` to avoid blocking
4. **Same event loop constraint** - Persistence only works from the main loop where `InternalConfig.db` was created
5. **mongo_call wrapper** - Uses `db_retry.mongo_call` for consistent retry/error handling
6. **Unique error codes per machine** - Error codes like `witness_api_invalid_response_{machine_name}` prevent cross-machine clearing

## Known Issues / Gotchas

1. **Event loop binding** - `AsyncMongoClient` is bound to the loop where it's created. Persistence silently skips if called from a different loop.
2. **Singleton reset in tests** - Tests must reset `ErrorCodeManager._instance = None` in fixtures (see `tests/conftest.py`)
3. **Circular import avoidance** - `_async_persist_add` imports `InternalConfig` inside the method

## Testing Commands

```bash
# Run ErrorCodeManager tests
uv run pytest tests/mylogger/test_error_code_manager.py -v

# Run all mylogger tests (56 tests)
uv run pytest tests/mylogger/ -v

# Run witness monitor tests (validates error code uniqueness fix)
uv run pytest tests/witness_monitor/ -v
```

## Recent Bug Fixes

1. **Event loop conflict** - Fixed "Cannot use AsyncMongoClient in different event loop" by only using `asyncio.get_running_loop()` instead of `notification_loop`

2. **Error code uniqueness** - Fixed witness errors being cleared incorrectly by making error codes unique per machine:
   ```python
   # witness_events.py line 346
   error_code = f"witness_api_invalid_response_{machine_name}"
   ```

## Extension Points

- `ErrorCodeManager.get_error_history()` - Query historical errors from MongoDB
- `ErrorCodeManager.get_active_errors_from_db()` - Restore state after restart
- Add indexes on `error_codes` collection for performance: `code`, `active`, `created_at`
