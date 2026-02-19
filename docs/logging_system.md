# v4vapp-backend-v2 Logging System

> Comprehensive reference for the logging architecture, configuration, filters,
> handlers, notifications, and runtime behaviour.
>
> **Use this document as context for any future AI/Copilot requests that touch
> logging, notifications, or console output.**

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Key Source Files](#2-key-source-files)
3. [Configuration Layer](#3-configuration-layer)
   - 3.1 [YAML Config (`logging:` section)](#31-yaml-config-logging-section)
   - 3.2 [JSON dictConfig files](#32-json-dictconfig-files)
4. [InternalConfig.setup_logging()](#4-internalconfigsetup_logging)
5. [Formatters](#5-formatters)
6. [Handlers](#6-handlers)
7. [Filters (the pipeline)](#7-filters-the-pipeline)
8. [Notification System](#8-notification-system)
9. [Error Code Tracking](#9-error-code-tracking)
10. [Log Rotation](#10-log-rotation)
11. [Per-Logger Level Overrides](#11-per-logger-level-overrides)
12. [Uvicorn / FastAPI Logging](#12-uvicorn--fastapi-logging)
13. [Docker Compose Log Output](#13-docker-compose-log-output)
14. [Quick Reference: `extra` dict keys](#14-quick-reference-extra-dict-keys)
15. [Testing Patterns](#15-testing-patterns)

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Python logging root                          │
│  level: set from config.logging.default_log_level (usually DEBUG)   │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Handler: queue_handler (QueueHandler)                               │
│    filter: ErrorTrackingFilter                                       │
│    ├── Handler: file_json (RotatingFileHandler)                      │
│    │     filter: ErrorTrackingFilter                                 │
│    │     formatter: MyJSONFormatter  → logs/*.jsonl                  │
│    └── Handler: notification (CustomNotificationHandler)             │
│          filters: ErrorTrackingFilter, NotificationFilter            │
│          formatter: simple                                           │
│          → Telegram bot via NotificationProtocol                     │
│                                                                      │
│  Handler: stderr (StreamHandler → sys.stderr)                        │
│    filter: ErrorTrackingFilter                                       │
│    level: WARNING                                                    │
│                                                                      │
│  Handler: stdout_color (colorlog.StreamHandler → sys.stdout)         │
│    filters: ErrorTrackingFilter, ConsoleLogFilter,                   │
│             AddJsonDataIndicatorFilter, AddNotificationBellFilter     │
│    level: governed by ConsoleLogFilter (config.logging.console_log_  │
│           level, default INFO)                                       │
│    formatter: ColoredFormatter (colorlog)                            │
└──────────────────────────────────────────────────────────────────────┘
```

The **"backend"** logger (`logging.getLogger("backend")`) is the primary
logger used throughout the codebase via:

```python
from v4vapp_backend_v2.config.setup import logger
```

All `v4vapp_backend_v2.*` loggers propagate up to root, which holds the
actual handlers.

### Flow of a single log record

1. `logger.info("message", extra={...})` creates a `LogRecord`.
2. Root logger level gate (usually DEBUG) — passes almost everything.
3. Each handler's own level gate checks the record.
4. Filters run **in order** on each handler:
   - `ErrorTrackingFilter` — tracks/suppresses error codes, sets
     `_error_tracking_processed` flag to avoid double-processing.
   - `ConsoleLogFilter` (stdout only) — compares `record.levelno` against
     `config.logging.console_log_level`.
   - `NotificationFilter` (notification handler only) — only passes
     WARNING+ or records with `extra={"notification": True}`.
   - `AddJsonDataIndicatorFilter` (stdout only) — appends `[field1, field2]`
     from extra dict to the message text.
   - `AddNotificationBellFilter` (stdout only) — appends 🔔 emoji for
     WARNING+ or notification-flagged records.
5. Formatter produces the output string.
6. Handler emits (writes to file / sends to Telegram / prints to console).

---

## 2. Key Source Files

| File | Role |
|------|------|
| `src/v4vapp_backend_v2/config/setup.py` | `InternalConfig` singleton, `LoggingConfig` model, `setup_logging()`, level parsing, rotation namer |
| `src/v4vapp_backend_v2/config/mylogger.py` | `MyJSONFormatter`, all filter classes, `CustomNotificationHandler`, `_json_default()` |
| `src/v4vapp_backend_v2/config/notification_protocol.py` | `NotificationProtocol`, `BotNotification` — async bridge to Telegram |
| `src/v4vapp_backend_v2/helpers/notification_bot.py` | `NotificationBot` — Telegram Bot wrapper with rate limiting, pattern filtering, retry logic |
| `src/v4vapp_backend_v2/config/error_code_class.py` | `ErrorCode` dataclass — tracks individual error state + timestamps |
| `src/v4vapp_backend_v2/config/error_code_manager.py` | `ErrorCodeManager` — dict-like container with MongoDB persistence |
| `config/logging/5-queued-stderr-json-file.json` | Production logging dictConfig (queued handlers) |
| `config/logging/2-stderr-json-file.json` | Simpler logging dictConfig (no queue, no notification) |
| `config/*.config.yaml` | Per-environment config; `logging:` section sets levels, log file, notification bot, etc. |

---

## 3. Configuration Layer

### 3.1 YAML Config (`logging:` section)

Defined in `LoggingConfig` (Pydantic model in `setup.py`):

```yaml
logging:
  log_config_file: 5-queued-stderr-json-file.json   # Which JSON dictConfig to load
  default_log_level: DEBUG    # Root logger + file handler level
  console_log_level: INFO     # ConsoleLogFilter threshold for stdout
  log_folder: logs            # Directory for .jsonl log files
  rotation_folder: true       # Put rotated files in logs/rotation/

  log_levels:                 # Per-logger overrides (name → level)
    grpc: WARNING
    asyncio: WARNING
    httpcore: WARNING
    httpx: WARNING
    pymongo: WARNING
    uvicorn: WARNING          # ← controls Uvicorn's own logger
    uvicorn.error: WARNING
    uvicorn.access: WARNING
    telegram: WARNING
    # ... etc.

  log_notification_silent:    # Package prefixes to never send to Telegram
    - nectarapi

  default_notification_bot_name: "@brianoflondon_bot"
```

**Important:** `log_levels` entries are applied _after_ `dictConfig` via:

```python
for logger_name, level in self.config.logging.log_levels.items():
    logging.getLogger(logger_name).setLevel(level)
```

This is how third-party library loggers (uvicorn, pymongo, etc.) are silenced
or elevated.

### 3.2 JSON dictConfig files

Located in `config/logging/`. Loaded by `logging.config.dictConfig()`.

**`5-queued-stderr-json-file.json`** (production):
- Uses `QueueHandler` → asynchronous file + notification.
- `file_json`: `RotatingFileHandler`, level DEBUG, JSON formatter.
- `notification`: `CustomNotificationHandler`, level DEBUG, simple formatter,
  filtered by `NotificationFilter`.
- `stderr`: `StreamHandler` to stderr, level WARNING.
- All handlers have `ErrorTrackingFilter`.

**`2-stderr-json-file.json`** (simpler):
- No queue handler, no notification handler.
- `file_json`: `RotatingFileHandler`, level INFO.
- `stderr`: `StreamHandler`, level WARNING.

Both set `"disable_existing_loggers": false` which is critical — it means
loggers created before `dictConfig` runs (like `uvicorn.access`) keep
working.

---

## 4. InternalConfig.setup_logging()

Called once during singleton initialization. Steps:

1. **Load JSON dictConfig** from `config/logging/<log_config_file>`.
2. **Patch log filename** in the config dict to use `config.logging.log_folder / <entrypoint>.jsonl`.
3. **Create log directory** if missing.
4. **Apply `logging.config.dictConfig(config)`** — installs formatters, handlers, filters.
5. **Apply per-logger level overrides** from `config.logging.log_levels`.
6. **Start QueueListener** if `queue_handler` exists (for async file/notification writing).
7. **Set up or reuse asyncio event loop** for notification sending.
8. **Set simple format string** from config (fallback to default).
9. **Assign rotation namer** to all `RotatingFileHandler` instances.
10. **Install `stdout_color` handler** on root logger:
    - Uses `colorlog.ColoredFormatter` if available (fallback: plain).
    - Adds filters: `ErrorTrackingFilter`, `ConsoleLogFilter`,
      `AddJsonDataIndicatorFilter`, `AddNotificationBellFilter`.
    - Skips installation if a console handler already exists (pytest/live).
    - Can be forced with env var `V4VAPP_FORCE_CONSOLE_LOG=1`.
11. **Set root logger level** to `config.logging.default_log_level`.
12. **Enable propagation** for `v4vapp_backend_v2` and root loggers.

---

## 5. Formatters

### `simple` (used for stderr and notification)

```
%(asctime)s.%(msecs)03d %(levelname)-8s %(module)-22s %(lineno)6d : %(message)s
```

Date format: `%Y-%m-%dT%H:%M:%S%z` (no date in `stdout_color`, which uses `%m-%dT%H:%M:%S`).

Output example:
```
02-10T18:01:37.449 INFO     hive_monitor_v2           313 : 🐝 Last recorded ...
```

### `json` (`MyJSONFormatter`, used for file handler)

Produces one JSON object per line (`.jsonl`). Fields:

```json
{
  "level": "INFO",
  "human_time": "18:01:37.449 Mon 10 Feb",
  "message": "...",
  "timestamp": "2026-02-10T18:01:37.449000+00:00",
  "logger": "backend",
  "module": "hive_monitor_v2",
  "function": "some_func",
  "line": 313,
  "thread_name": "MainThread",
  // ... any extra fields from the log record
}
```

Extra fields from the `extra={}` dict are automatically included.
`_json_default()` handles `Decimal`, `bson.Decimal128`, and falls back
to `str()`.

### `ColoredFormatter` (stdout_color handler)

Same format string as `simple` but with ANSI color prefixes via `colorlog`:

| Level | Color |
|-------|-------|
| DEBUG | cyan |
| INFO | blue |
| WARNING | yellow |
| ERROR | red |
| CRITICAL | red on white |

---

## 6. Handlers

| Name | Class | Output | Level | Filters |
|------|-------|--------|-------|---------|
| `queue_handler` | `QueueHandler` | Delegates to `file_json` + `notification` | — | `ErrorTrackingFilter` |
| `file_json` | `RotatingFileHandler` | `logs/<entrypoint>.jsonl` | DEBUG | `ErrorTrackingFilter` |
| `notification` | `CustomNotificationHandler` | Telegram bot | DEBUG | `ErrorTrackingFilter`, `NotificationFilter` |
| `stderr` | `StreamHandler` (stderr) | Container/terminal stderr | WARNING | `ErrorTrackingFilter` |
| `stdout_color` | `colorlog.StreamHandler` (stdout) | Container/terminal stdout | — (filtered by `ConsoleLogFilter`) | `ErrorTrackingFilter`, `ConsoleLogFilter`, `AddJsonDataIndicatorFilter`, `AddNotificationBellFilter` |

The `queue_handler` uses Python's `QueueHandler` + `QueueListener` pattern:
log records are placed on a queue by the calling thread and consumed by a
background thread that dispatches to `file_json` and `notification`. This
prevents I/O (file writes, Telegram HTTP calls) from blocking the main
application.

---

## 7. Filters (the pipeline)

### `ErrorTrackingFilter`

- Runs on nearly every handler.
- Looks for `extra={"error_code": "..."}` — tracks in `InternalConfig.error_codes`.
- Looks for `extra={"error_code_clear": "..."}` — removes from tracking.
- **Suppresses duplicate error code logs** within `re_alert_time` (default 1 hour).
- Sets `_error_tracking_processed` / `_error_tracking_result` on the record
  to prevent double-processing when multiple handlers have this filter.

### `NotificationFilter`

- Only on the `notification` handler.
- Passes records that are:
  - Level ≥ WARNING, **or**
  - Have `extra={"notification": True}`.
- Blocks records with `extra={"notification": False}`.
- Blocks records from packages listed in `config.logging.log_notification_silent`.

### `ConsoleLogFilter`

- Only on `stdout_color`.
- Compares `record.levelno` against `config.logging.console_log_level` (default INFO).
- Cached; call `ConsoleLogFilter.refresh_cached_level()` after config changes.

### `AddJsonDataIndicatorFilter`

- Only on `stdout_color`.
- Appends `[field1, field2, ...]` to the message showing which extra fields
  are present (excluding built-in and notification-related fields).
- Example: `🐝 Last recorded witness block ... [block_counter]`

### `AddNotificationBellFilter`

- Only on `stdout_color`.
- Appends ` 🔔` to messages that are WARNING+ or have `notification=True`.

### `NonErrorFilter`

- Passes only records with level ≤ INFO. Used in some configurations to split
  info-and-below to stdout vs errors to stderr.

### `NotDebugFilter`

- Passes only records with level > DEBUG.

---

## 8. Notification System

### Flow

```
LogRecord with notification=True or level≥WARNING
  → NotificationFilter (passes)
  → CustomNotificationHandler.emit()
    → NotificationProtocol.send_notification()
      → asyncio.run_coroutine_threadsafe() on notification_loop
        → BotNotification._send_notification()
          → NotificationBot.send_message()
            → python-telegram-bot Bot.send_message()
```

### NotificationBot features

- **Pattern-based rate limiting**: Tracks the last 20 chars of recent
  messages; if the same pattern appears ≥5 times in 60 seconds, subsequent
  messages are dropped (with a one-time warning log).
- **Retry logic**: Up to 3 retries with exponential backoff for `TimedOut`.
  Handles `RetryAfter` (Telegram flood control) by sleeping the specified
  duration.
- **Markdown support**: Auto-detects markdown, sanitizes for Telegram's
  MarkdownV1 and MarkdownV2 parse modes.
- **ANSI stripping**: Removes terminal color codes before sending.
- **Truncation**: Messages are truncated to 300 characters.
- **Machine name suffix**: Appends `InternalConfig().local_machine_name` to
  every message.

### Extra bot routing

Log records can specify additional Telegram bots to notify:

```python
logger.warning("alert!", extra={
    "extra_bot_name": "@some_other_bot",       # single extra bot
    # OR
    "extra_bot_names": ["@bot1", "@bot2"],     # multiple extra bots
})
```

### Bot configuration

Bot configs are JSON files in `config/` named `<bot_name>_n_bot_config.json`:

```json
{
  "name": "@brianoflondon_bot",
  "token": "...",
  "chat_id": 12345678
}
```

The default bot is set via `config.logging.default_notification_bot_name`.

### Critical: `notification_loop` must point to the running event loop

#### The problem

`InternalConfig.__init__()` → `setup_logging()` runs **before** `asyncio.run()`
(i.e. from the synchronous Typer `main()` command).  At that point there is no
running event loop, so `setup_logging()` creates a **detached** loop via
`asyncio.new_event_loop()` and stores it in `InternalConfig.notification_loop`.

The `QueueHandler` / `QueueListener` logging config
(`5-queued-stderr-json-file.json`) processes log records on a **background
thread**.  When `CustomNotificationHandler.emit()` fires,
`NotificationProtocol.send_notification()` checks `loop.is_running()`:

| `loop.is_running()` | Code path | Effect |
|---|---|---|
| `True`  | `asyncio.run_coroutine_threadsafe(coro, loop)` | **Non-blocking** — schedules work on the main event loop and returns immediately. |
| `False` | `loop.run_until_complete(coro)` | **Blocks the QueueListener thread** until the Telegram HTTP request completes (10s connect + 30s read timeout). |

Because the detached loop is never started, `is_running()` is always `False`.
Every notification record blocks the listener thread for up to 40 seconds.
While the thread is blocked, **no subsequent log records are dequeued** — so
file writes, console output, and all other handler processing stall.

This manifests as the monitor appearing to "hang" after the first
`notification: True` log message (typically the "✅ LND gRPC client started"
startup message).  The `asyncio.create_task()` tasks are actually running on
the main event loop, but their log output is stuck in the queue.

#### The fix

At the **top** of every monitor's `main_async_start()` coroutine (the first
function called by `asyncio.run()`), reassign `notification_loop` to the real
running loop:

```python
async def main_async_start(...) -> None:
    # Point notification_loop at the actual running event loop so the
    # QueueListener thread uses the non-blocking run_coroutine_threadsafe()
    # path instead of the blocking run_until_complete() path.
    InternalConfig.notification_loop = asyncio.get_running_loop()
    ...
```

This must be done in **every** monitor entry point:

| Monitor | File | Function |
|---|---|---|
| LND | `src/lnd_monitor_v2.py` | `main_async_start()` |
| Hive | `src/hive_monitor_v2.py` | `main_async_start()` |
| DB | `src/db_monitor.py` | `main_async_start()` |
| Binance | `src/binance_monitor.py` | `main_async_start()` |

#### Why `setup_logging()` can't do this itself

`setup_logging()` is called from `InternalConfig.__init__()`, which runs in
synchronous context (no event loop yet).  It **must** create *some* loop
reference so that log messages emitted during the rest of `__init__()` (Redis
setup, config validation, etc.) don't crash.  The detached loop works fine for
those early messages because the QueueListener hasn't started yet at that point
or the notification handler simply queues them.  The reassignment in
`main_async_start()` is the earliest safe point where a running loop exists.

---

## 9. Error Code Tracking

Error codes provide a deduplication and persistence mechanism for recurring
errors.

### Setting an error code

```python
logger.warning(
    "Calculated HBD/USD price is low: 0.940",
    extra={
        "error_code": "Low HBD USD Price",
        "re_alert_time": timedelta(hours=2),  # optional, default 1h
    }
)
```

### Clearing an error code

```python
logger.info(
    "HBD/USD price recovered",
    extra={"error_code_clear": "Low HBD USD Price"}
)
```

### Behaviour

- First occurrence: error is added to `InternalConfig.error_codes`
  (in-memory + MongoDB), a separate `❌ New error: ...` log is emitted, and
  the original record passes through.
- Subsequent occurrences within `re_alert_time`: the record is **suppressed**
  (filter returns `False`) — it won't appear in logs, console, or
  notifications.
- After `re_alert_time` elapses: the record passes through again and the
  timer resets.
- On clear: a `✅ Error code ... cleared after ...` info log is emitted with
  the elapsed duration.

---

## 10. Log Rotation

`RotatingFileHandler` is used with:
- `maxBytes`: 2,000,000 (2MB in production config)
- `backupCount`: 10

A custom `namer` function (from `make_rotation_namer()`) transforms rotated
filenames:

```
logs/hive_monitor_v2.jsonl.1  →  logs/hive_monitor_v2.001.jsonl
```

If `config.logging.rotation_folder` is `true`:

```
logs/hive_monitor_v2.jsonl.1  →  logs/rotation/hive_monitor_v2.001.jsonl
```

The padding width adapts to `backupCount` (e.g., 3 digits for count ≤ 999).

---

## 11. Per-Logger Level Overrides

The `log_levels` dict in the YAML config lets you silence noisy third-party
libraries:

```yaml
log_levels:
  grpc: WARNING
  pymongo: WARNING
  uvicorn: WARNING
  uvicorn.error: WARNING
  uvicorn.access: WARNING
```

These are applied in `setup_logging()` via:

```python
for logger_name, level in self.config.logging.log_levels.items():
    logging.getLogger(logger_name).setLevel(level)
```

**This only affects records emitted through Python's `logging` module.** It
does NOT affect output that bypasses `logging` entirely (see Uvicorn section
below).

---

## 12. Uvicorn / FastAPI Logging

### How Uvicorn logs work

Uvicorn uses **two Python loggers**:

| Logger name | Purpose | Default level |
|-------------|---------|---------------|
| `uvicorn.error` | Server lifecycle messages ("Started server process", "Uvicorn running on ...") | INFO |
| `uvicorn.access` | HTTP request access logs ("127.0.0.1 - GET /status 200 OK") | INFO |

When you call `uvicorn.run(app, ...)`, Uvicorn:

1. Configures its own loggers (`uvicorn`, `uvicorn.error`, `uvicorn.access`).
2. By default, sets up its **own handlers** that write directly to stderr.
3. These handlers use Uvicorn's own formatters (the familiar
   `INFO: 127.0.0.1:35852 - "GET /status HTTP/1.1" 200 OK` format).

### Why the INFO lines appear in docker compose

The log lines you see like:

```
api-v2          | INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
api-v2          | INFO:     127.0.0.1:35852 - "GET /status HTTP/1.1" 200 OK
admin-interface | INFO:     Started server process [1]
admin-interface | INFO:     Application startup complete.
admin-interface | INFO:     127.0.0.1:52434 - "GET /status HTTP/1.1" 200 OK
```

These come from **Uvicorn's own logging handlers**, NOT from your
application's logging system. Uvicorn installs its own `StreamHandler` on
`uvicorn.error` and `uvicorn.access` loggers. Even though your config sets:

```yaml
log_levels:
  uvicorn: WARNING
  uvicorn.error: WARNING
  uvicorn.access: WARNING
```

...this only changes the **logger level** on those loggers. But Uvicorn's
`run()` function **re-configures its own loggers** on startup, potentially
overriding your level settings. Specifically:

- `uvicorn.run()` accepts a `log_level` parameter (defaults to `"info"`).
- During startup, Uvicorn calls its own `logging.config.dictConfig()` which
  **resets** the handlers and levels on `uvicorn.*` loggers.
- Your `setup_logging()` may run before or after this, depending on
  initialization order.

### Where each service stands

| Service | How Uvicorn is called | Uvicorn log_level | access_log |
|---------|-----------------------|-------------------|------------|
| `api-v2` (`api_v2.py`) | `uvicorn.run(app, host=..., port=...)` | **Not set** (defaults to `"info"`) | **Not set** (defaults to `True`) |
| `admin-interface` (`run_admin.py`) | `uvicorn.run(app, ..., log_level=args.log_level, access_log=True)` | CLI arg `--log-level debug` | Explicitly `True` |
| StatusAPI (internal health) | `uvicorn.Config(..., log_level="critical", access_log=False)` | `"critical"` | `False` |

The StatusAPI (the internal `/status` endpoint used by health checks) already
correctly suppresses its own Uvicorn logs.

### The `devdocker.config.yaml` vs `devhive.config.yaml` difference

- **`devdocker.config.yaml`** includes:
  ```yaml
  log_levels:
    uvicorn: WARNING
    uvicorn.error: WARNING
    uvicorn.access: WARNING
  ```
  ...but these get overwritten when Uvicorn re-configures itself.

- **`devhive.config.yaml`** does NOT include any `uvicorn*` entries in
  `log_levels` at all.

In both cases the visible `INFO:` lines persist because Uvicorn's own
configuration takes precedence.

### How to control these lines (options, without code changes)

There are several approaches, roughly from simplest to most thorough:

#### Option A: Set `log_level` in `uvicorn.run()` calls

The most direct fix. In both `api_v2.py` and `run_admin.py`, pass:

```python
uvicorn.run(app, host=..., port=..., log_level="warning")
```

This tells Uvicorn to configure its own loggers at WARNING level, which
silences the startup messages and access logs.

#### Option B: Disable access logs only

If you want the startup messages but not the per-request spam:

```python
uvicorn.run(app, host=..., port=..., access_log=False)
```

This disables only the `INFO: 127.0.0.1 - "GET /status" 200 OK` lines while
keeping `Uvicorn running on...` and `Application startup complete`.

#### Option C: Pass a custom `log_config` to Uvicorn

Uvicorn accepts `log_config=` parameter (dict or path to a YAML/JSON file):

```python
uvicorn.run(app, log_config=None, ...)  # Disables Uvicorn's own log config entirely
```

Setting `log_config=None` tells Uvicorn to NOT configure logging at all,
which means YOUR dictConfig from `setup_logging()` stays in full effect.
Then your `log_levels` YAML entries for `uvicorn.*` will actually work.

This is the cleanest approach because it lets your unified logging system
control everything, but it requires that `InternalConfig()` initializes
before `uvicorn.run()` is called (which it does in both entrypoints).

#### Option D: Suppress at docker compose level

If you only want to hide them from `docker compose logs` output without
changing the application:

```yaml
logging:
  driver: "json-file"
  options:
    max-size: "10m"
```

This doesn't actually filter the content though — just limits file size.
There's no built-in docker log filtering by content.

### The `/status` health check lines specifically

The lines:
```
api-v2          | INFO:     127.0.0.1:35852 - "GET /status HTTP/1.1" 200 OK
admin-interface | INFO:     127.0.0.1:52434 - "GET /status HTTP/1.1" 200 OK
```

These appear every 30 seconds due to the Docker healthcheck:

```yaml
healthcheck:
  test: ["CMD", "python", "src/health_check.py", "--host", "localhost", "--port", "8000"]
  interval: 30s
```

These are Uvicorn access logs for the health check requests. They are
particularly noisy because they repeat every 30 seconds per container.
Disabling via `access_log=False` or raising the log level to WARNING
would eliminate them.

---

## 13. Docker Compose Log Output

When running via `docker compose`, **all stdout and stderr from the container
process is captured** as docker logs. What you see in `docker compose logs`
is the combined stdout+stderr of each container.

For v4vapp services this means:

1. **Your application logs** (from stdout_color handler → stdout):
   ```
   02-10T18:01:37.449 INFO     hive_monitor_v2           313 : 🐝 Last recorded ...
   ```

2. **Uvicorn's own logs** (from Uvicorn's handler → stderr):
   ```
   INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
   INFO:     127.0.0.1:35852 - "GET /status HTTP/1.1" 200 OK
   ```

3. **print() statements** (go directly to stdout):
   ```
   🚀 Starting V4VApp Admin Interface
   📁 Config file: devdocker.config.yaml
   ```

These are visually distinct — your app logs have the timestamp+module format,
Uvicorn's have the `INFO:     ` prefix format with extra spaces, and print
statements have no prefix at all.

---

## 14. Quick Reference: `extra` dict keys

| Key | Type | Effect |
|-----|------|--------|
| `notification` | `bool` | `True`: force send to Telegram. `False`: suppress Telegram. |
| `notification_str` | `str` | Override the notification message text (send this instead of `record.getMessage()`). |
| `silent` | `bool` | Telegram `disable_notification=True` (no sound/vibration). |
| `error_code` | `str` | Track this error code in ErrorCodeManager; suppress duplicates. |
| `error_code_clear` | `str` | Clear a tracked error code. |
| `re_alert_time` | `timedelta` | Override the default 1-hour re-alert window for error codes. |
| `error_code_obj` | `ErrorCode` | Attached by ErrorTrackingFilter; available in handlers. |
| `bot_name` | `str` | Override which Telegram bot sends this notification. |
| `extra_bot_name` | `str` | Send notification to an additional bot. |
| `extra_bot_names` | `list[str]` | Send notification to multiple additional bots. |
| `block_counter` | various | Domain data included in JSON file logs and shown in `[...]` on console. |
| `json_data` | `bool` | (Legacy) indicator that extra data is present. |

Any extra keys not in the built-in set are:
- Included in the JSON log file as additional fields.
- Listed in the `[field1, field2]` suffix on console output by
  `AddJsonDataIndicatorFilter`.

---

## 15. Testing Patterns

- Tests monkeypatch `InternalConfig.base_config_path` to `tests/data/config`
  and `InternalConfig._instance = None` to reset the singleton.
- Test logging configs are in `tests/data/config/logging/`.
- The test `conftest.py` provides a session-scoped event loop for async tests.
- `NotificationBot` can be mocked by replacing `CustomNotificationHandler.sender`
  with a mock `NotificationProtocol`.
- `ErrorCodeManager` is reset via `InternalConfig.error_code_manager = ErrorCodeManager(db_enabled=False)` in tests.
- The `ConsoleLogFilter` cached level should be refreshed after config changes
  in tests: `ConsoleLogFilter.refresh_cached_level()`.

---

## Summary of common configuration knobs

| What you want | Where to change it |
|---------------|-------------------|
| Silence a noisy library | `config.yaml` → `logging.log_levels.<lib>: WARNING` (works for Python-logging-based libraries, but see Uvicorn caveat) |
| Change console output level | `config.yaml` → `logging.console_log_level: WARNING` |
| Change file log level | `config.yaml` → `logging.default_log_level: INFO` |
| Suppress Telegram for a package | `config.yaml` → `logging.log_notification_silent: [package_name]` |
| Suppress Telegram for one message | `extra={"notification": False}` |
| Force Telegram for INFO message | `extra={"notification": True}` |
| Track an error with dedup | `extra={"error_code": "My Error Name"}` |
| Control Uvicorn output | `uvicorn.run(..., log_level="warning", access_log=False)` or `log_config=None` to defer to your config |
| Switch logging config entirely | `config.yaml` → `logging.log_config_file: 2-stderr-json-file.json` |
| Put rotated logs in subfolder | `config.yaml` → `logging.rotation_folder: true` |
