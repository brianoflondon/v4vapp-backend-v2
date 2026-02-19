# Ledger Balance Cache

Redis-based caching for ledger account balance queries, using **generation-based invalidation** for O(1) bulk expiry.

## Overview

`one_account_balance()` is the primary function for fetching a single account's balance details. It runs a MongoDB aggregation pipeline that can be expensive on large ledgers. The cache sits in front of this pipeline so repeated reads (e.g. dashboard refreshes, API polling) are served from Redis.

### Key design goals

- **Instant bulk invalidation** — when any `LedgerEntry` is saved, *all* cached balances become stale. Invalidation must be O(1), not O(n) over cached keys.
- **No stale reads** — a cache hit after a ledger write must never return pre-write data.
- **Fault tolerance** — if Redis is unavailable, the system falls back transparently to the database.
- **Pydantic-native serialisation** — `LedgerAccountDetails` is stored as JSON via `model_dump_json()` / `model_validate_json()`, preserving `Decimal` precision.

## How it works

### Generation counter

A single Redis key holds an integer **generation number**:

```
ledger:__generation__   →   42
```

Every cached balance key embeds the current generation:

```
ledger:bal:v42:a1b2c3d4e5f6...
```

| Operation | What happens | Cost |
|---|---|---|
| **Cache read** | Get current generation, build key with `v{gen}`, `GET` the key | O(1) |
| **Cache write** | Get current generation, build key with `v{gen}`, `SETEX` with TTL | O(1) |
| **Invalidate all** | `INCR ledger:__generation__` | O(1) |

After invalidation the generation becomes `43`. All existing keys contain `v42` and will never be looked up again — they expire naturally when their TTL runs out. No `SCAN`/`DEL` loop needed.

### Cache key construction

Keys are built from a SHA-256 hash of the query parameters:

```
{account.name}:{account_type}:{sub}:{contra}|{as_of_date (minute-truncated)}|{age_seconds}
```

Truncating `as_of_date` to the minute means near-simultaneous "give me the current balance" requests share a cache entry.

### TTL policy

| Query type | TTL | Rationale |
|---|---|---|
| "Live" (`as_of_date` was `None` / now) | 60 s | Balance changes frequently |
| Historical (explicit `as_of_date`) | 300 s | Past data is immutable |

### In-progress msats

`in_progress_msats` (hold/release totals) change independently of ledger writes, so they are **always refreshed** on a cache hit — only the expensive aggregation pipeline result is cached.

## Source files

| File | Role |
|---|---|
| `src/v4vapp_backend_v2/accounting/ledger_cache.py` | Cache module: `get_cached_balance`, `set_cached_balance`, `invalidate_ledger_cache`, `get_cache_generation` |
| `src/v4vapp_backend_v2/accounting/account_balances.py` | `one_account_balance()` — cache integration point |
| `src/v4vapp_backend_v2/accounting/ledger_entry_class.py` | `LedgerEntry.save()` — calls `invalidate_ledger_cache()` after insert/upsert |
| `tests/accounting/test_ledger_cache.py` | Test suite for the cache layer |

## API

### `one_account_balance()`

```python
async def one_account_balance(
    account: LedgerAccount | str,
    as_of_date: datetime | None = None,
    age: timedelta | None = None,
    in_progress: InProgressResults | None = None,
    use_cache: bool = True,          # ← new parameter
) -> LedgerAccountDetails:
```

Set `use_cache=False` to bypass the cache for a specific call (e.g. debugging, forced refresh).

### `invalidate_ledger_cache()`

```python
from v4vapp_backend_v2.accounting.ledger_cache import invalidate_ledger_cache

new_generation = await invalidate_ledger_cache()
```

Called automatically inside `LedgerEntry.save()`. Can also be called manually if you need to force-invalidate from other code paths (e.g. bulk imports, admin tools).

### `get_cache_generation()`

```python
from v4vapp_backend_v2.accounting.ledger_cache import get_cache_generation

gen = await get_cache_generation()  # int, 0 if unset
```

Useful for diagnostics / logging.

## Why not aiocache `@cached`?

The project has `aiocache` installed and it was originally considered. Reasons for using raw Redis instead:

1. **Generation-based invalidation** — aiocache's `@cached` supports per-key TTL and namespace clearing via `SCAN + DEL`, but not O(1) generation-based invalidation.
2. **Pydantic serialisation** — aiocache uses pickle by default, which is fragile across code changes. Using `model_dump_json()` / `model_validate_json()` is safer and debuggable (you can inspect cached values with `redis-cli`).
3. **Selective freshness** — `in_progress_msats` needs to be refreshed even on cache hits, which doesn't fit the `@cached` decorator model cleanly.
4. **Simplicity** — the cache module is ~140 lines with no external dependencies beyond `redis.asyncio`.

## Configuration

The cache uses `InternalConfig.redis_async` (the shared async Redis client configured in `setup.py`). No additional configuration is needed — if Redis is available, caching works automatically.

The generation key and all balance keys live under the `ledger:` prefix in the configured Redis database.

## Monitoring

Log messages to watch for:

| Message | Meaning |
|---|---|
| `Ledger cache HIT: ledger:bal:v...` | DEBUG — served from cache |
| `Ledger cache SET: ledger:bal:v... (ttl=60s)` | DEBUG — stored new entry |
| `🗑️ Ledger cache invalidated — generation now N` | INFO — all entries orphaned |
| `cache_hit=0.003s for one_account_balance ...` | INFO — timing for cached response |
| `Failed to set/invalidate ledger cache: ...` | WARNING — Redis issue, falling back to DB |
