"""
Ledger balance cache using Redis.  Keys embed a generation number but also
include the account name/sub pair so that we can perform *selective invalidation*
when only a few accounts change.

Cache keys are constructed as:
    ledger:bal:v{generation}:{param_hash}

Operations fall into two categories:

* **Full invalidation** – increment the generation counter and ignore every
  existing key. This path is O(1) and is used as a fallback or when the
  entire cache needs flushing.
* **Selective invalidation** – delete only keys whose embedded account
  information matches a supplied debit/credit pair. This uses a lightweight
  SCAN/DEL loop and keeps unrelated entries alive.

All operations are fault-tolerant: if Redis is unavailable the functions
return ``None`` / silently skip, and the caller falls back to the database.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from v4vapp_backend_v2.config.decorators import async_time_decorator
from v4vapp_backend_v2.config.setup import InternalConfig, logger

if TYPE_CHECKING:
    from v4vapp_backend_v2.accounting.accounting_classes import LedgerAccountDetails
    from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount


# Redis key that stores the current generation counter.
GENERATION_KEY = "ledger:__generation__"

# Default TTL for cached balance entries (seconds).
DEFAULT_TTL_SECONDS = 1200

# Shorter TTL for "live" queries where as_of_date was not explicitly provided.
LIVE_TTL_SECONDS = 1200

# Longer TTL for point-in-time / historical queries.
HISTORICAL_TTL_SECONDS = 1200


# ---------------------------------------------------------------------------
# Key construction
# ---------------------------------------------------------------------------


def _make_cache_key(
    generation: int,
    account: LedgerAccount,
    as_of_date: datetime | None,
    age: timedelta | None,
) -> str:
    """Build a deterministic Redis key from the query parameters.

    When ``as_of_date`` is ``None`` we treat the query as a "live" lookup and
    omit any timestamp information from the hash.  This keeps keys stable
    across minute boundaries when callers are repeatedly asking for the
    current balance.
    """
    account_part = f"{account.name}:{account.account_type.value}:{account.sub}:{account.contra}"

    if as_of_date is None:
        date_part = "live"
    else:
        # Truncate to the minute so near-simultaneous "now" requests share a key.
        date_part = as_of_date.replace(second=0, microsecond=0).isoformat()
    if age is None or age.total_seconds() <= 0:
        age_part = "none"
    else:
        # Round age to the nearest second for key stability.
        age_part = str(int(age.total_seconds())) if age else "none"

    raw = f"{account_part}|{date_part}|{age_part}"
    key_hash = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return f"ledger:bal:v{generation}:{account.name}:{account.sub}:{key_hash}"


# ---------------------------------------------------------------------------
# Generation helpers
# ---------------------------------------------------------------------------


async def get_cache_generation() -> int:
    """Return the current cache generation number (0 if unset/unavailable)."""
    try:
        val = await InternalConfig.redis_async.get(GENERATION_KEY)
        return int(val) if val else 0
    except Exception:
        return 0


@async_time_decorator
async def invalidate_all_ledger_cache() -> int:
    """Increment the generation counter, instantly orphaning every cached entry.

    Old keys will be ignored on lookup (wrong generation) and removed by Redis
    when their TTL expires.

    Returns the new generation number, or 0 on failure.
    """
    try:
        new_gen: int = await InternalConfig.redis_async.incr(GENERATION_KEY)
        logger.debug(
            f"🗑️  Ledger cache invalidated — generation now {new_gen}",
            extra={"notification": False},
        )
        return new_gen
    except Exception as e:
        logger.warning(f"Failed to invalidate ledger cache: {e}")
        return 0


@async_time_decorator
async def invalidate_ledger_cache(
    debit_name: str, debit_sub: str, credit_name: str, credit_sub: str
) -> int:
    """Remove cached balances matching either debit or credit account.

    When a ledger entry is created or updated we only expect two accounts to be
    affected.  Instead of bumping the global generation (which would trash the
    entire cache), this function scans Redis for keys whose embedded name/sub
    matches the supplied pairs and deletes them.  Only a handful of keys are
    touched, and unrelated cache entries remain usable.

    The return value is the current generation number (unchanged).  On error we
    fall back to ``invalidate_all_ledger_cache()`` to guarantee no stale data is
    returned.
    """
    # build glob patterns for the two account pairs
    debit_key = f"ledger:bal:v*:{debit_name}:{debit_sub}:*"
    credit_key = f"ledger:bal:v*:{credit_name}:{credit_sub}:*"
    try:
        # Use SCAN to locate and delete matching keys.  In a typical
        # deployment only a few cache entries will match the account pair, so
        # this loop completes quickly.  It avoids the O(n) cost of SCAN/DEL over
        # the whole ledger: namespace that we would incur with a naive
        # invalidation.
        for pattern in [debit_key, credit_key]:
            cursor_val = 0
            while True:
                cursor_val, keys = await InternalConfig.redis_async.scan(
                    cursor_val, match=pattern, count=100
                )
                if keys:
                    await InternalConfig.redis_async.delete(*keys)
                if cursor_val == 0:
                    break
        logger.debug(
            f"🗑️  Ledger cache invalidated for accounts {debit_name}:{debit_sub} and {credit_name}:{credit_sub}",
            extra={"notification": False},
        )
        return await get_cache_generation()  # Return current generation after invalidation
    except Exception as e:
        logger.warning(f"Failed to invalidate ledger cache for accounts: {e}")
        return await invalidate_all_ledger_cache()  # Fallback to full invalidation on error


# ---------------------------------------------------------------------------
# Get / Set
# ---------------------------------------------------------------------------


async def get_cached_balance(
    account: LedgerAccount,
    as_of_date: datetime | None,
    age: timedelta | None,
) -> LedgerAccountDetails | None:
    """Return a cached ``LedgerAccountDetails`` or ``None`` on miss / error.

    ``as_of_date`` may be ``None`` to indicate a live query.  The caller
    should have recorded that fact (see ``one_account_balance``) in order to
    keep the cache key consistent.
    """
    from v4vapp_backend_v2.accounting.account_balances import LedgerAccountDetails

    try:
        gen = await get_cache_generation()
        key = _make_cache_key(gen, account, as_of_date, age)
        data: str | None = await InternalConfig.redis_async.get(key)
        if data is not None:
            result = LedgerAccountDetails.model_validate_json(data)
            logger.info(f"Ledger cache HIT: {key}")
            return result
    except Exception as e:
        logger.debug(f"Ledger cache miss/error: {e}")
    return None


async def set_cached_balance(
    account: LedgerAccount,
    as_of_date: datetime | None,
    age: timedelta | None,
    result: LedgerAccountDetails,
    ttl: int = DEFAULT_TTL_SECONDS,
) -> None:
    """Store a ``LedgerAccountDetails`` in the cache.

    Uses Pydantic JSON serialisation (preserves ``Decimal`` precision).
    ``as_of_date`` may be ``None`` for live queries; callers are expected to
    signal the original intent so that the key remains stable across minute
    rolls.
    """
    try:
        gen = await get_cache_generation()
        key = _make_cache_key(gen, account, as_of_date, age)
        data = result.model_dump_json()
        await InternalConfig.redis_async.setex(key, ttl, data)
        logger.info(f"Ledger cache SET: {key} (ttl={ttl}s)")
    except Exception as e:
        logger.warning(f"Failed to set ledger cache: {e}")
