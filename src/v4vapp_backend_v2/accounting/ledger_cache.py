"""
Ledger balance cache using Redis with generation-based invalidation.

Cache keys are constructed as:
    ledger:bal:v{generation}:{param_hash}

To invalidate ALL cached ledger balances at once (e.g. when a new LedgerEntry
is saved), call ``invalidate_ledger_cache()`` which increments the generation
counter.  Old keys expire naturally via TTL — no SCAN/DEL needed, so
invalidation is O(1).

All operations are fault-tolerant: if Redis is unavailable the functions
return ``None`` / silently skip, and the caller falls back to the database.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

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
    as_of_date: datetime,
    age: timedelta | None,
) -> str:
    """Build a deterministic Redis key from the query parameters."""
    account_part = f"{account.name}:{account.account_type.value}:{account.sub}:{account.contra}"
    # Truncate to the minute so near-simultaneous "now" requests share a key.
    date_part = as_of_date.replace(second=0, microsecond=0).isoformat()
    age_part = str(int(age.total_seconds())) if age else "none"

    raw = f"{account_part}|{date_part}|{age_part}"
    key_hash = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return f"ledger:bal:v{generation}:{key_hash}"


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


async def invalidate_ledger_cache() -> int:
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


# ---------------------------------------------------------------------------
# Get / Set
# ---------------------------------------------------------------------------


async def get_cached_balance(
    account: LedgerAccount,
    as_of_date: datetime,
    age: timedelta | None,
) -> LedgerAccountDetails | None:
    """Return a cached ``LedgerAccountDetails`` or ``None`` on miss / error."""
    from v4vapp_backend_v2.accounting.accounting_classes import LedgerAccountDetails

    try:
        gen = await get_cache_generation()
        key = _make_cache_key(gen, account, as_of_date, age)
        data: str | None = await InternalConfig.redis_async.get(key)
        if data is not None:
            result = LedgerAccountDetails.model_validate_json(data)
            logger.debug(f"Ledger cache HIT: {key}")
            return result
    except Exception as e:
        logger.debug(f"Ledger cache miss/error: {e}")
    return None


async def set_cached_balance(
    account: LedgerAccount,
    as_of_date: datetime,
    age: timedelta | None,
    result: LedgerAccountDetails,
    ttl: int = DEFAULT_TTL_SECONDS,
) -> None:
    """Store a ``LedgerAccountDetails`` in the cache.

    Uses Pydantic JSON serialisation (preserves ``Decimal`` precision).
    """
    try:
        gen = await get_cache_generation()
        key = _make_cache_key(gen, account, as_of_date, age)
        data = result.model_dump_json()
        await InternalConfig.redis_async.setex(key, ttl, data)
        logger.debug(f"Ledger cache SET: {key} (ttl={ttl}s)")
    except Exception as e:
        logger.warning(f"Failed to set ledger cache: {e}")
