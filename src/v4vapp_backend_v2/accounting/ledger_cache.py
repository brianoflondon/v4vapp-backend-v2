"""
Ledger balance cache using Redis.  Keys embed a generation number but also
include the account name/sub pair so that we can perform *selective invalidation*
when only a few accounts change.

Cache keys are constructed as:
    ledger:bal:v{generation}:{sub}:{name}:{account_type}:{contra}:{date_part}:{age_part}:cp{0|1}

Where:
    {date_part} = "live" for current queries, or "2026-02-28T2359Z" (minute-truncated UTC)
    {age_part}  = "none" or "{n}s" (e.g. "86400s")
    cp0 / cp1   = use_checkpoints=False / True

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

import asyncio
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from colorama import Fore

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
    use_checkpoints: bool = True,
) -> str:
    """Build a fully human-readable Redis key from the query parameters.

    Key format:
        ledger:bal:v{generation}:{sub}:{name}:{account_type}:{contra}:{date_part}:{age_part}:cp{0|1}

    When ``as_of_date`` is ``None`` the query is treated as a "live" lookup.
    Datetime values are truncated to the minute and formatted without colons in
    the time portion so they don't conflict with the ``:`` segment separator.
    """
    if as_of_date is None:
        date_part = "live"
    else:
        # Truncate to the minute; format as YYYYMMDDTHHMMz (no colons in time)
        date_part = as_of_date.replace(second=0, microsecond=0).strftime("%Y-%m-%dT%H%MZ")

    if age is None or age.total_seconds() <= 0:
        age_part = "none"
    else:
        age_part = f"{int(age.total_seconds())}s"

    cp_part = "cp" if use_checkpoints else "no_cp"
    contra_part = "contra" if account.contra else "normal"

    start = f"ledger:bal:v{generation}"
    name_account = f"{account.sub}:{account.name}:{account.account_type.value}-{contra_part}"
    dates = f"date-{date_part}-age-{age_part}"

    answer = f"{start}:{name_account}:{cp_part}:{dates}"
    return answer


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


async def invalidate_all_ledger_cache() -> int:
    """Increment the generation counter, instantly orphaning every cached entry.

    Old keys will be ignored on lookup (wrong generation) and removed by Redis
    when their TTL expires.

    Returns the new generation number, or 0 on failure.
    """
    try:
        new_gen: int = await InternalConfig.redis_async.incr(GENERATION_KEY)
        logger.info(
            f"🗑️  Ledger cache invalidated — generation now {new_gen}",
            extra={"notification": False},
        )
        return new_gen
    except Exception as e:
        logger.exception(
            f"This shouldn't happen. Failed to invalidate ledger cache: {e}",
            extra={"notification": True},
        )
        return 0


async def invalidate_ledger_cache(
    debit_name: str, debit_sub: str, credit_name: str = "", credit_sub: str = ""
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
    # These patterns MUST match the _make_cache_key format,
    # especially the position of name/sub and the generation wildcard.
    debit_key = f"ledger:bal:v*:{debit_sub}:{debit_name}:*"
    patterns = [debit_key]
    if credit_name and credit_sub:
        credit_key = f"ledger:bal:v*:{credit_sub}:{credit_name}:*"
        patterns.append(credit_key)
    else:
        credit_key = None

    try:
        # Use SCAN to locate and delete matching keys.  In a typical
        # deployment only a few cache entries will match the account pair, so
        # this loop completes quickly.  It avoids the O(n) cost of SCAN/DEL over
        # the whole ledger: namespace that we would incur with a naive
        # invalidation.
        tasks = []
        for pattern in patterns:
            cursor_val = 0
            while True:
                cursor_val, keys = await InternalConfig.redis_async.scan(
                    cursor_val, match=pattern, count=100
                )
                if keys:
                    tasks.append(InternalConfig.redis_async.delete(*keys))
                    logger.debug(
                        f"🗑️  Deleted {len(keys)} cache keys matching pattern: {pattern}",
                        extra={"notification": False},
                    )
                if cursor_val == 0:
                    break
        if tasks:
            await asyncio.gather(*tasks)
        logger.debug(
            f"🗑️  Ledger cache invalidated for accounts {debit_name}:{debit_sub} and {credit_name}:{credit_sub}",
            extra={"notification": False},
        )
        return await get_cache_generation()  # Return current generation after invalidation
    except Exception as e:
        logger.exception(
            f"Failed to invalidate ledger cache for accounts: {e}, falling back to full invalidation",
            extra={"notification": True},
        )
        return await invalidate_all_ledger_cache()  # Fallback to full invalidation on error


# ---------------------------------------------------------------------------
# Get / Set
# ---------------------------------------------------------------------------


async def get_cached_balance(
    account: LedgerAccount,
    as_of_date: datetime | None,
    age: timedelta | None,
    use_checkpoints: bool = True,
) -> LedgerAccountDetails | None:
    """Return a cached ``LedgerAccountDetails`` or ``None`` on miss / error.

    ``as_of_date`` may be ``None`` to indicate a live query.  The caller
    should have recorded that fact (see ``one_account_balance``) in order to
    keep the cache key consistent.
    """
    from v4vapp_backend_v2.accounting.account_balances import LedgerAccountDetails

    try:
        gen = await get_cache_generation()
        key = _make_cache_key(gen, account, as_of_date, age, use_checkpoints)
        data: str | None = await InternalConfig.redis_async.get(key)
        if data is not None:
            result = LedgerAccountDetails.model_validate_json(data)
            logger.info(f"{Fore.GREEN}HIT: {key}{Fore.RESET}")
            return result
    except Exception as e:
        logger.info(f"{Fore.RED}miss/error: {e}{Fore.RESET}")
    return None


async def set_cached_balance(
    account: LedgerAccount,
    as_of_date: datetime | None,
    age: timedelta | None,
    result: LedgerAccountDetails,
    ttl: int = DEFAULT_TTL_SECONDS,
    use_checkpoints: bool = True,
    report_time: float | None = None,
) -> None:
    """Store a ``LedgerAccountDetails`` in the cache.

    Uses Pydantic JSON serialisation (preserves ``Decimal`` precision).
    ``as_of_date`` may be ``None`` for live queries; callers are expected to
    signal the original intent so that the key remains stable across minute
    rolls.
    """
    try:
        gen = await get_cache_generation()
        key = _make_cache_key(gen, account, as_of_date, age, use_checkpoints)
        data = result.model_dump_json()
        await InternalConfig.redis_async.setex(key, ttl, data)
        if report_time is not None:
            report_time_str = f"{report_time:.3f}s"
            logger.info(f"SET: {key} (ttl={ttl}s in {report_time_str})")
            if report_time > 1.0:
                logger.warning(f"Slow cache set: {report_time_str} for key {key}")
        else:
            logger.info(f"SET: {key} (ttl={ttl}s)")
    except Exception as e:
        logger.warning(f"Failed to set ledger cache: {e}")
