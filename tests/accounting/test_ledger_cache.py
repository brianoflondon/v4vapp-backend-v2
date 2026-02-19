"""
Tests for the ledger balance cache (generation-based invalidation).

These tests use the same test DB fixture as test_account_balances, then exercise
the cache module's get / set / invalidate flow.
"""

import json
from pathlib import Path

import pytest
from bson import json_util

from v4vapp_backend_v2.accounting.account_balances import one_account_balance
from v4vapp_backend_v2.accounting.accounting_classes import LedgerAccountDetails
from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_cache import (
    GENERATION_KEY,
    get_cache_generation,
    get_cached_balance,
    invalidate_ledger_cache,
    set_cached_balance,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn


@pytest.fixture(scope="module")
def module_monkeypatch():
    from _pytest.monkeypatch import MonkeyPatch

    monkey_patch = MonkeyPatch()
    yield monkey_patch
    monkey_patch.undo()


@pytest.fixture(autouse=True, scope="module")
async def setup_test_db(module_monkeypatch):
    test_config_path = Path("tests/data/config")
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    module_monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    i_c = InternalConfig()
    db_conn = DBConn()
    await db_conn.setup_database()

    # Load test ledger data
    await InternalConfig.db["ledger"].drop()
    with open("tests/accounting/test_data/v4vapp-dev.ledger.json") as f:
        raw_data = f.read()
        json_data = json.loads(raw_data, object_hook=json_util.object_hook)
    for entry_raw in json_data:
        entry = LedgerEntry.model_validate(entry_raw)
        await entry.save()

    # Reset the generation counter for a clean test slate
    await InternalConfig.redis_async.delete(GENERATION_KEY)

    yield

    # Cleanup: drop ledger, reset singleton, clean up test cache keys
    await InternalConfig.db["ledger"].drop()
    # Remove any test cache keys (they expire via TTL anyway)
    try:
        gen = await get_cache_generation()
        # Use SCAN to find and delete test keys (small number expected)
        cursor_val = 0
        while True:
            cursor_val, keys = await InternalConfig.redis_async.scan(
                cursor_val, match="ledger:*", count=100
            )
            if keys:
                await InternalConfig.redis_async.delete(*keys)
            if cursor_val == 0:
                break
    except Exception:
        pass
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


# ---- Tests ----


async def test_generation_starts_at_zero():
    """If the generation key doesn't exist, generation should be 0."""
    await InternalConfig.redis_async.delete(GENERATION_KEY)
    assert await get_cache_generation() == 0


async def test_invalidate_increments_generation():
    """Each call to invalidate_ledger_cache should increment the generation."""
    await InternalConfig.redis_async.delete(GENERATION_KEY)
    gen1 = await invalidate_ledger_cache()
    assert gen1 == 1
    gen2 = await invalidate_ledger_cache()
    assert gen2 == 2


async def test_cache_miss_returns_none():
    """A cold cache should return None."""
    account = LiabilityAccount(name="VSC Liability", sub="v4vapp-test")
    from datetime import datetime, timezone

    result = await get_cached_balance(account, datetime.now(tz=timezone.utc), None)
    assert result is None


async def test_set_and_get_cached_balance():
    """Storing then retrieving a balance should return an equivalent object."""
    account = LiabilityAccount(name="VSC Liability", sub="v4vapp-test")

    # Get a real balance from the DB
    balance = await one_account_balance(account=account, use_cache=False)
    assert isinstance(balance, LedgerAccountDetails)

    from datetime import datetime, timezone

    as_of = datetime.now(tz=timezone.utc)

    await set_cached_balance(account, as_of, None, balance, ttl=30)
    cached = await get_cached_balance(account, as_of, None)

    assert cached is not None
    assert isinstance(cached, LedgerAccountDetails)
    assert cached.sub == balance.sub
    assert cached.name == balance.name
    assert cached.hive == balance.hive
    assert cached.hbd == balance.hbd
    assert cached.msats == balance.msats


async def test_invalidation_orphans_cached_entries():
    """After invalidation, previously cached entries should not be found."""
    account = LiabilityAccount(name="VSC Liability", sub="v4vapp-test")
    from datetime import datetime, timezone

    as_of = datetime.now(tz=timezone.utc)

    balance = await one_account_balance(account=account, use_cache=False)
    await set_cached_balance(account, as_of, None, balance, ttl=30)

    # Confirm it's cached
    assert await get_cached_balance(account, as_of, None) is not None

    # Invalidate
    await invalidate_ledger_cache()

    # Now the old cache entry should be missed (wrong generation)
    assert await get_cached_balance(account, as_of, None) is None


async def test_one_account_balance_uses_cache():
    """Calling one_account_balance twice should return cached on the second call."""
    await InternalConfig.redis_async.delete(GENERATION_KEY)
    account = LiabilityAccount(name="VSC Liability", sub="v4vapp-test")

    # First call — populates cache
    result1 = await one_account_balance(account=account, use_cache=True)
    assert isinstance(result1, LedgerAccountDetails)

    # Second call — should hit cache (we can't easily assert this without
    # mocking, but we verify the result is equivalent)
    result2 = await one_account_balance(account=account, use_cache=True)
    assert isinstance(result2, LedgerAccountDetails)
    assert result2.sub == result1.sub
    assert result2.hive == result1.hive
    assert result2.hbd == result1.hbd
    assert result2.msats == result1.msats


async def test_use_cache_false_bypasses_cache():
    """use_cache=False should skip the cache entirely."""
    account = LiabilityAccount(name="VSC Liability", sub="v4vapp-test")

    result = await one_account_balance(account=account, use_cache=False)
    assert isinstance(result, LedgerAccountDetails)
