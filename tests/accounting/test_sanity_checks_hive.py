from decimal import Decimal

import asyncio

import pytest

from v4vapp_backend_v2.accounting import sanity_checks
from v4vapp_backend_v2.accounting.sanity_checks import (
    SANITY_CHECK_TIMEOUT_SECONDS,
    SanityCheckResult,
    SanityCheckResults,
)
from v4vapp_backend_v2.helpers.currency_class import Currency


@pytest.mark.asyncio
async def test_hive_task_failure_logged(caplog, monkeypatch):
    """Ensure our new inner timeout/warning around the hive call is exercised.

    We patch `account_hive_balances_async` to sleep a short time, then force the
    overall runner to use very small timeouts so the inner wrapper around the
    task is cancelled. The test asserts that the explicit "hive balance fetch
    failed" warning is emitted.
    """

    # fake async hive call that takes a small amount of time
    async def fake_hive(*args, **kwargs):
        await asyncio.sleep(0.1)
        return {"HIVE": 0, "HBD": 0}

    # minimal account balance result so the other tasks complete quickly
    async def fake_account_balance(*args, **kwargs):
        class Dummy:
            balances_net = {Currency.HIVE: Decimal(0), Currency.HBD: Decimal(0)}

        return Dummy()

    monkeypatch.setattr(sanity_checks, "account_hive_balances_async", fake_hive)
    monkeypatch.setattr(sanity_checks, "one_account_balance", fake_account_balance)

    # avoid loading real config; we just need a server_id attribute
    class DummyConfig:
        server_id = "testserver"

    monkeypatch.setattr(sanity_checks, "InternalConfig", lambda *args, **kwargs: DummyConfig())

    async def _fake_all_held():
        return {}

    monkeypatch.setattr(sanity_checks, "all_held_msats", _fake_all_held)
    monkeypatch.setattr(
        sanity_checks,
        "all_sanity_checks",
        [sanity_checks.server_account_hive_balances],
    )

    orig_to = sanity_checks.asyncio.timeout

    def timeout_override(delay):
        # only shorten the inner hive-fetch duration (20s) while leaving
        # the per‑check and overall timeouts alone so the check itself isn't
        # cancelled immediately.
        if delay == SANITY_CHECK_TIMEOUT_SECONDS - 5:
            return orig_to(0.001)
        return orig_to(delay)

    monkeypatch.setattr(sanity_checks.asyncio, "timeout", timeout_override)

    caplog.set_level("WARNING")

    results: SanityCheckResults = await sanity_checks.run_all_sanity_checks()

    # check that the hive-specific warning appeared
    assert any("hive balance fetch failed" in rec.message for rec in caplog.records)
    # the check catches the exception and reports failure, while the inner
    # hive fetch warning is still logged.
    assert results.failed


@pytest.mark.asyncio
async def test_run_all_sanity_checks_uses_redis_cache(monkeypatch):
    """Ensure run_all_sanity_checks uses Redis cache when available."""

    # Track how many times the check is executed
    calls = {"count": 0}

    async def fake_check(in_progress):
        calls["count"] += 1
        return SanityCheckResult(name="fake", is_valid=True, details="ok")

    async def fake_all_held():
        return {}

    class FakeRedis:
        def __init__(self):
            self.store = {}

        async def get(self, key):
            return self.store.get(key)

        async def setex(self, key, ttl, value):
            self.store[key] = value

    fake_redis = FakeRedis()

    monkeypatch.setattr(sanity_checks.InternalConfig, "redis_async", fake_redis, raising=False)
    monkeypatch.setattr(sanity_checks, "all_sanity_checks", [fake_check])
    monkeypatch.setattr(sanity_checks, "all_held_msats", fake_all_held)

    # first call should execute the check and cache the results
    first = await sanity_checks.run_all_sanity_checks()
    assert calls["count"] == 1

    # second call should hit cache and not execute the check again
    second = await sanity_checks.run_all_sanity_checks()
    assert calls["count"] == 1
    assert first.check_time == second.check_time
    assert first.passed == second.passed


@pytest.mark.asyncio
async def test_run_all_sanity_checks_does_not_cache_failures(monkeypatch):
    """Failure result should not be cached so next call re-runs checks."""

    calls = {"count": 0}

    async def fake_check(in_progress):
        calls["count"] += 1
        return SanityCheckResult(name="fake", is_valid=False, details="bad")

    async def fake_all_held():
        return {}

    class FakeRedis:
        def __init__(self):
            self.store = {}

        async def get(self, key):
            return self.store.get(key)

        async def setex(self, key, ttl, value):
            self.store[key] = value

    fake_redis = FakeRedis()
    monkeypatch.setattr(sanity_checks.InternalConfig, "redis_async", fake_redis, raising=False)
    monkeypatch.setattr(sanity_checks, "all_sanity_checks", [fake_check])
    monkeypatch.setattr(sanity_checks, "all_held_msats", fake_all_held)

    first = await sanity_checks.run_all_sanity_checks()
    assert calls["count"] == 1
    assert first.failed

    second = await sanity_checks.run_all_sanity_checks()
    assert calls["count"] == 2
    assert second.failed


@pytest.mark.asyncio
async def test_server_account_hive_balances_open_orders_show_info(monkeypatch):
    """Mismatch with open orders should be shown but flagged as pass."""

    async def fake_one_account_balance(*args, **kwargs):
        account = kwargs.get("account") or (args[0] if args else None)

        class Dummy:
            balances_net = {Currency.HIVE: Decimal("0.000"), Currency.HBD: Decimal("0.000")}

        result = Dummy()
        if account and getattr(account, "name", "") == "Customer Deposits Hive":
            result.balances_net = {
                Currency.HIVE: Decimal("5697.668"),
                Currency.HBD: Decimal("860.061"),
            }
        elif account and getattr(account, "name", "") == "Traded Deposits Hive":
            result.balances_net = {Currency.HIVE: Decimal("0.000"), Currency.HBD: Decimal("0.000")}
        return result

    async def fake_account_hive_balances_async(hive_accname):
        return {"HIVE": Decimal("5697.668"), "HBD": Decimal("1000.0")}

    class DummyConfig:
        server_id = "v4vapp"

    async def fake_all_held():
        return {}

    class FakeAmount:
        def __init__(self, value):
            self.amount_decimal = Decimal(value)

    monkeypatch.setattr(sanity_checks, "Amount", FakeAmount)
    monkeypatch.setattr(sanity_checks, "one_account_balance", fake_one_account_balance)
    monkeypatch.setattr(sanity_checks, "account_hive_balances_async", fake_account_hive_balances_async)
    monkeypatch.setattr(sanity_checks, "InternalConfig", lambda *args, **kwargs: DummyConfig())
    monkeypatch.setattr(
        sanity_checks,
        "LimitOrderCreate",
        type(
            "L",
            (),
            {
                "get_hive_open_orders": staticmethod(
                    lambda: [{"orderid": 946901416}, {"orderid": 1364100027}]
                )
            },
        ),
    )
    monkeypatch.setattr(sanity_checks, "all_held_msats", fake_all_held)
    monkeypatch.setattr(
        sanity_checks, "all_sanity_checks", [sanity_checks.server_account_hive_balances]
    )

    results = await sanity_checks.run_all_sanity_checks(use_cache=False)
    entry = next(
        (r for name, r in results.results if name == "server_account_hive_balances"), None
    )
    assert entry is not None
    assert entry.is_valid is True
    assert (
        "Open Hive orders may be affecting balances. Open order IDs: 946901416, 1364100027."
        in entry.details
    )
    assert entry.details.startswith("**Server Hive Mismatch:**\n")
    assert "0.000 HIVE" in entry.details
    assert "-139.939 HBD" in entry.details
