import asyncio
import importlib.machinery
import importlib.util
import types
from typing import cast

import pytest


def _import_lock_module_fresh() -> types.ModuleType:
    """Load a fresh lock_str module so tests exercise the real implementation."""
    spec = importlib.util.spec_from_file_location(
        "lock_str_real_timeouts",
        "src/v4vapp_backend_v2/process/lock_str_class.py",
    )
    if spec is None or spec.loader is None:
        raise AssertionError("Unable to import lock_str_class.py for timeout tests")
    spec = cast(importlib.machinery.ModuleSpec, spec)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _FakeRedisLock:
    """In-memory stand-in for redis.asyncio.lock.Lock with TTL support."""

    _LOCKS: dict[str, dict[str, float | None]] = {}

    def __init__(
        self,
        _redis,
        name: str,
        timeout: int | float | None = None,
        sleep: float = 0.01,
        blocking: bool = True,
        blocking_timeout: int | float | None = None,
    ) -> None:
        self.name = name
        self.timeout = timeout
        self.sleep = sleep
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout

    @classmethod
    def clear(cls) -> None:
        cls._LOCKS.clear()

    @classmethod
    def _purge_if_expired(cls, name: str) -> None:
        lock_info = cls._LOCKS.get(name)
        if not lock_info:
            return
        expires_at = cast(float | None, lock_info.get("expires_at"))
        if expires_at is not None and asyncio.get_running_loop().time() >= float(expires_at):
            cls._LOCKS.pop(name, None)

    async def acquire(self):
        while True:
            self._purge_if_expired(self.name)
            if self.name not in self._LOCKS:
                expires_at = None
                if self.timeout is not None:
                    expires_at = asyncio.get_running_loop().time() + float(self.timeout)
                self._LOCKS[self.name] = {
                    "expires_at": expires_at,
                }
                return True
            if not self.blocking:
                return False
            await asyncio.sleep(self.sleep)

    async def release(self):
        self._LOCKS.pop(self.name, None)


class _FalseOnAcquireRedisLock:
    def __init__(
        self,
        _redis,
        name: str,
        timeout: int | float | None = None,
        sleep: float = 0.01,
        blocking: bool = True,
        blocking_timeout: int | float | None = None,
    ) -> None:
        self.name = name
        self.timeout = timeout
        self.sleep = sleep
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout

    async def acquire(self):
        await asyncio.sleep(self.sleep)
        return False

    async def release(self):
        return None


@pytest.mark.asyncio
async def test_acquire_lock_respects_blocking_timeout(monkeypatch: pytest.MonkeyPatch):
    mod = _import_lock_module_fresh()
    _FakeRedisLock.clear()

    # Keep tests deterministic and avoid creating background reporter tasks.
    monkeypatch.setattr(mod, "RedisLock", _FakeRedisLock)
    monkeypatch.setattr(mod, "start_lock_reporter", lambda: None)
    mod.InternalConfig.redis_async = object()

    lock_name = mod.LockStr("timeout_blocking")

    acquired = await lock_name.acquire_lock(timeout=None, blocking_timeout=0.2)
    assert acquired is True

    with pytest.raises(mod.CustIDLockException, match="Failed to acquire lock"):
        await lock_name.acquire_lock(timeout=None, blocking_timeout=0.1)


@pytest.mark.asyncio
async def test_acquire_lock_raises_when_lock_acquire_returns_false(
    monkeypatch: pytest.MonkeyPatch,
):
    mod = _import_lock_module_fresh()

    monkeypatch.setattr(mod, "RedisLock", _FalseOnAcquireRedisLock)
    monkeypatch.setattr(mod, "start_lock_reporter", lambda: None)
    mod.InternalConfig.redis_async = object()

    lock_name = mod.LockStr("timeout_false")
    with pytest.raises(mod.CustIDLockException, match="Failed to acquire lock"):
        await lock_name.acquire_lock(timeout=None, blocking_timeout=0.1)


@pytest.mark.asyncio
async def test_acquire_lock_respects_lock_timeout_ttl(monkeypatch: pytest.MonkeyPatch):
    mod = _import_lock_module_fresh()
    _FakeRedisLock.clear()

    monkeypatch.setattr(mod, "RedisLock", _FakeRedisLock)
    monkeypatch.setattr(mod, "start_lock_reporter", lambda: None)
    mod.InternalConfig.redis_async = object()

    lock_name = mod.LockStr("timeout_ttl")

    first_acquired = await lock_name.acquire_lock(timeout=0.05, blocking_timeout=0.2)
    assert first_acquired is True

    # Wait past TTL; next acquire should succeed even without explicit release.
    await asyncio.sleep(0.08)

    second_acquired = await lock_name.acquire_lock(timeout=0.05, blocking_timeout=0.2)
    assert second_acquired is True
