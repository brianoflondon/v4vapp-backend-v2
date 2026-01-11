import importlib.util
import types

import pytest
from redis.exceptions import LockError


def _import_lock_module_fresh() -> types.ModuleType:
    """Load a fresh copy of the lock_str module so tests can exercise the real
    implementation even when other tests patch `LockStr` in the imported module.
    """
    spec = importlib.util.spec_from_file_location(
        "lock_str_real",
        "src/v4vapp_backend_v2/process/lock_str_class.py",
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.asyncio
async def test_release_lock_handles_lockerror_not_owned(monkeypatch):
    mod = _import_lock_module_fresh()

    class DummyRedis:
        def __init__(self):
            self.deleted = False

        async def delete(self, *args):
            self.deleted = True

    dummy = DummyRedis()

    # Inject dummy redis into the module's InternalConfig
    mod.InternalConfig.redis_async = dummy

    async def raise_lock_error(self):
        raise LockError("Cannot release a lock that's not owned or is already unlocked.")

    # Allow constructing a Lock without a full redis client by stubbing __init__
    def dummy_init(self, *args, **kwargs):
        # store name in instance so release() can reference if needed
        self.name = kwargs.get("name") or (args[1] if len(args) > 1 else None)
        return None

    monkeypatch.setattr("redis.asyncio.lock.Lock.__init__", dummy_init, raising=False)
    monkeypatch.setattr("redis.asyncio.lock.Lock.release", raise_lock_error, raising=False)

    res = await mod.LockStr.release_lock("someid")
    assert res is True
    assert dummy.deleted is True


@pytest.mark.asyncio
async def test_release_lock_handles_lockerror_not_owned_variant(monkeypatch):
    mod = _import_lock_module_fresh()

    class DummyRedis:
        def __init__(self):
            self.deleted = False

        async def delete(self, *args):
            self.deleted = True

    dummy = DummyRedis()

    mod.InternalConfig.redis_async = dummy

    async def raise_lock_error(self):
        raise LockError("Lock not owned by this process")

    def dummy_init(self, *args, **kwargs):
        self.name = kwargs.get("name") or (args[1] if len(args) > 1 else None)
        return None

    monkeypatch.setattr("redis.asyncio.lock.Lock.__init__", dummy_init, raising=False)
    monkeypatch.setattr("redis.asyncio.lock.Lock.release", raise_lock_error, raising=False)

    res = await mod.LockStr.release_lock("otherid")
    assert res is True
    assert dummy.deleted is True
