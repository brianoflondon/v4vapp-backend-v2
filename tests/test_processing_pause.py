"""Redis pause flag, unpause event, and the admin-screen control."""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from v4vapp_backend_v2.process import processing_pause
from v4vapp_backend_v2.process.processing_pause import (
    PAUSE_MESSAGE,
    UNPAUSE_MESSAGE,
    _control_reason,
    apply_pause_flag_from_redis,
    get_processing_pause,
    is_processing_paused,
    reset_processing_pause_for_tests,
    set_local_pause,
    set_processing_paused,
    wait_until_paused_or_shutdown,
    wait_while_paused,
)


@pytest.fixture(autouse=True)
def _reset_pause_state():
    reset_processing_pause_for_tests()
    yield
    reset_processing_pause_for_tests()


class _FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}
        self.published: list[tuple[str, str]] = []

    async def get(self, name):
        return self.store.get(name)

    async def set(self, name, value):
        self.store[name] = value
        return True

    async def delete(self, name):
        self.store.pop(name, None)
        return 1

    async def publish(self, channel, message):
        self.published.append((channel, message))
        return 1


@pytest.fixture
def fake_redis(monkeypatch):
    fake = _FakeRedis()
    monkeypatch.setattr(processing_pause, "pause_redis_key", lambda: "db_monitor:processing_paused:test")
    monkeypatch.setattr(
        processing_pause, "pause_channel", lambda: "db_monitor:processing_paused:test:events"
    )
    monkeypatch.setattr(processing_pause.InternalConfig, "redis_async", fake)
    return fake


def test_pause_control_sits_under_sanity_checks():
    template = Path("src/v4vapp_backend_v2/admin/templates/base.html.jinja").read_text()
    sanity_at = template.index('id="sanity-sidebar-placeholder"')
    toggle_at = template.index('id="processing-pause-toggle"')
    assert toggle_at > sanity_at
    assert "Are you sure you want to pause all processing?" in template
    assert "/admin/api/processing-pause" in template


def test_control_reason_names_the_unpause_event():
    assert _control_reason({"data": UNPAUSE_MESSAGE}) == "unpause event"
    assert _control_reason({"data": PAUSE_MESSAGE}) == "pause event"
    assert _control_reason({"data": b"unpause"}) == "unpause event"
    assert _control_reason(None) == "redis flag"


@pytest.mark.asyncio
async def test_set_processing_paused_writes_the_flag_and_publishes(fake_redis):
    paused = await set_processing_paused(True)
    assert paused["paused"] is True
    assert fake_redis.store["db_monitor:processing_paused:test"] == paused["paused_at"]
    assert fake_redis.published == [
        ("db_monitor:processing_paused:test:events", PAUSE_MESSAGE),
    ]

    running = await set_processing_paused(False)
    assert running == {"paused": False, "paused_at": None}
    assert "db_monitor:processing_paused:test" not in fake_redis.store
    assert fake_redis.published[-1] == (
        "db_monitor:processing_paused:test:events",
        UNPAUSE_MESSAGE,
    )
    assert await get_processing_pause() == {"paused": False, "paused_at": None}


@pytest.mark.asyncio
async def test_apply_flag_pauses_and_an_unpause_event_clears_it(fake_redis):
    fake_redis.store["db_monitor:processing_paused:test"] = "2026-10-28 12:00:00 UTC"
    await apply_pause_flag_from_redis("pause event")
    assert is_processing_paused()

    del fake_redis.store["db_monitor:processing_paused:test"]
    await apply_pause_flag_from_redis("unpause event")
    assert not is_processing_paused()


@pytest.mark.asyncio
async def test_a_failed_flag_read_keeps_the_current_pause(fake_redis, monkeypatch):
    set_local_pause(True, reason="test")

    async def broken_read():
        raise ConnectionError("redis down")

    monkeypatch.setattr(processing_pause, "get_processing_pause", broken_read)
    await apply_pause_flag_from_redis("redis flag")
    assert is_processing_paused()


@pytest.mark.asyncio
async def test_wait_while_paused_returns_on_the_unpause_event():
    set_local_pause(True, reason="test")
    shutdown = asyncio.Event()

    async def unpause_soon():
        await asyncio.sleep(0.01)
        set_local_pause(False, reason="unpause event")

    asyncio.create_task(unpause_soon())
    await asyncio.wait_for(wait_while_paused(shutdown, "hive_ops"), timeout=2)
    assert not is_processing_paused()


@pytest.mark.asyncio
async def test_wait_until_paused_notices_the_redis_flag(fake_redis, monkeypatch):
    monkeypatch.setattr(processing_pause, "POLL_SECONDS", 0.01)
    fake_redis.store["db_monitor:processing_paused:test"] = "2026-10-28 12:00:00 UTC"
    shutdown = asyncio.Event()
    await asyncio.wait_for(wait_until_paused_or_shutdown(shutdown), timeout=2)
    assert is_processing_paused()


@pytest.mark.asyncio
async def test_stall_checks_stay_quiet_for_the_pause_grace(monkeypatch):
    from v4vapp_backend_v2.process.process_overwatch import Overwatch

    Overwatch.reset()
    Overwatch._suppress_stalls_until = datetime.now(tz=UTC) + timedelta(minutes=5)

    def boom(self):
        raise AssertionError("stall scan ran during the quiet period")

    monkeypatch.setattr(Overwatch, "active_flows", property(boom))
    try:
        result = await Overwatch().check_stalls()
    finally:
        Overwatch.reset()
    assert result == []
