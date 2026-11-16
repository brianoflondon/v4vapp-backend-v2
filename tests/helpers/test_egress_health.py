"""One probe decides the outbound path is down, and consumers stop calling out."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

import hive_monitor_v2
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.helpers.crypto_prices import (
    LAST_GOOD_QUOTE_REDIS_KEY,
    AllQuotes,
    Binance,
    CoinGecko,
    CoinMarketCap,
    HiveInternalMarket,
    QuoteResponse,
    save_last_good_quote,
)
from v4vapp_backend_v2.helpers.egress_health import (
    CONNECT_TIMEOUT_S,
    DOWN_KEY,
    DOWN_TTL_S,
    FAIL_THRESHOLD,
    LOCK_KEY,
    PROBE_TARGETS,
    SKIPPED_KEY,
    HostProbe,
    ProbeReport,
    ProbeTarget,
    _alert,
    _hold_probe_lock,
    degraded_blocks_restart,
    egress_is_down,
    format_outage_length,
    log_degraded,
    pause_outbound,
    probe_host,
    probe_once,
    probes_enabled,
    publish_probe,
    reset_for_tests,
    server_name,
    start_egress_probe,
)
from v4vapp_backend_v2.witness_monitor.witness_events import send_kuma_heartbeat

STARTED = datetime(2026, 11, 16, 4, 22, 25, tzinfo=UTC)
FAILED = ["api.binance.com", "api.coingecko.com", "api.hive.blog"]
IPV4 = {
    "api.binance.com": "203.0.113.1",
    "api.coingecko.com": "203.0.113.2",
    "api.hive.blog": "203.0.113.3",
}


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}
        self.hashes: dict[str, dict[str, int]] = {}
        self.expiry: dict[str, int | None] = {}

    def get(self, key: str):
        return self.values.get(key)

    def set(self, key: str, value, ex: int | None = None, nx: bool = False):
        if nx and key in self.values:
            return None
        self.values[key] = value
        self.expiry[key] = ex
        return True

    def delete(self, key: str):
        self.values.pop(key, None)
        self.hashes.pop(key, None)
        return 1

    def expire(self, key: str, ttl: int):
        self.expiry[key] = ttl
        return True

    def hincrby(self, key: str, field: str, amount: int):
        bucket = self.hashes.setdefault(key, {})
        bucket[field] = int(bucket.get(field, 0)) + int(amount)
        return bucket[field]

    def hgetall(self, key: str):
        return dict(self.hashes.get(key, {}))


class MemoryRedis:
    """The last-good tests use a client with get and set only."""

    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}

    def get(self, key: str):
        return self.store.get(key)

    def set(self, name: str, value: bytes, ex: int | None = None):
        self.store[name] = value
        return True


@pytest.fixture(autouse=True)
def clean_egress():
    reset_for_tests()
    yield
    reset_for_tests()


@pytest.fixture
def memory_redis(monkeypatch: pytest.MonkeyPatch) -> MemoryRedis:
    redis = MemoryRedis()

    def _keep_test_redis(self) -> None:
        InternalConfig.redis = redis

    monkeypatch.setattr(InternalConfig, "setup_redis", _keep_test_redis)
    monkeypatch.setattr(InternalConfig, "redis", redis)
    return redis


def _good_quote() -> QuoteResponse:
    return QuoteResponse(
        hive_usd=Decimal("0.25"),
        hbd_usd=Decimal("1.01"),
        btc_usd=Decimal("80000"),
        hive_hbd=Decimal("0.2475"),
        source="Binance",
        fetch_date=datetime.now(tz=UTC),
        error="",
    )


def _report(failed: list[str]) -> ProbeReport:
    hosts = []
    for target in PROBE_TARGETS:
        reached = target.name not in failed
        hosts.append(
            HostProbe(
                target.name,
                ok=reached,
                ipv4="ok" if reached else "fail",
                ipv6="skip",
            )
        )
    return ProbeReport(tuple(hosts))


def _patch_sources(mocker, quote_for):
    async def fake_get_quote(self, use_cache=True):
        return quote_for(type(self).__name__)

    for cls in (CoinGecko, Binance, CoinMarketCap, HiveInternalMarket):
        mocker.patch.object(cls, "get_quote", fake_get_quote)


async def _resolve(host: str, port: int) -> dict[str, str | None]:
    return {"ipv4": IPV4.get(host), "ipv6": None}


def _connect(up: set[str]):
    async def connect(ip: str, port: int, timeout: float) -> bool:
        assert timeout == CONNECT_TIMEOUT_S
        return ip in up

    return connect


def test_three_of_four_is_down_and_two_is_up():
    assert FAIL_THRESHOLD == 3
    assert len(PROBE_TARGETS) == 4
    assert _report(FAILED).down is True
    assert _report(FAILED[:2]).down is False
    assert _report([]).down is False


@pytest.mark.asyncio
async def test_probe_marks_down_only_when_three_hosts_fail():
    down = await probe_once(connect=_connect({"1.1.1.1"}), resolve=_resolve)
    assert down.down is True
    assert down.failed == FAILED

    up = await probe_once(
        connect=_connect({"1.1.1.1", "203.0.113.1", "203.0.113.2"}),
        resolve=_resolve,
    )
    assert up.down is False
    assert up.failed == ["api.hive.blog"]


@pytest.mark.asyncio
async def test_a_host_is_up_when_either_family_connects():
    target = ProbeTarget("example", host="example.com")

    async def resolve(host: str, port: int) -> dict[str, str | None]:
        return {"ipv4": "203.0.113.9", "ipv6": "2001:db8::9"}

    async def ipv6_only(ip: str, port: int, timeout: float) -> bool:
        return ip == "2001:db8::9"

    host = await probe_host(target, connect=ipv6_only, resolve=resolve)
    assert host.ok is True
    assert host.ipv4 == "fail"
    assert host.ipv6 == "ok"

    async def neither(ip: str, port: int, timeout: float) -> bool:
        return False

    failed = await probe_host(target, connect=neither, resolve=resolve)
    assert failed.ok is False
    assert failed.ipv4 == "fail"
    assert failed.ipv6 == "fail"


@pytest.mark.asyncio
async def test_missing_addresses_mark_the_host_down():
    target = ProbeTarget("example", host="example.com")

    async def resolve(host: str, port: int) -> dict[str, str | None]:
        return {"ipv4": None, "ipv6": None}

    async def connect(ip: str, port: int, timeout: float) -> bool:
        raise AssertionError("connect without an address")

    host = await probe_host(target, connect=connect, resolve=resolve)
    assert host.ok is False
    assert host.ipv4 == "skip"
    assert host.ipv6 == "skip"

    async def broken(host: str, port: int) -> dict[str, str | None]:
        raise OSError("dns")

    assert (await probe_host(target, connect=connect, resolve=broken)).ok is False


def test_down_alerts_once_and_restore_reports_skips():
    redis = FakeRedis()
    alerts: list[str] = []
    first = publish_probe(
        _report(FAILED),
        redis=redis,
        server="gad",
        now=STARTED,
        alerter=alerts.append,
    )
    assert first == "down"
    stored = json.loads(redis.get(DOWN_KEY))
    assert stored["since"] == STARTED.isoformat()
    assert stored["outage_id"] == STARTED.isoformat()
    assert stored["failed_hosts"] == FAILED
    assert redis.expiry[DOWN_KEY] == DOWN_TTL_S
    assert alerts == [
        (
            "Outbound internet down on gad (since 04:22:25 UTC) "
            "failed=api.binance.com,api.coingecko.com,api.hive.blog"
        )
    ]

    later = STARTED + timedelta(seconds=8)
    again = publish_probe(
        _report(FAILED),
        redis=redis,
        server="gad",
        now=later,
        alerter=alerts.append,
    )
    assert again is None
    refreshed = json.loads(redis.get(DOWN_KEY))
    assert refreshed["since"] == stored["since"]
    assert refreshed["outage_id"] == stored["outage_id"]
    assert len(alerts) == 1

    redis.hashes[SKIPPED_KEY] = {"quotes": 2, "binance": 1}
    restored_at = STARTED + timedelta(seconds=90)
    restored = publish_probe(
        _report([]),
        redis=redis,
        server="gad",
        now=restored_at,
        alerter=alerts.append,
    )
    assert restored == "restored"
    assert DOWN_KEY not in redis.values
    assert SKIPPED_KEY not in redis.hashes
    assert alerts[1] == (
        "Outbound internet restored on gad after 2 min. skipped binance=1, quotes=2"
    )


def test_restore_under_a_minute_and_a_quiet_up_probe():
    redis = FakeRedis()
    alerts: list[str] = []
    publish_probe(_report(FAILED), redis=redis, server="gad", now=STARTED, alerter=alerts.append)
    moment = STARTED + timedelta(seconds=45)
    assert (
        publish_probe(_report([]), redis=redis, server="gad", now=moment, alerter=alerts.append)
        == "restored"
    )
    assert alerts[1] == "Outbound internet restored on gad after 45 s. skipped none"

    quiet: list[str] = []
    quiet_result = publish_probe(
        _report([]),
        redis=redis,
        server="gad",
        now=moment,
        alerter=quiet.append,
    )
    assert quiet_result is None
    assert quiet == []


def test_lengths_and_alert_shape(mocker):
    assert format_outage_length(STARTED.isoformat(), STARTED + timedelta(seconds=45)) == "45 s"
    assert format_outage_length(STARTED.isoformat(), STARTED + timedelta(seconds=90)) == "2 min"
    warn = mocker.patch("v4vapp_backend_v2.helpers.egress_health.logger.warning")
    _alert("Outbound internet down on gad")
    extra = warn.call_args.kwargs["extra"]
    assert extra["notification"] is True
    assert "error_code" not in extra


def test_degraded_log_is_once_per_minute_and_does_not_page(mocker):
    warn = mocker.patch("v4vapp_backend_v2.helpers.egress_health.logger.warning")
    assert log_degraded("quotes", "serving cache") is True
    assert log_degraded("quotes", "serving cache") is False
    assert log_degraded("binance", "paused") is True
    assert warn.call_count == 2
    assert warn.call_args_list[0].kwargs["extra"]["notification"] is False


def test_pause_and_restart_suppression_follow_the_flag(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(InternalConfig, "redis", redis)
    assert pause_outbound("quotes", "paused") is False
    assert degraded_blocks_restart("stall") is False
    assert redis.hashes == {}

    redis.values[DOWN_KEY] = json.dumps({"down": True, "since": STARTED.isoformat()})
    reset_for_tests()
    assert pause_outbound("quotes", "paused") is True
    assert pause_outbound("quotes", "paused") is True
    assert degraded_blocks_restart("5 empty restarts") is True
    assert redis.hashes[SKIPPED_KEY] == {"quotes": 2, "hive_restart": 1}


def test_skip_counter_ignores_a_redis_client_without_hashes(monkeypatch):
    redis = MemoryRedis()
    monkeypatch.setattr(InternalConfig, "redis", redis)
    redis.store[DOWN_KEY] = json.dumps({"down": True}).encode()
    assert pause_outbound("kuma", "skip") is True


def test_missing_or_broken_redis_fails_open(monkeypatch):
    monkeypatch.setattr(InternalConfig, "redis", None)
    assert egress_is_down() is False

    class Boom:
        def get(self, key):
            raise RuntimeError("redis down")

    monkeypatch.setattr(InternalConfig, "redis", Boom())
    reset_for_tests()
    assert egress_is_down() is False


def test_flag_cache_clears_on_reset(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(InternalConfig, "redis", redis)
    redis.values[DOWN_KEY] = json.dumps({"down": True})
    assert egress_is_down() is True
    redis.values.pop(DOWN_KEY)
    assert egress_is_down() is True
    reset_for_tests()
    assert egress_is_down() is False


def test_server_name_falls_back(monkeypatch):
    monkeypatch.delenv("LOCAL_MACHINE_NAME", raising=False)

    def boom():
        raise RuntimeError("no config")

    monkeypatch.setattr("v4vapp_backend_v2.helpers.egress_health.InternalConfig", boom)
    assert server_name() == "unknown"


def test_probe_lock_is_held_by_one_token():
    redis = FakeRedis()
    assert _hold_probe_lock(redis, "leader") is True
    assert _hold_probe_lock(redis, "other") is False
    assert _hold_probe_lock(redis, "leader") is True
    assert redis.expiry[LOCK_KEY] == 25


def test_probe_lock_accepts_a_client_without_nx():
    class NoNx(FakeRedis):
        def set(self, key, value, ex=None, nx=False):
            if nx:
                raise TypeError("nx")
            return super().set(key, value, ex=ex)

    redis = NoNx()
    assert _hold_probe_lock(redis, "leader") is True
    assert redis.get(LOCK_KEY) == "leader"


def test_probe_stays_off_under_pytest():
    assert probes_enabled() is False
    assert start_egress_probe() is None


@pytest.mark.asyncio
async def test_start_egress_probe_is_idempotent(mocker):
    mocker.patch("v4vapp_backend_v2.helpers.egress_health.probes_enabled", return_value=True)

    async def idle(shutdown_event=None):
        await asyncio.Event().wait()

    mocker.patch("v4vapp_backend_v2.helpers.egress_health.egress_probe_loop", idle)
    first = start_egress_probe()
    second = start_egress_probe()
    assert first is second
    assert first is not None
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first


@pytest.mark.asyncio
async def test_quotes_serve_last_good_and_do_not_fetch_while_down(mocker, memory_redis):
    save_last_good_quote(_good_quote())
    memory_redis.store[DOWN_KEY] = json.dumps({"down": True}).encode()
    reset_for_tests()
    calls: list[bool] = []
    real = AllQuotes._fallback_last_good

    def wrapped(self, announce: bool = True):
        calls.append(announce)
        return real(self, announce=announce)

    mocker.patch.object(AllQuotes, "_fallback_last_good", wrapped)
    mocker.patch(
        "v4vapp_backend_v2.helpers.crypto_prices.enabled_quote_service_clients",
        side_effect=AssertionError("quote fetch while egress is down"),
    )
    quotes = AllQuotes()
    await quotes.get_all_quotes(use_cache=False, store_db=False)
    assert calls == [False]
    assert quotes.quote.stale is True
    assert quotes.quote.btc_usd == Decimal("80000")
    assert quotes.quote.error == ""
    assert LAST_GOOD_QUOTE_REDIS_KEY in memory_redis.store


@pytest.mark.asyncio
async def test_fresh_global_cache_is_served_unstale_while_down(mocker, memory_redis):
    _patch_sources(mocker, lambda name: _good_quote())
    first = AllQuotes()
    await first.get_all_quotes(use_cache=False, store_db=False)
    assert first.quote.stale is False

    memory_redis.store[DOWN_KEY] = json.dumps({"down": True}).encode()
    reset_for_tests()
    mocker.patch(
        "v4vapp_backend_v2.helpers.crypto_prices.enabled_quote_service_clients",
        side_effect=AssertionError("quote fetch while egress is down"),
    )
    second = AllQuotes()
    await second.get_all_quotes(use_cache=False, store_db=False)
    assert second.quote.stale is False
    assert second.quote.btc_usd == Decimal("80000")


@pytest.mark.asyncio
async def test_kuma_heartbeat_returns_before_http_while_down(mocker):
    mocker.patch(
        "v4vapp_backend_v2.witness_monitor.witness_events.pause_outbound",
        return_value=True,
    )
    client = mocker.patch("v4vapp_backend_v2.witness_monitor.witness_events.httpx.AsyncClient")
    await send_kuma_heartbeat()
    client.assert_not_called()


async def _hold_hive_tasks():
    await asyncio.sleep(60)


def _hive_tasks():
    return [
        asyncio.create_task(_hold_hive_tasks(), name="all_ops_loop"),
        asyncio.create_task(_hold_hive_tasks(), name="store_rates"),
    ]


async def _stop_hive_tasks(tasks):
    for task in tasks:
        task.cancel()
    for task in tasks:
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.fixture
def hive_state():
    saved_status = hive_monitor_v2.STATUS_OBJ
    startup_was_set = hive_monitor_v2.startup_complete_event.is_set()
    hive_monitor_v2.STATUS_OBJ = hive_monitor_v2.StatusObject()
    hive_monitor_v2.startup_complete_event.clear()
    yield hive_monitor_v2
    hive_monitor_v2.STATUS_OBJ = saved_status
    if startup_was_set:
        hive_monitor_v2.startup_complete_event.set()
    else:
        hive_monitor_v2.startup_complete_event.clear()


@pytest.mark.asyncio
async def test_hive_health_ignores_stale_progress_while_down(hive_state, mocker):
    from status.status_api import StatusAPIException

    tasks = _hive_tasks()
    hive_state.startup_complete_event.set()
    hive_state.STATUS_OBJ.last_progress_at = datetime.now(tz=UTC) - timedelta(
        seconds=hive_state.HEALTH_STALE_PROGRESS_SECONDS + 30
    )
    mocker.patch.object(hive_state, "egress_is_down", return_value=True)
    logged = mocker.patch.object(hive_state, "log_degraded")
    blocked = mocker.patch.object(hive_state, "degraded_blocks_restart")
    try:
        result = await hive_state.health_check()
        assert "last_progress_age_s" in result
        logged.assert_called_once()
        assert logged.call_args.args[0] == "hive_health"
        blocked.assert_not_called()

        hive_state.STATUS_OBJ.fatal_reason = "block drift"
        with pytest.raises(StatusAPIException, match="fatal"):
            await hive_state.health_check()
    finally:
        await _stop_hive_tasks(tasks)


@pytest.mark.asyncio
async def test_hive_health_still_fails_on_stale_progress_when_up(hive_state, mocker):
    from status.status_api import StatusAPIException

    tasks = _hive_tasks()
    hive_state.startup_complete_event.set()
    hive_state.STATUS_OBJ.last_progress_at = datetime.now(tz=UTC) - timedelta(
        seconds=hive_state.HEALTH_STALE_PROGRESS_SECONDS + 30
    )
    mocker.patch.object(hive_state, "egress_is_down", return_value=False)
    try:
        with pytest.raises(StatusAPIException, match="no stream progress"):
            await hive_state.health_check()
    finally:
        await _stop_hive_tasks(tasks)
