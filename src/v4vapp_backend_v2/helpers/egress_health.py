"""One outbound-path check per server, shared through that server's Redis.

Gad and Edi keep separate Redis instances. This module writes ``egress:down``
on the Redis of the process that runs it. It does not read another host's flag.
A single leader probes every few seconds. Quote fetches, the Binance monitor,
and the Kuma push skip their own outbound calls while the flag is set, and the
Hive monitor does not exit for a stream stall that a restart cannot fix.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from v4vapp_backend_v2.config.setup import InternalConfig, logger

PROBE_INTERVAL_S = 8.0
CONNECT_TIMEOUT_S = 2.0
DOWN_TTL_S = 30
LOCK_TTL_S = 25
FAIL_THRESHOLD = 3
DEGRADED_LOG_INTERVAL_S = 60.0

DOWN_KEY = "egress:down"
LOCK_KEY = "egress:probe_lock"
SKIPPED_KEY = "egress:skipped"


@dataclass(frozen=True)
class ProbeTarget:
    name: str
    port: int = 443
    host: str = ""
    ipv4: str | None = None
    ipv6: str | None = None


# Four unrelated targets. A host is up when either address family connects.
PROBE_TARGETS = (
    ProbeTarget("api.binance.com", host="api.binance.com"),
    ProbeTarget("api.coingecko.com", host="api.coingecko.com"),
    ProbeTarget("api.hive.blog", host="api.hive.blog"),
    ProbeTarget("cloudflare", ipv4="1.1.1.1", ipv6="2606:4700:4700::1111"),
)

ConnectFn = Callable[[str, int, float], Awaitable[bool]]
ResolveFn = Callable[[str, int], Awaitable[dict[str, str | None]]]

_last_degraded_log: dict[str, float] = {}
_cached_down = False
_cached_at = 0.0
_CACHE_S = 2.0
_probe_task: asyncio.Task | None = None


@dataclass(frozen=True)
class HostProbe:
    name: str
    ok: bool
    ipv4: str
    ipv6: str


@dataclass(frozen=True)
class ProbeReport:
    hosts: tuple[HostProbe, ...]

    @property
    def failed(self) -> list[str]:
        return [host.name for host in self.hosts if not host.ok]

    @property
    def down(self) -> bool:
        return len(self.failed) >= FAIL_THRESHOLD


def probes_enabled() -> bool:
    """The background probe stays off under pytest and when EGRESS_PROBE=0."""
    flag = os.environ.get("EGRESS_PROBE", "1").lower()
    if flag in {"0", "false", "no"}:
        return False
    return "pytest" not in sys.modules and not os.environ.get("PYTEST_CURRENT_TEST")


def reset_for_tests() -> None:
    """Drop the in-process flag cache and the per-minute log gate."""
    global _cached_down, _cached_at, _probe_task
    _cached_down = False
    _cached_at = 0.0
    _last_degraded_log.clear()
    _probe_task = None


def _note_local_state(down: bool) -> None:
    global _cached_down, _cached_at
    _cached_down = down
    _cached_at = time.monotonic()


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _decode_json(raw: Any) -> dict | None:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode()
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def _client():
    """The process Redis. Tests replace InternalConfig.redis; production sets it at startup."""
    return getattr(InternalConfig, "redis", None)


def egress_is_down() -> bool:
    """True when this server's probe has marked the public outbound path down.

    Missing Redis, a missing key, or a bad payload means the path is treated as
    up, so a Redis fault does not pause every outbound call.
    """
    global _cached_down, _cached_at
    now = time.monotonic()
    if now - _cached_at < _CACHE_S:
        return _cached_down
    down = False
    try:
        client = _client()
        if client is not None:
            data = _decode_json(client.get(DOWN_KEY))
            down = isinstance(data, dict) and data.get("down") is True
    except Exception:
        down = False
    _cached_down = down
    _cached_at = now
    return down


def record_skipped(subsystem: str) -> None:
    """Count a call this subsystem did not make while the path was down."""
    try:
        client = _client()
        if client is None or not hasattr(client, "hincrby"):
            return
        client.hincrby(SKIPPED_KEY, subsystem, 1)
        if hasattr(client, "expire"):
            client.expire(SKIPPED_KEY, 6 * 60 * 60)
    except Exception:
        return


def log_degraded(subsystem: str, message: str) -> bool:
    """Log at most one WARNING per minute for a subsystem. Never pages Telegram."""
    now = time.monotonic()
    last = _last_degraded_log.get(subsystem, 0.0)
    if now - last < DEGRADED_LOG_INTERVAL_S:
        return False
    _last_degraded_log[subsystem] = now
    logger.warning(
        message,
        extra={"notification": False, "subsystem": subsystem, "egress": "down"},
    )
    return True


def pause_outbound(subsystem: str, message: str) -> bool:
    """True when the caller should serve cache and skip the outbound call."""
    if not egress_is_down():
        return False
    record_skipped(subsystem)
    log_degraded(subsystem, message)
    return True


def degraded_blocks_restart(reason: str) -> bool:
    """True when a Hive-monitor fatal exit would not repair the network."""
    if not egress_is_down():
        return False
    record_skipped("hive_restart")
    log_degraded("hive_restart", f"restart suppressed: egress down ({reason})")
    return True


def server_name() -> str:
    try:
        name = getattr(InternalConfig(), "local_machine_name", "") or ""
    except Exception:
        name = ""
    return name or os.environ.get("LOCAL_MACHINE_NAME", "unknown")


def _parse_since(since_iso: str) -> datetime | None:
    text = since_iso.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def format_since(since_iso: str) -> str:
    parsed = _parse_since(since_iso)
    if parsed is None:
        return since_iso
    return parsed.astimezone(UTC).strftime("%H:%M:%S UTC")


def format_outage_length(since_iso: str, now: datetime) -> str:
    parsed = _parse_since(since_iso)
    if parsed is None:
        return "an unknown time"
    seconds = max(0, int((now - parsed).total_seconds()))
    if seconds < 60:
        return f"{seconds} s"
    minutes = round(seconds / 60)
    return f"{minutes} min"


def format_skipped(counts: dict[str, int]) -> str:
    if not counts:
        return "skipped none"
    parts = [f"{name}={count}" for name, count in sorted(counts.items())]
    return "skipped " + ", ".join(parts)


def down_message(server: str, since_iso: str, failed: list[str]) -> str:
    hosts = ",".join(failed)
    return f"Outbound internet down on {server} (since {format_since(since_iso)}) failed={hosts}"


def restored_message(server: str, since_iso: str, now: datetime, counts: dict[str, int]) -> str:
    length = format_outage_length(since_iso, now)
    return f"Outbound internet restored on {server} after {length}. {format_skipped(counts)}"


def _take_skipped(client) -> dict[str, int]:
    counts: dict[str, int] = {}
    try:
        if not hasattr(client, "hgetall"):
            return counts
        raw = client.hgetall(SKIPPED_KEY) or {}
        for key, value in raw.items():
            try:
                counts[_text(key)] = int(_text(value))
            except (TypeError, ValueError):
                continue
        if hasattr(client, "delete"):
            client.delete(SKIPPED_KEY)
    except Exception:
        return counts
    return counts


def _alert(message: str) -> None:
    logger.warning(message, extra={"notification": True, "egress": "transition"})


def publish_probe(
    report: ProbeReport,
    *,
    redis=None,
    server: str | None = None,
    now: datetime | None = None,
    alerter: Callable[[str], None] | None = None,
) -> str | None:
    """Write or clear ``egress:down``. Returns ``down``, ``restored``, or None.

    A second down probe keeps the original ``since`` and does not alert again.
    """
    client = redis if redis is not None else _client()
    if client is None:
        return None
    moment = now or datetime.now(tz=UTC)
    name = server or server_name()
    send = alerter or _alert
    try:
        current = _decode_json(client.get(DOWN_KEY))
    except Exception:
        return None
    was_down = isinstance(current, dict) and current.get("down") is True
    if report.down:
        if was_down and current and current.get("since"):
            since = current.get("since")
        else:
            since = moment.isoformat()
        payload = {
            "down": True,
            "since": since,
            "outage_id": current.get("outage_id") if was_down else since,
            "server": name,
            "failed_hosts": report.failed,
            "probed_at": moment.isoformat(),
        }
        try:
            client.set(DOWN_KEY, json.dumps(payload), ex=DOWN_TTL_S)
        except Exception:
            return None
        _note_local_state(True)
        if not was_down:
            send(down_message(name, str(since), report.failed))
            return "down"
        return None
    if not was_down:
        _note_local_state(False)
        return None
    counts = _take_skipped(client)
    try:
        client.delete(DOWN_KEY)
    except Exception:
        return None
    _note_local_state(False)
    since = str(current.get("since") or moment.isoformat())
    send(restored_message(name, since, moment, counts))
    return "restored"


def _hold_probe_lock(client, token: str) -> bool:
    try:
        current = client.get(LOCK_KEY)
    except Exception:
        return False
    if current is not None:
        if _text(current) == token:
            try:
                client.expire(LOCK_KEY, LOCK_TTL_S)
            except Exception:
                return False
            return True
        return False
    try:
        return bool(client.set(LOCK_KEY, token, nx=True, ex=LOCK_TTL_S))
    except TypeError:
        # A test double that only stores values. One probe is enough there.
        try:
            client.set(LOCK_KEY, token, ex=LOCK_TTL_S)
        except Exception:
            return False
        return True
    except Exception:
        return False


async def tcp_open(ip: str, port: int, timeout: float) -> bool:
    try:
        _reader, writer = await asyncio.wait_for(asyncio.open_connection(ip, port), timeout)
    except Exception:
        return False
    try:
        writer.close()
        await writer.wait_closed()
    except Exception:
        logger.debug("Closing the egress probe socket failed", exc_info=True)
    return True


async def resolve_host(host: str, port: int) -> dict[str, str | None]:
    def _lookup(family: int) -> str | None:
        try:
            infos = socket.getaddrinfo(host, port, family, socket.SOCK_STREAM)
        except socket.gaierror:
            return None
        if not infos:
            return None
        return infos[0][4][0]

    ipv4, ipv6 = await asyncio.gather(
        asyncio.to_thread(_lookup, socket.AF_INET),
        asyncio.to_thread(_lookup, socket.AF_INET6),
    )
    return {"ipv4": ipv4, "ipv6": ipv6}


async def probe_host(
    target: ProbeTarget,
    *,
    connect: ConnectFn = tcp_open,
    resolve: ResolveFn = resolve_host,
) -> HostProbe:
    if target.ipv4 or target.ipv6:
        addresses = {"ipv4": target.ipv4, "ipv6": target.ipv6}
    else:
        try:
            addresses = await resolve(target.host or target.name, target.port)
        except Exception:
            addresses = {"ipv4": None, "ipv6": None}
    families = [(family, ip) for family, ip in addresses.items() if ip]
    if not families:
        return HostProbe(target.name, ok=False, ipv4="skip", ipv6="skip")
    results = await asyncio.gather(
        *(connect(ip, target.port, CONNECT_TIMEOUT_S) for _family, ip in families),
        return_exceptions=True,
    )
    state = {"ipv4": "skip", "ipv6": "skip"}
    ok = False
    for (family, _ip), result in zip(families, results, strict=True):
        reached = result is True
        state[family] = "ok" if reached else "fail"
        ok = ok or reached
    return HostProbe(target.name, ok=ok, ipv4=state["ipv4"], ipv6=state["ipv6"])


async def probe_once(
    *,
    connect: ConnectFn = tcp_open,
    resolve: ResolveFn = resolve_host,
    targets: tuple[ProbeTarget, ...] = PROBE_TARGETS,
) -> ProbeReport:
    hosts = await asyncio.gather(
        *(probe_host(target, connect=connect, resolve=resolve) for target in targets)
    )
    return ProbeReport(hosts=tuple(hosts))


async def egress_probe_loop(shutdown_event: asyncio.Event | None = None) -> None:
    """Probe on a lock so one process per server writes the shared flag."""
    token = uuid.uuid4().hex
    while True:
        if shutdown_event is not None and shutdown_event.is_set():
            return
        try:
            client = _client()
            if client is not None and _hold_probe_lock(client, token):
                report = await probe_once()
                publish_probe(report, redis=client)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log_degraded("egress_probe", f"Egress probe cycle failed: {exc}")
        try:
            if shutdown_event is None:
                await asyncio.sleep(PROBE_INTERVAL_S)
            else:
                await asyncio.wait_for(shutdown_event.wait(), PROBE_INTERVAL_S)
                return
        except TimeoutError:
            continue
        except asyncio.CancelledError:
            raise


def start_egress_probe(shutdown_event: asyncio.Event | None = None) -> asyncio.Task | None:
    """Start the leader loop once in this process. Other processes only read Redis."""
    global _probe_task
    if not probes_enabled():
        return None
    if _probe_task is not None and not _probe_task.done():
        return _probe_task
    _probe_task = asyncio.create_task(egress_probe_loop(shutdown_event), name="egress_probe")
    return _probe_task
