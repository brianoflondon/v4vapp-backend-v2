"""
Live Hive/Nectar stability exploration for long-running monitors.

Exercises patterns that caused overnight EMFILE / socket.accept failures in
hive_monitor (dual clients, NodePoolMonitor, close-then-use, abandoned stream
next(), rebuild cycles).

Requires hive-nectar 1.0.7+ (fix/long-running-client-lifecycle) for hard-close
and default monitor_interval=0. Still useful against older builds to document
regressions.

Not for default CI: needs network and takes time.

Run (short suite):
  NECTAR_STABILITY=1 pytest tests/hive/test_nectar_long_running_stability.py -v -s

Optional soak:
  NECTAR_STABILITY=1 NECTAR_STABILITY_SOAK_SECONDS=120 \\
    pytest tests/hive/test_nectar_long_running_stability.py -v -s -k soak

Knobs: NECTAR_STABILITY_CYCLES, NECTAR_STABILITY_STREAM_SECONDS,
NECTAR_STABILITY_REBUILD_CYCLES
"""

from __future__ import annotations

import gc
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import pytest
from nectar.blockchain import Blockchain
from nectar.hive import Hive

from v4vapp_backend_v2.hive.hive_extras import (
    close_hive_client,
    default_hive_nodes,
    make_stream_hive,
    stream_hive_kwargs,
)

pytestmark = [
    pytest.mark.skipif(
        os.getenv("NECTAR_STABILITY") != "1",
        reason="Set NECTAR_STABILITY=1 to run live Nectar stability probes",
    ),
    pytest.mark.skipif(
        os.getenv("GITHUB_ACTIONS") == "true",
        reason="Live Nectar stability tests are not for GitHub Actions",
    ),
]

ICON = "🔬"
DEFAULT_CYCLES = int(os.getenv("NECTAR_STABILITY_CYCLES", "8"))
STREAM_SECONDS = int(os.getenv("NECTAR_STABILITY_STREAM_SECONDS", "25"))
SOAK_SECONDS = int(os.getenv("NECTAR_STABILITY_SOAK_SECONDS", "0"))
REBUILD_CYCLES = int(os.getenv("NECTAR_STABILITY_REBUILD_CYCLES", "12"))

# Opt-in monitor interval for apps that want background probes.
OPT_IN_MONITOR_INTERVAL = 30.0


@dataclass
class ResourceSnapshot:
    label: str
    threads: int
    node_pool_monitors: int
    probe_threads: int
    fd_count: int | None
    tcp_established: int | None
    notes: list[str] = field(default_factory=list)

    def delta(self, other: "ResourceSnapshot") -> dict[str, Any]:
        return {
            "threads": self.threads - other.threads,
            "node_pool_monitors": self.node_pool_monitors - other.node_pool_monitors,
            "probe_threads": self.probe_threads - other.probe_threads,
            "fd_count": (
                None
                if self.fd_count is None or other.fd_count is None
                else self.fd_count - other.fd_count
            ),
            "tcp_established": (
                None
                if self.tcp_established is None or other.tcp_established is None
                else self.tcp_established - other.tcp_established
            ),
        }


@dataclass
class StabilityReport:
    findings: list[str] = field(default_factory=list)
    nectar_suggestions: list[str] = field(default_factory=list)
    snapshots: list[ResourceSnapshot] = field(default_factory=list)

    def add(self, msg: str) -> None:
        self.findings.append(msg)
        print(f"{ICON} {msg}")

    def suggest(self, msg: str) -> None:
        if msg not in self.nectar_suggestions:
            self.nectar_suggestions.append(msg)
            print(f"{ICON} [nectar?] {msg}")

    def dump(self) -> None:
        print("\n" + "=" * 72)
        print("NECTAR LONG-RUNNING STABILITY REPORT")
        print("=" * 72)
        for s in self.snapshots:
            print(
                f"  [{s.label}] threads={s.threads} "
                f"NodePoolMonitor={s.node_pool_monitors} "
                f"probe={s.probe_threads} fd={s.fd_count} "
                f"tcp_est={s.tcp_established}"
            )
            for n in s.notes:
                print(f"      note: {n}")
        print("\nFindings:")
        for f in self.findings:
            print(f"  - {f}")
        print("\nSuggested hive-nectar improvements:")
        for s in self.nectar_suggestions:
            print(f"  - {s}")
        if not self.nectar_suggestions:
            print("  (none from this run — lifecycle looks good)")
        print("=" * 72 + "\n")


def _count_named_threads(*substrings: str) -> int:
    n = 0
    for t in threading.enumerate():
        name = t.name or ""
        if any(s in name for s in substrings):
            n += 1
    return n


def _fd_count() -> int | None:
    pid = os.getpid()
    fd_dir = f"/proc/{pid}/fd"
    if os.path.isdir(fd_dir):
        try:
            return len(os.listdir(fd_dir))
        except OSError:
            pass
    try:
        out = subprocess.check_output(
            ["lsof", "-nP", "-p", str(pid)],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return max(0, len(out.strip().splitlines()) - 1)
    except (OSError, subprocess.CalledProcessError):
        return None


def _tcp_established() -> int | None:
    pid = os.getpid()
    try:
        out = subprocess.check_output(
            ["lsof", "-nP", "-a", "-p", str(pid), "-iTCP", "-sTCP:ESTABLISHED"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        lines = [ln for ln in out.strip().splitlines() if ln and not ln.startswith("COMMAND")]
        return len(lines)
    except (OSError, subprocess.CalledProcessError):
        return None


def snapshot(label: str) -> ResourceSnapshot:
    return ResourceSnapshot(
        label=label,
        threads=threading.active_count(),
        node_pool_monitors=_count_named_threads("NodePoolMonitor"),
        probe_threads=_count_named_threads("probe_node_health"),
        fd_count=_fd_count(),
        tcp_established=_tcp_established(),
    )


def _nectar_version() -> str:
    try:
        import importlib.metadata as md

        return md.version("hive-nectar")
    except Exception:
        return "?"


def _safe_rpc_call(hive: Hive) -> dict[str, Any]:
    return hive.rpc.get_dynamic_global_properties()


def _bare_hive(*, monitor_interval: float | None = None, **extra: Any) -> Hive:
    kwargs: dict[str, Any] = {
        "node": default_hive_nodes(),
        "timeout": 12,
        "num_retries": 3,
        "num_retries_call": 2,
    }
    if monitor_interval is not None:
        kwargs["monitor_interval"] = monitor_interval
    kwargs.update(extra)
    return Hive(**kwargs)


def _stream_ops_count(hive: Hive, seconds: float, *, only_virtual: bool = False) -> int:
    chain = Blockchain(blockchain_instance=hive)
    head = chain.get_current_block_num()
    count = 0
    deadline = time.monotonic() + seconds
    kwargs: dict[str, Any] = {
        "start": max(1, head - 30),
        "stop": head + 500,
        "threading": False,
        "max_batch_size": None,
    }
    if only_virtual:
        kwargs["only_virtual_ops"] = True
        kwargs["start"] = head - 5
        kwargs["stop"] = head - 5
    for _event in chain.stream(**kwargs):
        count += 1
        if time.monotonic() >= deadline:
            break
        if count >= 200:
            break
    return count


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_default_monitor_interval_is_off(report: StabilityReport):
    """hive-nectar 1.0.7+: default multi-node Hive must not start NodePoolMonitor."""
    base = snapshot("default_mon_before")
    report.snapshots.append(base)

    h = _bare_hive()
    try:
        _safe_rpc_call(h)
        mid = snapshot("default_mon_after_create")
        report.snapshots.append(mid)
        pm = getattr(getattr(h.rpc, "nodes", None), "pool_manager", None)
        interval = getattr(pm, "monitor_interval", None) if pm else None
        alive = bool(
            pm
            and getattr(pm, "_monitor_thread", None)
            and pm._monitor_thread.is_alive()  # type: ignore[union-attr]
        )
        d = mid.delta(base)
        report.add(
            f"Default Hive: monitor_interval={interval} thread_alive={alive} "
            f"ΔNodePoolMonitor={d['node_pool_monitors']}"
        )
        assert d["node_pool_monitors"] == 0, "default Hive must not spawn NodePoolMonitor"
        if interval not in (0, 0.0, None) or alive:
            report.suggest(
                "Default monitor_interval should be 0 with no NodePoolMonitor thread; "
                f"got interval={interval} alive={alive}"
            )
    finally:
        close_hive_client(h)


def test_opt_in_monitor_starts_and_rpc_close_stops_it(report: StabilityReport):
    """Opt-in monitor_interval>0 starts monitors; GrapheneRPC.close must stop them."""
    base = snapshot("optin_before")
    report.snapshots.append(base)

    clients: list[Hive] = []
    for i in range(DEFAULT_CYCLES):
        h = _bare_hive(monitor_interval=OPT_IN_MONITOR_INTERVAL)
        try:
            _safe_rpc_call(h)
        except Exception as e:
            report.add(f"opt-in Hive RPC {i} failed: {type(e).__name__}: {e}")
        clients.append(h)

    mid = snapshot("optin_after_create")
    report.snapshots.append(mid)
    d = mid.delta(base)
    report.add(
        f"After {DEFAULT_CYCLES} Hive(monitor_interval={OPT_IN_MONITOR_INTERVAL}): "
        f"Δthreads={d['threads']} ΔNodePoolMonitor={d['node_pool_monitors']} "
        f"Δfd={d['fd_count']} Δtcp={d['tcp_established']}"
    )
    if d["node_pool_monitors"] < DEFAULT_CYCLES:
        report.add(
            f"Expected ~{DEFAULT_CYCLES} monitors with opt-in; got {d['node_pool_monitors']}"
        )

    # Official GrapheneRPC.close only (not full Hive.close) — 1.0.7 must stop pools.
    for h in clients:
        try:
            if h.rpc is not None and hasattr(h.rpc, "close"):
                h.rpc.close()
        except Exception as e:
            report.add(f"rpc.close failed: {type(e).__name__}: {e}")

    after_rpc = snapshot("optin_after_rpc_close")
    report.snapshots.append(after_rpc)
    d_rpc = after_rpc.delta(base)
    report.add(
        f"After GrapheneRPC.close() on all {len(clients)}: "
        f"ΔNodePoolMonitor={d_rpc['node_pool_monitors']} Δthreads={d_rpc['threads']}"
    )
    if (d_rpc["node_pool_monitors"] or 0) > 0:
        report.suggest(
            "GrapheneRPC.close() left NodePoolMonitor threads running — "
            "close() must stop NodePoolManager (expected fixed in 1.0.7)."
        )
    else:
        report.add("PASS: GrapheneRPC.close() stopped all NodePoolMonitor threads")

    # Full app close path for residual cleanup of instance-level clients
    for h in clients:
        close_hive_client(h)
    clients.clear()
    gc.collect()
    time.sleep(0.5)

    after = snapshot("optin_after_full_close")
    report.snapshots.append(after)
    d_close = after.delta(base)
    report.add(
        f"After close_hive_client + gc: Δthreads={d_close['threads']} "
        f"ΔNodePoolMonitor={d_close['node_pool_monitors']} Δfd={d_close['fd_count']}"
    )


def test_make_stream_hive_disables_monitor(report: StabilityReport):
    """make_stream_hive must not leave NodePoolMonitor running."""
    base = snapshot("stream_hive_before")
    report.snapshots.append(base)

    clients = []
    for _ in range(DEFAULT_CYCLES):
        h = make_stream_hive()
        try:
            _safe_rpc_call(h)
        except Exception as e:
            report.add(f"make_stream_hive RPC failed: {type(e).__name__}: {e}")
        clients.append(h)

    mid = snapshot("stream_hive_after_create")
    report.snapshots.append(mid)
    d = mid.delta(base)
    report.add(
        f"After {DEFAULT_CYCLES} make_stream_hive: "
        f"ΔNodePoolMonitor={d['node_pool_monitors']} Δthreads={d['threads']} Δfd={d['fd_count']}"
    )
    assert d["node_pool_monitors"] == 0, (
        f"make_stream_hive left NodePoolMonitor growth={d['node_pool_monitors']}"
    )

    for h in clients:
        close_hive_client(h)
    gc.collect()
    time.sleep(0.3)
    after = snapshot("stream_hive_after_close")
    report.snapshots.append(after)


def test_close_is_hard_no_reconnect(report: StabilityReport):
    """After close, RPC must not soft-reconnect (nectar 1.0.7 RPCClosed)."""
    h = make_stream_hive()
    _safe_rpc_call(h)
    close_hive_client(h)

    rpc = getattr(h, "rpc", None)
    session_after = getattr(rpc, "session", "missing") if rpc is not None else None
    closed_flag = getattr(rpc, "closed", None) if rpc is not None else None
    report.add(
        f"After close_hive_client: rpc={rpc!r} session={session_after!r} "
        f"rpc.closed={closed_flag} _v4v_hive_closed={getattr(h, '_v4v_hive_closed', None)}"
    )

    # Hive.close() nulls rpc; further use should fail clearly.
    errors: list[str] = []
    try:
        if rpc is not None:
            _safe_rpc_call(h)
            report.add("FAIL: RPC call after close unexpectedly succeeded (soft close)")
            report.suggest(
                "GrapheneRPC.close() is soft — subsequent RPC reconnected. "
                "Hard-close with RPCClosed is required (1.0.7)."
            )
        else:
            report.add("PASS: Hive.close() nulled hive.rpc (cannot soft-reconnect via same handle)")
    except Exception as e:
        errors.append(f"{type(e).__name__}: {e}")
        report.add(f"After close, RPC raised: {type(e).__name__}: {e}")
        if type(e).__name__ not in ("RPCClosed", "RPCConnection", "AttributeError", "TypeError"):
            report.add(f"Unexpected error type after close: {type(e).__name__}")

    if rpc is not None:
        try:
            rpc.next()
            report.add("FAIL: rpc.next() after close unexpectedly succeeded")
            report.suggest("rpc.next() after close should raise RPCClosed")
        except Exception as e:
            errors.append(f"next:{type(e).__name__}: {e}")
            report.add(f"After close, rpc.next() raised: {type(e).__name__}: {e}")
            if type(e).__name__ == "RPCClosed" or "closed" in str(e).lower():
                report.add("PASS: hard-close observed on rpc.next()")


def test_close_rebuild_cycle_fd_growth(report: StabilityReport):
    """create → stream briefly → close → repeat (stream_ops recovery loop)."""
    base = snapshot("rebuild_before")
    report.snapshots.append(base)
    events_total = 0
    failures = 0

    for i in range(REBUILD_CYCLES):
        h = make_stream_hive()
        try:
            n = _stream_ops_count(
                h, max(3.0, STREAM_SECONDS / max(REBUILD_CYCLES, 1)), only_virtual=False
            )
            events_total += n
        except Exception as e:
            failures += 1
            report.add(f"rebuild cycle {i} stream error: {type(e).__name__}: {e}")
        finally:
            close_hive_client(h)
            del h
        if i % 4 == 3:
            gc.collect()
            report.snapshots.append(snapshot(f"rebuild_after_{i + 1}"))

    gc.collect()
    time.sleep(0.5)
    end = snapshot("rebuild_after_all")
    report.snapshots.append(end)
    d = end.delta(base)
    report.add(
        f"Rebuild x{REBUILD_CYCLES}: events={events_total} failures={failures} "
        f"Δthreads={d['threads']} Δfd={d['fd_count']} Δtcp={d['tcp_established']} "
        f"Δmonitors={d['node_pool_monitors']}"
    )
    if (d.get("fd_count") or 0) > 15 or (d.get("tcp_established") or 0) > 10:
        report.suggest(
            "Close+rebuild cycles grew FDs/TCP — check httpx keep-alive limits and "
            "that Hive.close() fully drains sessions."
        )
    if (d.get("node_pool_monitors") or 0) > 0:
        report.suggest("Rebuild cycles left NodePoolMonitor threads.")


def test_single_client_stream_brief(report: StabilityReport):
    """Single-client real-op stream for a short window (production-like)."""
    base = snapshot("single_stream_before")
    report.snapshots.append(base)

    h = make_stream_hive()
    try:
        n = _stream_ops_count(h, STREAM_SECONDS, only_virtual=False)
        report.add(f"Single-client stream ~{STREAM_SECONDS}s yielded {n} events")
    except Exception as e:
        report.add(f"Single-client stream failed: {type(e).__name__}: {e}")
        raise
    finally:
        close_hive_client(h)
        gc.collect()
        time.sleep(0.3)

    after = snapshot("single_stream_after")
    report.snapshots.append(after)
    d = after.delta(base)
    report.add(
        f"Single-client residual: Δthreads={d['threads']} Δfd={d['fd_count']} "
        f"Δtcp={d['tcp_established']} Δmonitors={d['node_pool_monitors']}"
    )


def test_dual_client_vs_single_resource_delta(report: StabilityReport):
    """Dual Hive (old buggy pattern) vs single client sequential virtual ops."""
    base = snapshot("dual_vs_single_base")
    report.snapshots.append(base)

    main_h = make_stream_hive()
    virt_h = make_stream_hive()
    dual_mid = snapshot("dual_two_clients_open")
    report.snapshots.append(dual_mid)
    dual_open = dual_mid.delta(base)

    n_main = 0
    n_virt = 0
    try:
        n_main = _stream_ops_count(main_h, max(10, STREAM_SECONDS // 2), only_virtual=False)
        n_virt = _stream_ops_count(virt_h, max(8, STREAM_SECONDS // 3), only_virtual=True)
    except Exception as e:
        report.add(f"Dual-client stream error: {type(e).__name__}: {e}")
    report.snapshots.append(snapshot("dual_after_stream"))

    close_hive_client(main_h)
    close_hive_client(virt_h)
    gc.collect()
    time.sleep(0.4)
    dual_closed = snapshot("dual_after_close")
    report.snapshots.append(dual_closed)

    base2 = snapshot("single_pattern_base")
    report.snapshots.append(base2)
    single_h = make_stream_hive()
    n_s = 0
    try:
        n_s = _stream_ops_count(single_h, max(10, STREAM_SECONDS // 2), only_virtual=False)
        n_s += _stream_ops_count(single_h, max(8, STREAM_SECONDS // 3), only_virtual=True)
    except Exception as e:
        report.add(f"Single-client sequential error: {type(e).__name__}: {e}")
    close_hive_client(single_h)
    gc.collect()
    time.sleep(0.4)
    single_closed = snapshot("single_after_close")
    report.snapshots.append(single_closed)

    report.add(
        f"Dual open cost: Δfd={dual_open['fd_count']} Δtcp={dual_open['tcp_established']} "
        f"Δthreads={dual_open['threads']} (events main={n_main} virt={n_virt})"
    )
    dual_residual = dual_closed.delta(base)
    single_residual = single_closed.delta(base2)
    report.add(
        f"After close residual dual Δfd={dual_residual['fd_count']} "
        f"vs single Δfd={single_residual['fd_count']}; "
        f"dual Δtcp={dual_residual['tcp_established']} "
        f"vs single Δtcp={single_residual['tcp_established']}; "
        f"single events~{n_s}"
    )


def test_abandoned_stream_timeout_zombie(report: StabilityReport):
    """Drop stream generator mid-consume (wait_for abandon simulation)."""
    base = snapshot("zombie_before")
    report.snapshots.append(base)

    h = make_stream_hive()
    chain = Blockchain(blockchain_instance=h)
    head = chain.get_current_block_num()
    gen = chain.stream(
        start=head - 5,
        stop=head + 10_000,
        threading=False,
        max_batch_size=None,
    )
    for i, _ev in enumerate(gen):
        if i >= 3:
            break

    del gen
    gc.collect()
    time.sleep(1.0)
    mid = snapshot("zombie_after_abandon_gen")
    report.snapshots.append(mid)

    close_hive_client(h)
    gc.collect()
    time.sleep(1.0)
    after = snapshot("zombie_after_close")
    report.snapshots.append(after)

    d_mid = mid.delta(base)
    d_after = after.delta(base)
    report.add(
        f"Abandoned stream generator: Δfd mid={d_mid['fd_count']} "
        f"after close={d_after['fd_count']}; Δthreads after={d_after['threads']}"
    )


def test_in_flight_next_after_close(report: StabilityReport):
    """
    Background thread stuck in next(stream) while main closes the client.

    Known remaining nectar gap: close may not unblock in-flight HTTP next().
    """
    base = snapshot("inflight_before")
    report.snapshots.append(base)

    h = make_stream_hive()
    chain = Blockchain(blockchain_instance=h)
    head = chain.get_current_block_num()
    gen = chain.stream(
        start=head - 2,
        stop=head + 50_000,
        threading=False,
        max_batch_size=None,
    )
    next(gen)

    result: dict[str, Any] = {"err": None, "ok": False}

    def _consume_forever() -> None:
        try:
            for _ in gen:
                pass
            result["ok"] = True
        except Exception as e:
            result["err"] = f"{type(e).__name__}: {e}"

    t = threading.Thread(target=_consume_forever, name="nectar-zombie-next", daemon=True)
    t.start()
    time.sleep(0.5)
    close_hive_client(h)
    t.join(timeout=15.0)
    alive = t.is_alive()
    report.add(
        f"In-flight next() after close: thread_alive={alive} "
        f"err={result['err']!r} exhausted_ok={result['ok']}"
    )
    if alive:
        report.suggest(
            "After close(), in-flight Blockchain.stream next() can block past "
            "timeouts. close should abort in-flight HTTP (close transport sockets) "
            "so abandoned asyncio.wait_for workers exit promptly."
        )
    elif result["err"]:
        report.add(f"PASS: in-flight worker exited with {result['err']}")
    else:
        report.add("In-flight worker exited cleanly after close")

    gc.collect()
    time.sleep(0.3)
    after = snapshot("inflight_after")
    report.snapshots.append(after)
    report.add(f"In-flight residual Δ={after.delta(base)}")


@pytest.mark.skipif(SOAK_SECONDS <= 0, reason="Set NECTAR_STABILITY_SOAK_SECONDS>0 for soak")
def test_soak_single_client_stream(report: StabilityReport):
    """Longer single-client soak; watch FD/thread drift."""
    base = snapshot("soak_start")
    report.snapshots.append(base)
    h = make_stream_hive()
    samples: list[ResourceSnapshot] = []
    try:
        chain = Blockchain(blockchain_instance=h)
        head = chain.get_current_block_num()
        start = head - 20
        deadline = time.monotonic() + SOAK_SECONDS
        count = 0
        last_sample = time.monotonic()
        for _ev in chain.stream(
            start=start, stop=head + 100_000, threading=False, max_batch_size=None
        ):
            count += 1
            now = time.monotonic()
            if now - last_sample >= 15:
                samples.append(snapshot(f"soak_t+{int(now - (deadline - SOAK_SECONDS))}s"))
                last_sample = now
            if now >= deadline:
                break
        report.add(f"Soak {SOAK_SECONDS}s single-client events={count}")
    finally:
        close_hive_client(h)
        gc.collect()
        time.sleep(0.5)
    end = snapshot("soak_end")
    report.snapshots.extend(samples)
    report.snapshots.append(end)
    d = end.delta(base)
    report.add(
        f"Soak residual Δthreads={d['threads']} Δfd={d['fd_count']} "
        f"Δtcp={d['tcp_established']} Δmonitors={d['node_pool_monitors']}"
    )
    if (d.get("fd_count") or 0) > 30 or (d.get("tcp_established") or 0) > 20:
        report.suggest(
            "Single-client soak still grew FDs/TCP — investigate httpx keep-alive limits."
        )


@pytest.fixture(scope="module")
def report():
    r = StabilityReport()
    r.add(f"hive-nectar={_nectar_version()} python={sys.version.split()[0]}")
    r.add(f"nodes={default_hive_nodes()[:4]}… kwargs={stream_hive_kwargs()}")
    yield r
    r.dump()


def test_zz_print_report(report: StabilityReport):
    """Runs last alphabetically; ensures dump if only partial selection."""
    assert report is not None
