from datetime import UTC, datetime, timedelta
from decimal import Decimal

from v4vapp_backend_v2.config.setup import DashConnectionConfig
from v4vapp_backend_v2.dash.runtime import assemble_dash_health, watcher_info_from_doc
from v4vapp_backend_v2.dash.watcher.loop import WatcherState


def _conn() -> DashConnectionConfig:
    return DashConnectionConfig(
        rpc_url="http://127.0.0.1:19998",
        rpc_user="u",
        rpc_password="p",
        network="testnet",
        xpub="x",
        master_fingerprint="73c5da0a",
        settle_policy="instantsend_or_chainlock",
        routing_fee_sats=Decimal(300),
        poll_interval_s=10,
    )


def test_assemble_disabled_without_conn() -> None:
    body = assemble_dash_health(version="1", conn=None, mongo_ok=None)
    assert body["status"] == "disabled"


def test_assemble_ok_with_fresh_heartbeat() -> None:
    now = datetime.now(UTC)
    body = assemble_dash_health(
        version="1",
        conn=_conn(),
        mongo_ok=True,
        dashd_info={"synced": True, "initialblockdownload": False},
        watcher_info={
            "last_tick_age_s": 2.0,
            "open_invoices": 0,
            "last_error": None,
        },
    )
    assert body["status"] == "ok"
    info = watcher_info_from_doc({"last_tick_at": now, "open_invoices": 3, "last_error": None})
    assert info is not None
    assert info["open_invoices"] == 3
    assert info["last_tick_age_s"] is not None
    assert info["last_tick_age_s"] < 5


def test_assemble_degraded_when_watcher_stale() -> None:
    body = assemble_dash_health(
        version="1",
        conn=_conn(),
        mongo_ok=True,
        watcher_info={"last_tick_age_s": 120.0, "open_invoices": 1, "last_error": None},
    )
    assert body["status"] == "degraded"


def test_assemble_degraded_before_first_tick() -> None:
    body = assemble_dash_health(version="1", conn=_conn(), mongo_ok=True)
    assert body["status"] == "degraded"
    assert WatcherState().last_tick_at is None


def test_assemble_error_on_dashd_snapshot() -> None:
    body = assemble_dash_health(
        version="1",
        conn=_conn(),
        mongo_ok=True,
        dashd_info={"error": True},
        watcher_info={"last_tick_age_s": 1.0, "open_invoices": 0, "last_error": None},
    )
    assert body["status"] == "error"


def test_watcher_info_from_naive_datetime() -> None:
    last = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=1)
    info = watcher_info_from_doc({"last_tick_at": last, "open_invoices": 0})
    assert info is not None
    assert info["last_tick_age_s"] is not None
