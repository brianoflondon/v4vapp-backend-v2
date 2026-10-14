"""Unit tests for the admin MCP server.

These tests do not open Mongo, Redis, Hive, or Lightning. They check the tool
catalog, the confirm gate, and argument handling.
"""

import json
import logging
import sys
from datetime import timedelta
from types import SimpleNamespace

import pytest
from fastapi.responses import JSONResponse
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from v4vapp_backend_v2.admin.mcp_server import runtime, tools
from v4vapp_backend_v2.admin.mcp_server.auth import (
    TOKEN_ENV,
    BearerTokenMiddleware,
    authorization_matches,
    require_access_token,
)
from v4vapp_backend_v2.admin.mcp_server.server import build_http_app, build_server, parse_args
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.hive.v4v_config import V4VConfigData


@pytest.fixture
def ready(monkeypatch: pytest.MonkeyPatch):
    """Skip database startup."""

    async def _ready() -> None:
        return None

    monkeypatch.setattr(tools, "ensure_ready", _ready)


def test_stdio_bind_keeps_protocol_stdout_separate():
    saved = runtime.bind_stdio_to_stderr()
    try:
        handler = logging.StreamHandler(stream=sys.stdout)
        assert handler.stream is sys.stderr
    finally:
        runtime.restore_stdout(saved)
    assert sys.stdout is saved
    assert handler.stream is sys.stderr


def test_cap_text_marks_the_omitted_tail():
    text = "abcde"
    capped = runtime.cap_text(text, limit=3)
    assert capped.startswith("abc")
    assert "truncated 2 characters" in capped
    assert runtime.cap_text("ok", limit=10) == "ok"


def test_unwrap_json_response_keeps_errors():
    assert runtime.unwrap(JSONResponse({"ok": True})) == {"ok": True}
    missing = runtime.unwrap(JSONResponse({"error": "missing"}, status_code=404))
    assert missing["success"] is False
    assert missing["status_code"] == 404
    assert runtime.unwrap({"plain": 1}) == {"plain": 1}


def test_confirmation_required_names_the_action():
    payload = runtime.confirmation_required("flush_ledger_cache", {"generation": "next"})
    assert payload["success"] is False
    assert payload["confirm_required"] is True
    assert "confirm=true" in payload["error"]
    assert payload["requested"] == {"generation": "next"}


def test_summarize_account_skips_balance_lines():
    summary = tools.summarize_account(
        SimpleNamespace(
            hive=1,
            hbd=2,
            usd=3,
            sats=4,
            msats=5,
            dash=6,
            duffs=7,
            magi_btc_sats=8,
            in_progress_msats=9,
            balances_net={"HIVE": 1},
            last_transaction_date=None,
            has_transactions=True,
        )
    )
    assert summary["sats"] == 4
    assert summary["net"] == {"HIVE": 1}
    assert summary["has_transactions"] is True
    assert "balances" not in summary


def test_parse_args_defaults_to_the_admin_dev_config():
    args = parse_args([])
    assert args.config == "devdocker.config.yaml"
    assert args.transport == "stdio"
    assert args.host == "127.0.0.1"
    assert args.port == 8090
    http_args = parse_args(
        [
            "--config",
            "hive.config.yaml",
            "--transport",
            "http",
            "--host",
            "0.0.0.0",
            "--port",
            "9000",
        ]
    )
    assert http_args.config == "hive.config.yaml"
    assert http_args.transport == "http"
    assert http_args.host == "0.0.0.0"
    assert http_args.port == 9000


@pytest.mark.asyncio
async def test_server_registers_admin_sections():
    server = build_server()
    listed = await server.list_tools()
    names = {tool.name for tool in listed}
    assert {
        "admin_overview",
        "admin_sanity",
        "hive_balances",
        "list_ledger_accounts",
        "account_balance",
        "list_users",
        "search_ledger_entries",
        "balance_sheet",
        "profit_and_loss",
        "get_v4v_config",
        "get_gateway",
        "get_quote_sources",
        "search_hive_deposits",
        "inspect_hive_deposit",
        "flush_ledger_cache",
        "set_quote_sources",
        "set_gateway",
    } <= names

    overview = next(tool for tool in listed if tool.name == "admin_overview")
    flush = next(tool for tool in listed if tool.name == "flush_ledger_cache")
    quotes = next(tool for tool in listed if tool.name == "set_quote_sources")
    gateway = next(tool for tool in listed if tool.name == "set_gateway")
    assert overview.annotations is not None
    assert overview.annotations.readOnlyHint is True
    assert flush.annotations is not None
    assert flush.annotations.readOnlyHint is False
    assert quotes.annotations is not None
    assert quotes.annotations.destructiveHint is True
    assert gateway.annotations is not None
    assert gateway.annotations.destructiveHint is True

    account = next(tool for tool in listed if tool.name == "account_balance")
    assert "account" in account.inputSchema["properties"]
    assert "Account label" in account.inputSchema["properties"]["account"]["description"]


@pytest.mark.asyncio
async def test_writes_wait_for_confirm(ready, monkeypatch: pytest.MonkeyPatch):
    called = {"cache": False, "quotes": False}

    async def invalidate():
        called["cache"] = True
        return 7

    async def update(payload):
        called["quotes"] = True
        return {"success": True, "quote_services": payload}

    monkeypatch.setattr(tools, "invalidate_all_ledger_cache", invalidate)
    monkeypatch.setattr(tools, "update_quote_sources_api", update)
    server = build_server()

    blocked = _payload(await server.call_tool("flush_ledger_cache", {}))
    assert blocked["confirm_required"] is True
    assert called["cache"] is False

    flushed = _payload(await server.call_tool("flush_ledger_cache", {"confirm": True}))
    assert flushed == {"success": True, "generation": 7}

    no_flags = _payload(await server.call_tool("set_quote_sources", {"confirm": True}))
    assert no_flags["success"] is False
    assert called["quotes"] is False

    held = _payload(await server.call_tool("set_quote_sources", {"binance": False}))
    assert held["confirm_required"] is True
    assert held["requested"] == {"Binance": False}
    assert called["quotes"] is False

    saved = _payload(
        await server.call_tool("set_quote_sources", {"confirm": True, "binance": False})
    )
    assert saved["success"] is True
    assert saved["quote_services"] == {"Binance": False}


@pytest.mark.asyncio
async def test_account_period_does_not_create_a_checkpoint(ready, monkeypatch: pytest.MonkeyPatch):
    seen = {}

    def last_end(period_type, now):
        assert period_type.value == "monthly"
        return now - timedelta(days=30)

    async def printout(**kwargs):
        seen.update(kwargs)
        return "", SimpleNamespace(
            hive=0,
            hbd=0,
            usd=0,
            sats=0,
            msats=0,
            dash=0,
            duffs=0,
            magi_btc_sats=0,
            in_progress_msats=0,
            balances_net={},
            last_transaction_date=None,
            has_transactions=False,
        )

    monkeypatch.setattr(tools, "last_completed_period_end", last_end)
    monkeypatch.setattr(tools, "account_balance_printout", printout)
    payload = await tools.account_balance(
        "Customer Deposits Hive (Asset) - Sub: v4vapp",
        period="monthly",
    )
    assert payload["success"] is True
    assert seen["period_start"] is not None
    assert seen["age"] > timedelta(0)
    assert "printout" not in payload


@pytest.mark.asyncio
async def test_account_balance_rejects_a_bad_label(ready):
    server = build_server()
    payload = _payload(await server.call_tool("account_balance", {"account": "not-an-account"}))
    assert payload["success"] is False
    assert "not-an-account" in payload["error"]


@pytest.mark.asyncio
async def test_list_users_filters_and_limits(ready, monkeypatch: pytest.MonkeyPatch):
    async def users_data_api(active_only: bool = True):
        assert active_only is True
        return {
            "users_data": [
                {"sub": "alice", "balance_sats": 10, "has_transactions": True},
                {"sub": "bob", "balance_sats": -2, "has_transactions": True},
                {"sub": "carol", "balance_sats": 3, "has_transactions": False},
            ],
            "summary": {"total_users": 3},
            "now": "2026-10-13T00:00:00+00:00",
        }

    monkeypatch.setattr(tools, "users_data_api", users_data_api)
    payload = await tools.list_users(name_contains="a", limit=1)
    assert payload["matched"] == 2
    assert payload["returned"] == 1
    assert payload["truncated"] is True
    assert payload["users"] == [{"sub": "alice", "balance_sats": 10, "has_transactions": True}]
    assert payload["summary"]["total_positive_sats"] == 13


@pytest.mark.asyncio
async def test_search_ledger_entries_caps_the_page(ready, monkeypatch: pytest.MonkeyPatch):
    seen = {}

    async def ledger_entries_data(**kwargs):
        seen.update(kwargs)
        return {"count": 0, "entries": [], "totals": {}}

    monkeypatch.setattr(tools, "ledger_entries_data", ledger_entries_data)
    payload = await tools.search_ledger_entries(limit=500, offset=-4, search="memo")
    assert seen["limit"] == 50
    assert seen["offset"] == 0
    assert seen["general_search"] == "memo"
    assert payload["limit"] == 50
    assert payload["offset"] == 0


@pytest.mark.asyncio
async def test_tool_errors_stay_in_the_result(ready, monkeypatch: pytest.MonkeyPatch):
    async def broken():
        raise RuntimeError("node down")

    monkeypatch.setattr(tools, "dashboard_lnd_info", broken)
    payload = await tools.lightning_node()
    assert payload["success"] is False
    assert "node down" in payload["error"]


def _payload(result) -> dict:
    text = result[0].text
    return json.loads(text)


def test_access_token_must_be_long_enough():
    with pytest.raises(ValueError, match=TOKEN_ENV):
        require_access_token({})
    with pytest.raises(ValueError):
        require_access_token({TOKEN_ENV: "short"})
    assert require_access_token({TOKEN_ENV: "a" * 16}) == "a" * 16


def test_authorization_header_must_be_the_bearer_token():
    token = "a" * 16
    assert authorization_matches(f"Bearer {token}", token)
    assert authorization_matches(f"bearer {token}", token)
    assert not authorization_matches(f"Bearer {token} extra", token)
    assert not authorization_matches("Bearer wrong-token-value", token)
    assert not authorization_matches(token, token)
    assert not authorization_matches(None, token)


def test_middleware_keeps_health_open_and_locks_other_paths():
    async def ok(_request):
        return PlainTextResponse("ok")

    app = Starlette(
        routes=[
            Route("/health", ok),
            Route("/mcp", ok, methods=["GET", "POST"]),
        ]
    )
    protected = BearerTokenMiddleware(app, token="t" * 24, public_paths=frozenset({"/health"}))
    with TestClient(protected) as client:
        assert client.get("/health").status_code == 200
        assert client.post("/mcp").status_code == 401
        wrong = client.post("/mcp", headers={"Authorization": "Bearer wrong-token"})
        assert wrong.status_code == 401
        leaked = client.post("/mcp", params={"access_token": "t" * 24})
        assert leaked.status_code == 401
        good = client.post("/mcp", headers={"Authorization": "Bearer " + ("t" * 24)})
        assert good.status_code == 200
        assert good.text == "ok"


def test_http_app_opens_health_and_locks_mcp():
    app = build_http_app(build_server(host="0.0.0.0"), token="t" * 24)
    with TestClient(app) as client:
        assert client.get("/health").json() == {"status": "ok"}
        denied = client.post("/mcp")
        assert denied.status_code == 401
        assert denied.headers["www-authenticate"] == "Bearer"


def test_gateway_state_uses_the_admin_closed_flags():
    state = tools.gateway_state(closed_get_lnd=True, closed_get_hive=False)
    assert state["hive_to_lightning"] == "closed"
    assert state["hive_to_lightning_open"] is False
    assert state["lightning_to_hive"] == "open"
    assert state["lightning_to_hive_open"] is True


def test_apply_gateway_keeps_the_rest_of_the_config():
    data = V4VConfigData(
        closed_get_lnd=False,
        closed_get_hive=False,
        quote_services={
            "CoinGecko": False,
            "Binance": True,
            "CoinMarketCap": True,
            "HiveInternalMarket": True,
        },
    )
    updated = tools.apply_gateway(data, hive_to_lightning=False, lightning_to_hive=None)
    assert updated.closed_get_lnd is True
    assert updated.closed_get_hive is False
    assert updated.quote_services["CoinGecko"] is False
    assert data.closed_get_lnd is False


@pytest.mark.asyncio
async def test_set_gateway_waits_for_confirm_then_closes_both(ready, monkeypatch):
    data = V4VConfigData(closed_get_lnd=False, closed_get_hive=False)
    saved = {"called": False}

    class FakeConfig:
        server_accname = "devser.v4vapp"

        def __init__(self):
            self.data = data

        def check(self):
            return None

        async def put(self):
            saved["called"] = True
            return True

    fake = FakeConfig()
    monkeypatch.setattr(tools, "load_v4v_config", lambda: fake)

    missing = await tools.set_gateway(confirm=True)
    assert missing["success"] is False
    assert saved["called"] is False

    blocked = await tools.set_gateway(hive_to_lightning=False, lightning_to_hive=False)
    assert blocked["confirm_required"] is True
    assert blocked["requested"] == {
        "hive_to_lightning": False,
        "lightning_to_hive": False,
    }
    assert saved["called"] is False
    assert fake.data.closed_get_lnd is False

    closed = await tools.set_gateway(
        confirm=True,
        hive_to_lightning=False,
        lightning_to_hive=False,
    )
    assert closed["hive_to_lightning"] == "closed"
    assert closed["lightning_to_hive"] == "closed"
    assert closed["hive_saved"] is True
    assert saved["called"] is True
    assert fake.data.closed_get_lnd is True
    assert fake.data.closed_get_hive is True
    assert data.closed_get_lnd is False


class _Client:
    def __init__(self, closed: bool) -> None:
        self._closed = closed


@pytest.mark.asyncio
async def test_ensure_ready_reopens_a_closed_client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(runtime, "_ready", False)
    monkeypatch.setattr(runtime, "arm_routers", lambda: None)
    current = _Client(closed=True)
    monkeypatch.setattr(InternalConfig, "db_client", current, raising=False)
    calls: list[str] = []

    class FakeConn:
        """A connection whose setup flag would otherwise skip a new client."""

        _setup = True

        async def setup_database(self) -> None:
            assert self._setup is False
            calls.append("setup")
            opened = _Client(closed=False)
            InternalConfig.db_client = opened

    from v4vapp_backend_v2.database import db_pymongo

    monkeypatch.setattr(db_pymongo, "DBConn", FakeConn)

    await runtime.ensure_ready()
    assert calls == ["setup"]
    assert runtime._ready is True

    await runtime.ensure_ready()
    assert calls == ["setup"]

    InternalConfig.db_client = _Client(closed=True)
    await runtime.ensure_ready()
    assert calls == ["setup", "setup"]
    assert runtime._mongo_client_is_open() is True


@pytest.mark.asyncio
async def test_shutdown_closes_the_client_only_after_it_was_opened(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(runtime, "_ready", False)
    calls: list[str] = []

    class Instance:
        async def close_db_clients_async(self) -> None:
            calls.append("close")

    monkeypatch.setattr(InternalConfig, "_instance", Instance(), raising=False)

    await runtime.shutdown()
    assert calls == []

    runtime._ready = True
    await runtime.shutdown()
    assert calls == ["close"]
    assert runtime._ready is False


@pytest.mark.asyncio
async def test_http_session_end_does_not_close_the_database(monkeypatch: pytest.MonkeyPatch):
    events: list[str] = []

    async def ensure() -> None:
        events.append("ready")

    async def stop() -> None:
        events.append("stop")

    monkeypatch.setattr("v4vapp_backend_v2.admin.mcp_server.server.ensure_ready", ensure)
    monkeypatch.setattr("v4vapp_backend_v2.admin.mcp_server.server.shutdown", stop)
    server = build_server()
    assert server.settings.lifespan is not None
    async with server.settings.lifespan(server):
        pass
    async with server.settings.lifespan(server):
        pass
    assert events == ["ready", "ready"]


@pytest.mark.asyncio
async def test_stdio_session_end_closes_the_database(monkeypatch: pytest.MonkeyPatch):
    events: list[str] = []

    async def ensure() -> None:
        events.append("ready")

    async def stop() -> None:
        events.append("stop")

    monkeypatch.setattr("v4vapp_backend_v2.admin.mcp_server.server.ensure_ready", ensure)
    monkeypatch.setattr("v4vapp_backend_v2.admin.mcp_server.server.shutdown", stop)
    server = build_server(close_database_on_session_end=True)
    assert server.settings.lifespan is not None
    async with server.settings.lifespan(server):
        pass
    assert events == ["ready", "stop"]


def test_http_process_shutdown_closes_the_database_once(monkeypatch: pytest.MonkeyPatch):
    calls: list[str] = []

    async def stop() -> None:
        calls.append("stop")

    monkeypatch.setattr("v4vapp_backend_v2.admin.mcp_server.server.shutdown", stop)
    app = build_http_app(build_server(host="0.0.0.0"), token="t" * 24)
    with TestClient(app) as client:
        assert client.get("/health").json() == {"status": "ok"}
        assert calls == []
    assert calls == ["stop"]
