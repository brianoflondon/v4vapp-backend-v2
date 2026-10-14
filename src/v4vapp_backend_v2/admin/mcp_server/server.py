"""MCP server for the V4VApp admin interface.

Local stdio:

    uv run v4vapp-admin-mcp --config devdocker.config.yaml

HTTP on a TCP port, for a container reached over Tailscale:

    V4VAPP_ADMIN_MCP_TOKEN="$(openssl rand -hex 32)" \\
      uv run v4vapp-admin-mcp --transport http --host 0.0.0.0 --port 8090

Clients send the token as ``Authorization: Bearer <token>`` to ``http://host:8090/mcp``.
"""

from __future__ import annotations

import argparse
import sys
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from v4vapp_backend_v2.admin.mcp_server.auth import BearerTokenMiddleware, require_access_token
from v4vapp_backend_v2.admin.mcp_server.runtime import (
    bind_stdio_to_stderr,
    ensure_ready,
    init_config,
    restore_stdout,
    shutdown,
)
from v4vapp_backend_v2.admin.mcp_server.tools import (
    READ_TOOLS,
    flush_ledger_cache,
    set_gateway,
    set_quote_sources,
)

INSTRUCTIONS = """
You are connected to the V4VApp admin interface.

Use the read tools for the dashboard, ledger accounts, users, ledger search,
financial reports, V4V configuration, quote sources, gateway state, and Hive
deposit inspection. Account labels come from list_ledger_accounts. Pass one
of those labels to account_balance.

flush_ledger_cache, set_quote_sources, and set_gateway change live state.
They do nothing unless confirm is true. set_gateway opens or closes
Hive → Lightning and Lightning → Hive. True opens a direction. False closes it.

This server does not create, edit, or reverse ledger entries, does not replay
deposits or retry Lightning payments, and does not change fees or limits.
Use the admin web UI for those.
""".strip()

READ_ONLY = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=True,
)
CACHE_WRITE = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)
CONFIG_WRITE = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=True,
    idempotentHint=True,
    openWorldHint=True,
)


def make_lifespan(*, close_on_session_end: bool):
    """Build the FastMCP session lifespan.

    FastMCP enters this once per Streamable HTTP session, and again if that
    session is replaced. Closing Mongo there leaves later tool calls with
    ``Cannot use AsyncMongoClient after close``. HTTP keeps the client until
    ``CloseDatabaseOnShutdown`` sees the process stop. Stdio has one session,
    so it closes when that session ends.
    """

    @asynccontextmanager
    async def lifespan(_server: FastMCP):
        await ensure_ready()
        try:
            yield {"ready": True}
        finally:
            if close_on_session_end:
                await shutdown()

    return lifespan


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the config file, transport, and HTTP bind address."""
    parser = argparse.ArgumentParser(description="V4VApp Admin MCP server")
    parser.add_argument(
        "--config",
        default="devdocker.config.yaml",
        help="Config file in config/ (default: devdocker.config.yaml)",
    )
    parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default="stdio",
        help="stdio for a local client, http for a TCP port (default: stdio)",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="HTTP bind address (default: 127.0.0.1). Use 0.0.0.0 inside Docker.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8090,
        help="HTTP port (default: 8090)",
    )
    return parser.parse_args(argv)


def build_server(
    host: str = "127.0.0.1",
    port: int = 8090,
    *,
    close_database_on_session_end: bool = False,
) -> FastMCP:
    """Register the admin tools on a new FastMCP server."""
    server: FastMCP[dict[str, Any]] = FastMCP(
        "v4vapp-admin",
        instructions=INSTRUCTIONS,
        lifespan=make_lifespan(close_on_session_end=close_database_on_session_end),
        host=host,
        port=port,
        log_level="WARNING",
    )
    for fn in READ_TOOLS:
        server.tool(annotations=READ_ONLY, structured_output=False)(fn)
    server.tool(annotations=CACHE_WRITE, structured_output=False)(flush_ledger_cache)
    server.tool(annotations=CONFIG_WRITE, structured_output=False)(set_quote_sources)
    server.tool(annotations=CONFIG_WRITE, structured_output=False)(set_gateway)
    return server


class CloseDatabaseOnShutdown:
    """Close Mongo when the HTTP process stops.

    Uvicorn sends one ASGI lifespan for the process. FastMCP's own lifespan
    is per MCP session and must not close the shared client.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "lifespan":
            await self.app(scope, receive, send)
            return

        async def send_and_close(message: Message) -> None:
            if message["type"] in {"lifespan.shutdown.complete", "lifespan.shutdown.failed"}:
                await shutdown()
            await send(message)

        await self.app(scope, receive, send_and_close)


def build_http_app(server: FastMCP, token: str) -> ASGIApp:
    """Streamable HTTP MCP app with a public health check and a bearer token."""

    @server.custom_route("/health", methods=["GET"], include_in_schema=False)
    async def health(_request: Request) -> Response:
        return JSONResponse({"status": "ok"})

    app = CloseDatabaseOnShutdown(server.streamable_http_app())
    return BearerTokenMiddleware(app, token=token, public_paths=frozenset({"/health"}))


def run_http(server: FastMCP, token: str, host: str, port: int) -> None:
    """Serve MCP on TCP. Logs go to the process output. The token is not printed."""
    print(
        f"Admin MCP listening on http://{host}:{port}/mcp",
        file=sys.stderr,
    )
    config = uvicorn.Config(
        build_http_app(server, token),
        host=host,
        port=port,
        log_level="warning",
    )
    uvicorn.Server(config).run()


def main(argv: list[str] | None = None) -> None:
    """Load config, then serve MCP over stdio or HTTP."""
    args = parse_args(argv)
    if args.transport == "http":
        try:
            token = require_access_token()
            init_config(args.config)
        except Exception as exc:
            print(f"Admin MCP failed to start: {exc}", file=sys.stderr)
            raise SystemExit(1) from exc
        run_http(build_server(host=args.host, port=args.port), token, args.host, args.port)
        return

    saved_stdout = bind_stdio_to_stderr()
    try:
        init_config(args.config)
    except Exception as exc:
        print(
            f"Admin MCP failed to read config {args.config}: {exc}",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc
    finally:
        restore_stdout(saved_stdout)

    build_server(close_database_on_session_end=True).run(transport="stdio")


if __name__ == "__main__":
    main()
