"""Startup and JSON helpers for the admin MCP server.

MCP stdio reserves stdout for protocol bytes. Logging setup in this process
captures ``sys.stdout`` while it points at stderr, then stdout is restored
for the protocol.
"""

from __future__ import annotations

import asyncio
import json
import sys
from typing import Any, TextIO

from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from v4vapp_backend_v2.config.setup import InternalConfig, logger

_ready = False
_lock = asyncio.Lock()


def bind_stdio_to_stderr() -> TextIO:
    """Point ``sys.stdout`` at stderr and return the previous stdout."""
    saved = sys.stdout
    sys.stdout = sys.stderr
    return saved


def restore_stdout(saved: TextIO) -> None:
    """Put the protocol stream back on stdout."""
    sys.stdout = saved


def init_config(config_filename: str) -> InternalConfig:
    """Load the admin config and send the startup print to the current stdout."""
    return InternalConfig(config_filename=config_filename, log_filename="admin_mcp.jsonl")


def arm_routers() -> None:
    """Satisfy admin routers that refuse to run until templates are set.

    The users JSON route checks that the web app called ``set_templates_and_nav``
    even though the JSON body does not use the templates.
    """
    from v4vapp_backend_v2.admin.navigation import NavigationManager
    from v4vapp_backend_v2.admin.routers import users

    users.set_templates_and_nav(object(), NavigationManager())  # type: ignore[arg-type]


def _mongo_client_is_open() -> bool:
    """True when the shared async client exists and has not been closed.

    ``AsyncMongoClient.close`` sets ``_closed`` and later operations raise
    ``InvalidOperation``. PyMongo does not publish another way to check.
    """
    client = getattr(InternalConfig, "db_client", None)
    if client is None:
        return False
    return not getattr(client, "_closed", False)


async def ensure_ready() -> None:
    """Open the shared Mongo client, or open it again if it was closed.

    Streamable HTTP starts a new MCP session for each client connection.
    The client is process-wide, so this must not treat an earlier session's
    startup as proof that the current client is still usable.
    """
    global _ready
    if _ready and _mongo_client_is_open():
        return
    async with _lock:
        if _ready and _mongo_client_is_open():
            return
        if _ready:
            logger.warning(
                "Admin MCP database client is closed. Opening it again.",
                extra={"notification": False},
            )
        from v4vapp_backend_v2.database.db_pymongo import DBConn

        conn = DBConn()
        # ``DBConn._setup`` lives on the instance that first opened Mongo.
        # A closed shared client has to be replaced, so clear the mark when
        # this instance already carries it.
        if not _mongo_client_is_open():
            conn._setup = False
        try:
            await conn.setup_database()
            arm_routers()
        except Exception:
            _ready = False
            raise
        if not _mongo_client_is_open():
            _ready = False
            raise RuntimeError("Admin MCP database client did not open")
        _ready = True
        logger.info("Admin MCP database ready", extra={"notification": False})


async def shutdown() -> None:
    """Close the async Mongo client when the MCP process stops.

    This is not safe to call at the end of an HTTP session. That session
    shares the client with every other session in the process.
    """
    global _ready
    if not _ready:
        return
    _ready = False
    instance = InternalConfig._instance
    if instance is None:
        return
    try:
        await instance.close_db_clients_async()
    except Exception:
        logger.exception("Admin MCP database shutdown failed", extra={"notification": False})


def unwrap(value: Any) -> Any:
    """Turn a FastAPI response into a plain JSON value."""
    if isinstance(value, JSONResponse):
        payload: Any = json.loads(value.body) if value.body else None
        if value.status_code >= 400:
            if isinstance(payload, dict):
                payload.setdefault("success", False)
                payload.setdefault("status_code", value.status_code)
            else:
                payload = {
                    "success": False,
                    "error": payload,
                    "status_code": value.status_code,
                }
        return payload
    return value


def to_jsonable(value: Any) -> Any:
    """Encode datetimes, decimals, and models for an MCP text result."""
    return jsonable_encoder(value)


def cap_text(text: str, limit: int = 24_000) -> str:
    """Keep a printout inside a single tool result."""
    if len(text) <= limit:
        return text
    omitted = len(text) - limit
    return (
        text[:limit]
        + f"\n\n… truncated {omitted} characters. Narrow the account, period, or search."
    )


def confirmation_required(action: str, requested: dict[str, Any] | None = None) -> dict[str, Any]:
    """Refuse a write until the caller passes confirm=true."""
    payload: dict[str, Any] = {
        "success": False,
        "confirm_required": True,
        "error": f"{action} did not run. Call again with confirm=true to apply it.",
    }
    if requested is not None:
        payload["requested"] = requested
    return payload
