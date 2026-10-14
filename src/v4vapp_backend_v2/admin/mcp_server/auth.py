"""Bearer token check for the admin MCP HTTP server.

The token is read from ``V4VAPP_ADMIN_MCP_TOKEN``. Clients send it as
``Authorization: Bearer <token>``. The comparison does not log the token.
"""

from __future__ import annotations

import os
import secrets
from collections.abc import Iterable, Mapping

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

TOKEN_ENV = "V4VAPP_ADMIN_MCP_TOKEN"
MIN_TOKEN_LENGTH = 16


def require_access_token(environ: Mapping[str, str] | None = None) -> str:
    """Return the configured token, or raise if it is missing or too short."""
    source = os.environ if environ is None else environ
    token = source.get(TOKEN_ENV, "").strip()
    if len(token) < MIN_TOKEN_LENGTH:
        raise ValueError(
            f"{TOKEN_ENV} must be a random string of at least {MIN_TOKEN_LENGTH} characters."
        )
    return token


def authorization_matches(header_value: str | None, token: str) -> bool:
    """True when the header is ``Bearer`` plus the expected token."""
    if not header_value:
        return False
    scheme, _, presented = header_value.partition(" ")
    presented = presented.strip()
    if scheme.lower() != "bearer" or not presented or any(char.isspace() for char in presented):
        return False
    if len(presented) != len(token):
        return False
    return secrets.compare_digest(presented, token)


def _header(scope: Scope, name: bytes) -> str | None:
    headers: Iterable[tuple[bytes, bytes]] = scope.get("headers") or []
    for key, value in headers:
        if key.lower() == name:
            return value.decode("latin-1")
    return None


class BearerTokenMiddleware:
    """Reject HTTP requests that do not carry the admin MCP access token.

    ``public_paths`` stay open. ``/health`` is the only public path, so a
    container probe can run without the token. Lifespan and other non-HTTP
    scopes are forwarded unchanged.
    """

    def __init__(
        self,
        app: ASGIApp,
        token: str,
        public_paths: frozenset[str] = frozenset(),
    ) -> None:
        self.app = app
        self.token = token
        self.public_paths = public_paths

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "")
        if path in self.public_paths:
            await self.app(scope, receive, send)
            return
        if authorization_matches(_header(scope, b"authorization"), self.token):
            await self.app(scope, receive, send)
            return
        response = JSONResponse({"error": "Unauthorized"}, status_code=401)
        response.headers["WWW-Authenticate"] = "Bearer"
        await response(scope, receive, send)
