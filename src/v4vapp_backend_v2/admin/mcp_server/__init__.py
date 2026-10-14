"""MCP server for the V4VApp admin interface.

Run it with ``v4vapp-admin-mcp``. It speaks MCP over stdin and stdout and uses
the same config, MongoDB, and Redis as the admin web UI.
"""

__all__ = ["main"]


def main() -> None:
    """Start the admin MCP server. Imported lazily so tests can load helpers."""
    from v4vapp_backend_v2.admin.mcp_server.server import main as run_server

    run_server()
