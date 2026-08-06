from __future__ import annotations

from typing import Any

try:
    from mcp.server import MCPServer
except ImportError:
    MCPServer = None  # type: ignore[assignment,misc]
    from mcp.server.fastmcp import FastMCP


def create_mcp_server(**kwargs: Any) -> Any:
    """Create the high-level server supported by the installed MCP SDK."""
    if MCPServer is not None:
        return MCPServer(**kwargs)
    return FastMCP(**kwargs, json_response=True)
