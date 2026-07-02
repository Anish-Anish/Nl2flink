"""
Connects the agents to the MCP servers in mcp_servers/.

Uses LangGraph's MCP adapter (langchain-mcp-adapters), so every tool a server exposes
becomes a LangChain tool the graph nodes can call by name. The servers run as stdio
subprocesses; point MCP_* settings elsewhere to use remote ones.

`call_tool("submit_flink_pipeline", statements=..., job_name=...)` is the only thing
the graph nodes need.
"""
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from backend.api_models import McpToolStatus

logger = logging.getLogger(__name__)

_SERVER_DIRECTORY = Path(__file__).resolve().parents[2] / "mcp_servers"
_PROJECT_ROOT = Path(__file__).resolve().parents[2]

#: name -> what it is for. The file is mcp_servers/<name>_server.py.
MCP_SERVERS = {
    "postgres_schema": "Reads live table and column definitions",
    "debezium_kafka": "Starts change data capture and manages topics",
    "flink": "Submits and manages streaming jobs",
    "snowflake": "Verifies sink tables and reads results",
    "slack": "Notifies requesters and posts alerts",
    "github": "Commits approved pipeline definitions",
}


def _server_connections() -> Dict[str, Dict[str, Any]]:
    """stdio launch configuration for each server."""
    environment = {**os.environ, "PYTHONPATH": str(_PROJECT_ROOT)}
    return {
        name: {
            "command": sys.executable,
            "args": [str(_SERVER_DIRECTORY / f"{name}_server.py")],
            "transport": "stdio",
            "env": environment,
        }
        for name in MCP_SERVERS
    }


class McpToolbox:
    """Loads the MCP tools once and calls them by name."""

    def __init__(self) -> None:
        self._tools: Optional[Dict[str, Any]] = None
        self._lock = asyncio.Lock()

    async def tools(self) -> Dict[str, Any]:
        """All tools from all servers, keyed by tool name. Loaded once per process."""
        async with self._lock:
            if self._tools is None:
                from langchain_mcp_adapters.client import MultiServerMCPClient

                client = MultiServerMCPClient(_server_connections())
                loaded = await client.get_tools()
                self._tools = {tool.name: tool for tool in loaded}
                logger.info("loaded %d MCP tools from %d servers",
                            len(self._tools), len(MCP_SERVERS))
            return self._tools

    async def call_tool(self, tool_name: str, **arguments: Any) -> Any:
        """Invoke one MCP tool. Raises KeyError if no server exposes that name."""
        tools = await self.tools()
        if tool_name not in tools:
            raise KeyError(f"MCP tool '{tool_name}' is not available "
                           f"(loaded: {sorted(tools)})")
        logger.info("calling MCP tool %s", tool_name)
        return await tools[tool_name].ainvoke(arguments)

    async def status(self) -> List[McpToolStatus]:
        """Per-server connection state, for the engineer's MCP strip."""
        try:
            tools = await self.tools()
        except Exception as error:
            logger.warning("MCP servers unavailable: %s", error)
            return [McpToolStatus(name=name, status="unavailable", description=description)
                    for name, description in MCP_SERVERS.items()]

        counts = await self._tool_counts_per_server()
        return [
            McpToolStatus(
                name=name,
                status="connected" if counts.get(name) else "connecting",
                tool_count=counts.get(name, 0),
                description=description,
            )
            for name, description in MCP_SERVERS.items()
        ]

    async def _tool_counts_per_server(self) -> Dict[str, int]:
        """Ask each server on its own how many tools it serves."""
        from langchain_mcp_adapters.client import MultiServerMCPClient

        counts: Dict[str, int] = {}
        connections = _server_connections()
        for name, connection in connections.items():
            try:
                client = MultiServerMCPClient({name: connection})
                counts[name] = len(await client.get_tools())
            except Exception as error:
                logger.warning("MCP server %s did not respond: %s", name, error)
                counts[name] = 0
        return counts


#: One toolbox for the whole process.
mcp_toolbox = McpToolbox()
