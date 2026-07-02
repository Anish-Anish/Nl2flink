"""
MCP server: the warehouse where pipeline results land.

Tools
  check_snowflake_table_exists  verify a sink table before declaring a pipeline live
  read_snowflake_rows           read result rows for a business answer

Run:  python mcp_servers/snowflake_server.py
"""
from typing import Any, Dict

from mcp.server.fastmcp import FastMCP

from backend.services.stack_clients import snowflake

server = FastMCP("snowflake")


@server.tool()
def check_snowflake_table_exists(full_table_name: str) -> bool:
    """True if DATABASE.SCHEMA.TABLE exists in the warehouse."""
    return snowflake.table_exists(full_table_name)


@server.tool()
def read_snowflake_rows(full_table_name: str, limit: int = 50) -> Dict[str, Any]:
    """Read rows from a result table as {columns, rows}."""
    return snowflake.fetch_rows(full_table_name, limit)


if __name__ == "__main__":
    server.run()
