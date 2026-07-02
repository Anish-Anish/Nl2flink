"""
MCP server: read the live MegaStore Postgres schema.

Tools
  list_postgres_tables        every table in the database
  describe_postgres_tables    columns and types for the tables you name

Run:  python mcp_servers/postgres_schema_server.py
"""
from typing import Dict, List

from mcp.server.fastmcp import FastMCP

from backend.services.stack_clients import postgres_schema

server = FastMCP("postgres-schema")


@server.tool()
def list_postgres_tables() -> List[str]:
    """Return the name of every table in the MegaStore database."""
    return postgres_schema.list_table_names()


@server.tool()
def describe_postgres_tables(table_names: List[str]) -> Dict[str, List[Dict[str, str]]]:
    """Return the columns of the given tables as {table: [{name, data_type}, ...]}."""
    described = postgres_schema.describe_tables(table_names)
    return {name: [{"name": c.name, "data_type": c.data_type} for c in table.columns]
            for name, table in described.items()}


if __name__ == "__main__":
    server.run()
