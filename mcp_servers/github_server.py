"""
MCP server: the repository that records every approved pipeline.

Tools
  commit_pipeline_yaml   write pipelines/<name>.yaml and return the commit SHA

Run:  python mcp_servers/github_server.py
"""
from typing import Dict

from mcp.server.fastmcp import FastMCP

from backend.services.stack_clients import github

server = FastMCP("github")


@server.tool()
def commit_pipeline_yaml(pipeline_name: str, pipeline_yaml: str, message: str) -> Dict[str, str]:
    """Commit an approved pipeline definition. Returns the commit SHA and file path."""
    commit_sha = github.commit_pipeline(pipeline_name, pipeline_yaml, message)
    return {"commit_sha": commit_sha, "short_sha": commit_sha[:7],
            "path": github.file_path(pipeline_name)}


if __name__ == "__main__":
    server.run()
