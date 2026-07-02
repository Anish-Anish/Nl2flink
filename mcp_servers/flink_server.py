"""
MCP server: submit and manage Flink streaming jobs.

Tools
  submit_flink_pipeline    run a compiled SQL script and return the job id
  get_flink_job_state      RUNNING / FINISHED / FAILED
  get_flink_job_throughput records per second
  cancel_flink_job         stop a running pipeline

Run:  python mcp_servers/flink_server.py
"""
from typing import Any, Dict, List

from mcp.server.fastmcp import FastMCP

from backend.services.stack_clients import flink_jobs, flink_sql

server = FastMCP("flink")


@server.tool()
def submit_flink_pipeline(statements: List[str], job_name: str) -> Dict[str, Any]:
    """Execute the pipeline's SQL statements in one session and return the Flink job id."""
    job_id = flink_sql.run_statements(statements, job_name)
    return {"job_id": job_id, "statement_count": len(statements)}


@server.tool()
def get_flink_job_state(job_id: str) -> str:
    """Current state of a Flink job."""
    return flink_jobs.job_state(job_id)


@server.tool()
def get_flink_job_throughput(job_id: str) -> float:
    """Records per second the job is currently producing."""
    return flink_jobs.records_per_second(job_id)


@server.tool()
def cancel_flink_job(job_id: str) -> Dict[str, str]:
    """Cancel a running Flink job."""
    flink_jobs.cancel_job(job_id)
    return {"job_id": job_id, "state": "CANCELLING"}


if __name__ == "__main__":
    server.run()
