"""
MCP server: change data capture from Postgres into Kafka.

Tools
  register_debezium_connector  start capturing a pipeline's tables
  get_debezium_connector_state RUNNING / FAILED / ...
  ensure_kafka_topics          create a pipeline's topics if they are missing
  read_recent_change_events    tail a topic (used by the engineer's live monitor)

Run:  python mcp_servers/debezium_kafka_server.py
"""
from typing import Any, Dict, List

from mcp.server.fastmcp import FastMCP

from backend.pipeline_spec import parse_pipeline_yaml
from backend.services.stack_clients import debezium, kafka

server = FastMCP("debezium-kafka")


@server.tool()
def register_debezium_connector(pipeline_yaml: str) -> Dict[str, str]:
    """Register the Postgres CDC connector for the tables this pipeline captures."""
    spec = parse_pipeline_yaml(pipeline_yaml)
    connector_name = debezium.register_connector(spec.name, spec.source)
    return {"connector": connector_name, "state": debezium.connector_state(connector_name)}


@server.tool()
def get_debezium_connector_state(connector_name: str) -> str:
    """Current state of a Debezium connector."""
    return debezium.connector_state(connector_name)


@server.tool()
def ensure_kafka_topics(topic_names: List[str]) -> Dict[str, Any]:
    """Create any of these topics that does not exist yet."""
    return {"created": kafka.ensure_topics(topic_names)}


@server.tool()
def read_recent_change_events(topic: str, limit: int = 25) -> List[Dict[str, Any]]:
    """Read the newest change events from a topic."""
    return kafka.read_recent_events(topic, limit)


if __name__ == "__main__":
    server.run()
