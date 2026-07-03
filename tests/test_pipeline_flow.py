"""
End-to-end smoke test: question -> generated pipeline -> review -> approve -> live.

No servers are needed. Postgres, the fine-tuned model and the MCP tools are replaced
with stubs, and the studio database is a temporary SQLite file, so this test checks the
wiring between the routers, the agents, the validator and the SQL builder.

    pytest tests/
"""
import asyncio
import json
import os
import tempfile

import pytest

os.environ["STUDIO_DB_URL"] = f"sqlite:///{tempfile.mktemp(suffix='.db')}"

from fastapi.testclient import TestClient                      # noqa: E402

from backend import dependencies                               # noqa: E402
from backend.database import initialise_database               # noqa: E402
from backend.main import app                                   # noqa: E402
from backend.routers import business_routes                    # noqa: E402
from backend.services.model_generator import GeneratedPipeline  # noqa: E402
from backend.services.stack_clients import PostgresColumn, PostgresTable  # noqa: E402

QUESTION = "Alert me when inventory falls below 5 units in any warehouse"

PIPELINE_YAML = """pipeline:
  name: low_inventory_alert
  version: "1.0"
  source:
    type: postgres_cdc
    connection: megastore_primary
    tables:
      - table: inventory
        primary_key: inventory_id
        capture_mode: cdc_only
    kafka_topic: megastore.retail_events
  transforms:
    - id: filter_low_stock
      type: filter
      source: inventory
      conditions:
        - field: quantity_on_hand
          operator: LESS_THAN
          value: 5
    - id: hourly_totals
      type: window_aggregation
      source: filter_low_stock
      window:
        type: TUMBLING
        size: 1
        unit: HOURS
        watermark_field: updated_at
      group_by:
        - warehouse_id
      aggregations:
        - alias: low_stock_skus
          function: COUNT
          field: product_id
  sink:
    - id: snowflake_low_stock
      type: snowflake
      source: hourly_totals
      connection: megastore_snowflake
      database: MEGASTORE_DW
      schema: ANALYTICS
      table: HOURLY_LOW_STOCK
      write_mode: APPEND
"""

INVENTORY_TABLE = PostgresTable(name="inventory", columns=[
    PostgresColumn(name="inventory_id", data_type="bigint", is_primary_key=True),
    PostgresColumn(name="product_id", data_type="bigint"),
    PostgresColumn(name="warehouse_id", data_type="bigint"),
    PostgresColumn(name="quantity_on_hand", data_type="integer"),
    PostgresColumn(name="updated_at", data_type="timestamp without time zone"),
])

MCP_RESULTS = {
    "list_postgres_tables": ["inventory", "products", "warehouses"],
    "commit_pipeline_yaml": {"commit_sha": "a3f9c12deadbeef", "short_sha": "a3f9c12",
                             "path": "pipelines/low_inventory_alert.yaml"},
    "ensure_kafka_topics": {"created": ["megastore.retail_events.public.inventory"]},
    "register_debezium_connector": {"connector": "megastore-cdc-low_inventory_alert",
                                    "state": "RUNNING"},
    "submit_flink_pipeline": {"job_id": "9f2c4a1b8e7d6c5b4a39281706f5e4d3",
                              "statement_count": 7},
    "check_snowflake_table_exists": True,
    "send_slack_message": True,
}


@pytest.fixture(autouse=True)
def stub_every_external_system(monkeypatch):
    """Replace Postgres, the model and the MCP tools with predictable stubs."""
    initialise_database()

    monkeypatch.setattr(dependencies.postgres_schema, "describe_tables",
                        lambda names: {"inventory": INVENTORY_TABLE} if "inventory" in names else {})
    monkeypatch.setattr(dependencies.model_generator, "generate",
                        lambda question: GeneratedPipeline(
                            yaml_text=PIPELINE_YAML, model_name="stub-model",
                            generation_time_ms=1234))

    async def fake_call_tool(tool_name, **arguments):
        return MCP_RESULTS[tool_name]

    monkeypatch.setattr(dependencies.toolbox, "call_tool", fake_call_tool)
    # Drive the background work from the test instead of letting it run detached.
    monkeypatch.setattr(business_routes, "start_background_work",
                        lambda coroutine: coroutine.close())


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


def test_question_becomes_a_live_pipeline(client):
    # 1. a business user asks a question
    response = client.post("/api/v1/pipelines/ask", json={"question": QUESTION,
                                                          "requested_by": "Priya"})
    assert response.status_code == 200
    request_id = response.json()["request_id"]

    # 2. the authoring agent generates and validates the pipeline
    asyncio.run(business_routes.build_pipeline_for_request(request_id))

    status = client.get(f"/api/v1/pipelines/{request_id}/status").json()
    assert status["status"] == "pending_review", status
    assert [step["state"] for step in status["journey_steps"]][:3] == ["done", "done", "active"]

    # 3. it reaches the engineer's queue with the compiled SQL attached
    queue = client.get("/api/v1/review/queue").json()
    assert queue["total_pending"] == 1
    assert queue["queue"][0]["request_id"] == request_id

    detail = client.get(f"/api/v1/review/{request_id}").json()
    assert detail["pipeline_name"] == "low_inventory_alert"
    assert detail["source_tables"] == ["inventory"]
    assert "CREATE TEMPORARY TABLE `inventory`" in detail["flink_sql"]
    assert "TABLE(TUMBLE(" in detail["flink_sql"]
    assert "INSERT INTO `snowflake_low_stock`" in detail["flink_sql"]
    assert detail["validation_errors"] == []

    # 4. the engineer approves; every deployment step is streamed back
    approval = client.post(f"/api/v1/review/{request_id}/approve",
                           json={"approved_by": "Arjun"})
    assert approval.status_code == 200
    steps = [json.loads(line[5:]) for line in approval.text.splitlines()
             if line.startswith("data:")]
    assert [step["name"] for step in steps] == [
        "commit_to_github", "start_change_capture", "submit_to_flink",
        "verify_result_table", "notify_requester", "finished"]
    assert all(step["status"] == "done" for step in steps)

    # 5. the pipeline is live, with the ids the deployment produced
    final = client.get(f"/api/v1/pipelines/{request_id}/status").json()
    assert final["status"] == "live"
    assert final["flink_job_id"] == "9f2c4a1b8e7d6c5b4a39281706f5e4d3"
    assert all(step["state"] == "done" for step in final["journey_steps"])


def test_invalid_pipeline_is_sent_back_instead_of_deployed(client, monkeypatch):
    """A pipeline referencing a table that does not exist must never reach review."""
    broken_yaml = PIPELINE_YAML.replace("table: inventory", "table: stock_levels")
    monkeypatch.setattr(dependencies.model_generator, "generate",
                        lambda question: GeneratedPipeline(yaml_text=broken_yaml,
                                                           model_name="stub", generation_time_ms=1))
    monkeypatch.setattr(dependencies.model_generator, "correct",
                        lambda **kwargs: GeneratedPipeline(yaml_text=broken_yaml,
                                                           model_name="stub", generation_time_ms=1))

    request_id = client.post("/api/v1/pipelines/ask",
                             json={"question": QUESTION}).json()["request_id"]
    asyncio.run(business_routes.build_pipeline_for_request(request_id))

    status = client.get(f"/api/v1/pipelines/{request_id}/status").json()
    assert status["status"] == "failed"
    assert client.get("/api/v1/review/queue").json()["total_pending"] == 0
