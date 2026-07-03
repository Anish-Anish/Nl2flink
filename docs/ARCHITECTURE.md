# How the code works

A map of the repository: what each file does, and how one business question becomes a
running Flink job.

---

## 1. The flow, end to end

```
Priya types a question
        │
        ▼
POST /api/v1/pipelines/ask ───────────────► a row in pipeline_requests (status: generating)
        │                                    backend/routers/business_routes.py
        ▼
PipelineAuthoringAgent  (LangGraph)          backend/agents/pipeline_agent.py
        │
        ├─ ground_in_schema ── MCP ────────► which tables exist
        ├─ generate ──────────────────────► the fine-tuned 14B model returns pipeline YAML
        ├─ validate ──────────────────────► tables, columns, step graph, compiles to Flink SQL
        └─ correct ───────────────────────► on failure, re-prompt with the errors (up to 3×)
        │
        ▼
status: pending_review ─────────────────────► Arjun sees it in the review queue
        │
        │ Arjun clicks Approve
        ▼
PipelineDeploymentAgent (LangGraph)          backend/agents/deployment_agent.py
        │   every node is one MCP call
        ├─ commit_to_github ──────────────► pipelines/<name>.yaml
        ├─ start_change_capture ──────────► Kafka topics + Debezium connector on Postgres
        ├─ submit_to_flink ───────────────► the compiled SQL runs; returns a Flink job id
        ├─ verify_result_table ───────────► the Snowflake sink exists
        └─ notify_requester ──────────────► Slack message to Priya
        │
        ▼
status: live ──────────────────────────────► GET /api/v1/pipelines/{id}/answer reads the
                                             Snowflake sink and returns the real rows
```

While that runs, the data itself flows:

```
Postgres ──Debezium──► Kafka ──Flink job──► Snowflake (results)
                                        └──► Kafka egress topic ──► Slack / email / Redis
```

---

## 2. Where everything lives

```
backend/          the API and everything behind it
ui/               the Streamlit front end (talks only to /api/v1)
mcp_servers/      the MCP servers the agents act through
training/         fine-tuning — NOT touched by this application
model_server/     serves the fine-tuned model over HTTP — NOT touched
dataset/          the 2,917 question → pipeline pairs the model was trained on
schema/           the MegaStore Postgres schema
```

### `backend/` — one file per concern

| File | What it does |
|---|---|
| `config.py` | Every server endpoint and credential, in one `Settings` class. Nothing else reads environment variables. |
| `pipeline_spec.py` | Pydantic model of the YAML the fine-tuned model emits: source, the 8 transform types, the 6 sink types. `parse_pipeline_yaml()` is the first validation layer. |
| `database.py` | The `pipeline_requests` table and the status flow. One row per question, updated in place. |
| `api_models.py` | Request/response models for the API. Plain data only — no HTML, no CSS class names. |
| `dependencies.py` | The shared instances (validator, agents, clients) the routers use. |
| `main.py` | The FastAPI app: CORS, routers, startup, error handling. |

### `backend/services/` — the work

| File | What it does |
|---|---|
| `stack_clients.py` | One small client per server: Postgres schema reads, Debezium connector registration, Kafka topics and tailing, Flink SQL Gateway, Flink JobManager, Snowflake, Slack, GitHub. Also owns the topic naming rules, so the SQL builder and the deployer cannot disagree. |
| `model_generator.py` | Calls the fine-tuned model server and builds its instructions. The first attempt sends the question exactly as typed (the form the model was tuned on); schema and errors are added only when correcting. |
| `pipeline_validator.py` | Checks the YAML, that every table and column exists in Postgres, that the step graph resolves, and that the pipeline compiles. Returns green checks for the engineer and error sentences for the model. |
| `flink_sql_builder.py` | Turns a spec into runnable Flink SQL: `SET` → `CREATE TABLE` (Kafka sources) → `CREATE VIEW` (one per transform) → `CREATE TABLE` (sinks) → `INSERT`. |
| `answer_reader.py` | Reads the pipeline's Snowflake sink and derives the headline numbers from the real rows. |

### `backend/agents/` — LangGraph over MCP

| File | What it does |
|---|---|
| `mcp_client.py` | Loads the MCP servers with `langchain-mcp-adapters`; `call_tool(name, **args)` is all the graph nodes need. |
| `pipeline_agent.py` | The authoring graph: ground → generate → validate → correct. Stops at a pipeline a human can review; never deploys. |
| `deployment_agent.py` | The deployment graph: commit → change capture → Flink → verify → notify. Each node is one MCP call, and the graph stops at the first failure so nothing is ever reported live that Flink refused. |

### `backend/routers/` — the HTTP surface

| File | Endpoints |
|---|---|
| `auth_routes.py` | `POST /api/v1/auth/login` |
| `business_routes.py` | `ask`, `{id}/status`, `{id}/answer`, `history`, `suggestions`, `DELETE {id}` |
| `engineer_routes.py` | `review/queue`, `review/{id}`, `PUT review/{id}/pipeline`, `POST review/{id}/approve` (SSE), `POST review/{id}/reject`, `review/deployed` |
| `monitor_routes.py` | `monitor/metrics`, `monitor/events`, `platform/status`, `platform/mcp` |

### `mcp_servers/` — what the agents are allowed to do

| Server | Tools |
|---|---|
| `postgres_schema_server.py` | `list_postgres_tables`, `describe_postgres_tables` |
| `debezium_kafka_server.py` | `register_debezium_connector`, `get_debezium_connector_state`, `ensure_kafka_topics`, `read_recent_change_events` |
| `flink_server.py` | `submit_flink_pipeline`, `get_flink_job_state`, `get_flink_job_throughput`, `cancel_flink_job` |
| `snowflake_server.py` | `check_snowflake_table_exists`, `read_snowflake_rows` |
| `slack_server.py` | `send_slack_message`, `send_slack_webhook` |
| `github_server.py` | `commit_pipeline_yaml` |

### `ui/` — Streamlit, and only Streamlit

| File | What it does |
|---|---|
| `streamlit_app.py` | Page setup, URL parameters, persona routing. |
| `api_client.py` | The only file that makes HTTP calls. One function per endpoint. |
| `views/business_view.py` | Priya's side: chat input, journey trail, answer card. No pipeline jargon. |
| `views/engineer_view.py` | Arjun's side: MCP strip, review queue, deployed list, live monitor. |
| `components/` | `answer_card`, `journey_trail`, `review_card`, `monitor_panel`, `sidebar`, `topbar` — all of the HTML lives here. |

---

## 3. The pipeline DSL

The fine-tuned model always emits this shape, and `backend/pipeline_spec.py` types it:

```yaml
pipeline:
  name: low_inventory_alert
  version: "1.0"
  source:
    type: postgres_cdc
    connection: megastore_primary
    tables:
      - { table: inventory, primary_key: inventory_id, capture_mode: cdc_only }
    kafka_topic: megastore.retail_events
  transforms:
    - id: filter_low_stock
      type: filter
      source: inventory
      conditions:
        - { field: quantity_on_hand, operator: LESS_THAN, value: 5 }
  sink:
    - id: slack_alert
      type: slack
      source: filter_low_stock
      channel: "#inventory-alerts"
      webhook_url: "${env.SLACK_WEBHOOK_INVENTORY}"
```

**Transforms** — `filter`, `map`, `stream_join`, `anti_join`, `window_aggregation`,
`session_window`, `deduplication`, `top_n`.
**Sinks** — `snowflake`, `postgres_write`, `slack`, `email`, `redis`, `notification_service`.

Every step has an `id`, and later steps read from it by name.

---

## 4. How the DSL becomes Flink SQL

`flink_sql_builder.py` maps each part of the DSL onto one SQL construct:

| DSL | Flink SQL |
|---|---|
| `source.tables` | `CREATE TEMPORARY TABLE` per table, `connector = kafka`, `format = debezium-json` |
| `filter` | `WHERE` |
| `map` | extra `SELECT` expressions |
| `stream_join` | `INNER` / `LEFT JOIN` |
| `anti_join` | `NOT EXISTS`, optionally bounded by an interval |
| `window_aggregation` | `TABLE(TUMBLE(...))` / `HOP` / `SESSION` + `GROUP BY` |
| `session_window` | `TABLE(SESSION(... PARTITION BY ...))` |
| `deduplication` | `ROW_NUMBER() = 1` |
| `top_n` | `ROW_NUMBER() <= n` |
| `snowflake`, `postgres_write` sinks | `CREATE TEMPORARY TABLE` with `connector = jdbc` |
| `slack`, `email`, `redis`, `notification_service` sinks | `CREATE TEMPORARY TABLE` with `connector = kafka` on an egress topic |

Each transform becomes a `CREATE TEMPORARY VIEW` named after its `id`, so the SQL reads
in the same order as the YAML. When a pipeline has several sinks the inserts are wrapped
in `EXECUTE STATEMENT SET`, so the whole pipeline is one Flink job with one job id and
one checkpoint.

**Why delivery sinks go through Kafka.** Flink SQL has no connector for a Slack channel
or an SMTP server. Those sinks write JSON to `megastore.egress.<pipeline>.<sink id>`,
and the delivery is performed from that topic using the channel, template or key pattern
in the YAML. Database sinks need no such hop — JDBC writes them directly.

---

## 5. Two design rules worth knowing

**Agent actions go through MCP; plain reads do not.** Anything that changes the world —
committing, registering a connector, submitting a job, sending a Slack message — is an
MCP tool call made by a LangGraph node. Read-only calls on the request path (reading
answer rows, checking Flink throughput for the monitor) call the client directly, because
starting an MCP subprocess per page refresh would only add latency.

**The API returns data, never markup.** Every response is plain JSON: numbers, strings,
enums, rows. All HTML is built in `ui/components/`. That is what makes it possible to
replace Streamlit with React later without touching a single backend file.

---

## 6. Running it

```bash
pip install -r requirements.txt
cp .env.example .env            # point it at your servers

# 1. the fine-tuned model (on the GPU box)
python model_server/model_server.py

# 2. the API
uvicorn backend.main:app --port 8000 --reload

# 3. the UI
streamlit run ui/streamlit_app.py
```

The MCP servers are started by the agents as subprocesses — nothing to run by hand.
To try one on its own:

```bash
PYTHONPATH=. python mcp_servers/postgres_schema_server.py
```

`GET /api/v1/platform/status` reports which servers the backend can currently reach, and
`GET /api/v1/platform/mcp` does the same for the MCP servers.

---

## 7. What is deliberately untouched

`training/`, `model_server/` and `unsloth_compiled_cache/` are fine-tuning artefacts.
The backend only speaks to the model server's existing HTTP contract
(`POST /generate {"query": ...} → {"yaml": ...}`); no file in those folders is imported
or modified.
