"""
Thin clients for every server in the stack, one small class each.

    PostgresSchemaReader  reads live tables/columns (grounding + validation)
    DebeziumClient        registers the Postgres CDC connector for a pipeline
    KafkaClient           creates the pipeline's topics, tails them for the monitor
    FlinkSqlGateway       runs the compiled SQL and returns the Flink job id
    FlinkJobManager       job state, throughput, cancellation
    SnowflakeClient       verifies sink tables and reads result rows for answers
    SlackClient           posts notifications and pipeline alerts
    GitHubClient          commits approved pipeline YAML

Everything here only talks to a server; no business rules live in this file.
"""
import base64
import json
import logging
import re
import time
from typing import Any, Dict, List, Optional

import requests
from pydantic import BaseModel
from sqlalchemy import create_engine, text

from backend.config import Settings, get_settings

logger = logging.getLogger(__name__)


class StackError(RuntimeError):
    """A server in the stack failed. Carries the service name so the API can say which."""

    def __init__(self, service: str, message: str):
        super().__init__(f"{service}: {message}")
        self.service = service
        self.message = message


def _request_json(service: str, method: str, url: str, timeout: float, **kwargs) -> Any:
    """One HTTP call with uniform error handling and JSON decoding."""
    try:
        response = requests.request(method, url, timeout=timeout, **kwargs)
    except requests.RequestException as error:
        raise StackError(service, f"{method} {url} failed: {error}") from error
    if response.status_code >= 400:
        raise StackError(service, f"{method} {url} returned {response.status_code}: "
                                  f"{response.text[:300]}")
    if not response.content:
        return None
    try:
        return response.json()
    except ValueError:
        return response.text


def _is_reachable(url: str, timeout: float = 3.0) -> bool:
    try:
        return requests.get(url, timeout=timeout).status_code < 500
    except requests.RequestException:
        return False


# ── naming rules shared by the SQL builder and the deployer ──────────────────
def cdc_topic_name(topic_prefix: str, postgres_schema: str, table: str) -> str:
    """Topic Debezium publishes one table's change events to."""
    return f"{topic_prefix}.{postgres_schema}.{table}"


def egress_topic_name(pipeline_name: str, sink_id: str) -> str:
    """Topic a Slack/email/Redis sink writes to, for the dispatcher to deliver."""
    return f"megastore.egress.{pipeline_name}.{sink_id}"


# ══════════════════════════════════════════════════════════════════════════════
#  POSTGRES — the source database
# ══════════════════════════════════════════════════════════════════════════════
class PostgresColumn(BaseModel):
    name: str
    data_type: str
    is_primary_key: bool = False


class PostgresTable(BaseModel):
    name: str
    columns: List[PostgresColumn] = []

    def column_names(self) -> List[str]:
        return [column.name for column in self.columns]

    def has_column(self, name: str) -> bool:
        return any(column.name == name for column in self.columns)

    def describe(self) -> str:
        """One compact line, the form used in the model prompt."""
        return f"{self.name}({', '.join(f'{c.name} {c.data_type}' for c in self.columns)})"


_TABLES_SQL = text("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = :schema AND table_type = 'BASE TABLE' ORDER BY table_name
""")

_COLUMNS_SQL = text("""
    SELECT table_name, column_name, data_type FROM information_schema.columns
    WHERE table_schema = :schema AND table_name = ANY(:tables)
    ORDER BY table_name, ordinal_position
""")

_PRIMARY_KEYS_SQL = text("""
    SELECT tc.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_schema = :schema AND tc.table_name = ANY(:tables)
""")


class PostgresSchemaReader:
    """Reads the live table layout of the operational database."""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._schema = settings.postgres_schema
        self._engine = None          # created on first use, so importing needs no driver
        self._cache: Dict[str, PostgresTable] = {}

    def _get_engine(self):
        if self._engine is None:
            self._engine = create_engine(self._settings.postgres_url, pool_pre_ping=True)
        return self._engine

    def list_table_names(self) -> List[str]:
        return [row[0] for row in self._query(_TABLES_SQL, {"schema": self._schema})]

    def describe_tables(self, table_names: List[str]) -> Dict[str, PostgresTable]:
        """Columns of the given tables, keyed by table name. Missing tables are omitted."""
        wanted = [name for name in dict.fromkeys(table_names) if name]
        missing = [name for name in wanted if name not in self._cache]
        if missing:
            self._load(missing)
        return {name: self._cache[name] for name in wanted if name in self._cache}

    def is_reachable(self) -> bool:
        try:
            self._query(text("SELECT 1"), {})
            return True
        except StackError:
            return False

    def clear_cache(self) -> None:
        self._cache.clear()

    def _load(self, table_names: List[str]) -> None:
        params = {"schema": self._schema, "tables": table_names}
        primary_keys: Dict[str, set] = {}
        for table_name, column_name in self._query(_PRIMARY_KEYS_SQL, params):
            primary_keys.setdefault(table_name, set()).add(column_name)

        grouped: Dict[str, List[PostgresColumn]] = {}
        for table_name, column_name, data_type in self._query(_COLUMNS_SQL, params):
            grouped.setdefault(table_name, []).append(PostgresColumn(
                name=column_name, data_type=data_type,
                is_primary_key=column_name in primary_keys.get(table_name, set())))
        for table_name, columns in grouped.items():
            self._cache[table_name] = PostgresTable(name=table_name, columns=columns)

    def _query(self, statement, params: dict) -> List[tuple]:
        try:
            with self._get_engine().connect() as connection:
                return list(connection.execute(statement, params).fetchall())
        except Exception as error:
            raise StackError("postgres", str(error)) from error


# ══════════════════════════════════════════════════════════════════════════════
#  DEBEZIUM — Postgres change data capture into Kafka
# ══════════════════════════════════════════════════════════════════════════════
class DebeziumClient:
    """Registers and inspects the CDC connector that feeds a pipeline's topics."""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._base_url = settings.debezium_url.rstrip("/")
        self._timeout = settings.http_timeout_seconds

    def connector_name(self, pipeline_name: str) -> str:
        return f"megastore-cdc-{pipeline_name}"

    def register_connector(self, pipeline_name: str, source) -> str:
        """Create or update this pipeline's connector (PUT, so re-deploying is safe)."""
        name = self.connector_name(pipeline_name)
        _request_json("debezium", "PUT", f"{self._base_url}/connectors/{name}/config",
                      self._timeout, json=self.build_connector_config(pipeline_name, source))
        logger.info("registered debezium connector %s for %s", name, source.table_names())
        return name

    def build_connector_config(self, pipeline_name: str, source) -> Dict[str, str]:
        """Kafka Connect configuration for the tables this pipeline captures."""
        settings = self._settings
        tables = ",".join(f"{settings.postgres_schema}.{t.table}" for t in source.tables)
        needs_snapshot = any(t.capture_mode.value == "full_snapshot_then_cdc" for t in source.tables)
        slot = re.sub(r"[^a-z0-9_]", "_", pipeline_name.lower())[:50]
        return {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "database.hostname": settings.postgres_host,
            "database.port": str(settings.postgres_port),
            "database.user": settings.postgres_user,
            "database.password": settings.postgres_password,
            "database.dbname": settings.postgres_db,
            "topic.prefix": source.kafka_topic,
            "schema.include.list": settings.postgres_schema,
            "table.include.list": tables,
            "plugin.name": "pgoutput",
            "publication.name": settings.debezium_publication,
            "publication.autocreate.mode": "filtered",
            "slot.name": f"slot_{slot}",
            "snapshot.mode": "initial" if needs_snapshot else "never",
            "tombstones.on.delete": "false",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",
            "decimal.handling.mode": "double",
            "time.precision.mode": "connect",
        }

    def connector_state(self, name: str) -> str:
        payload = _request_json("debezium", "GET", f"{self._base_url}/connectors/{name}/status",
                                self._timeout) or {}
        return (payload.get("connector") or {}).get("state", "UNKNOWN")

    def is_reachable(self) -> bool:
        return _is_reachable(f"{self._base_url}/connectors")


# ══════════════════════════════════════════════════════════════════════════════
#  KAFKA — the change-event bus
# ══════════════════════════════════════════════════════════════════════════════
class KafkaClient:
    """Creates a pipeline's topics and tails them for the live event feed."""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._servers = settings.kafka_bootstrap_servers.split(",")

    def ensure_topics(self, topic_names: List[str]) -> List[str]:
        """Create any topic that does not exist. Returns the ones that were created."""
        from kafka.admin import KafkaAdminClient, NewTopic

        admin = KafkaAdminClient(bootstrap_servers=self._servers, client_id="megastore-studio")
        try:
            missing = [name for name in dict.fromkeys(topic_names) if name not in admin.list_topics()]
            if missing:
                admin.create_topics([
                    NewTopic(name=name,
                             num_partitions=self._settings.kafka_partitions,
                             replication_factor=self._settings.kafka_replication_factor)
                    for name in missing])
                logger.info("created kafka topics %s", missing)
            return missing
        except Exception as error:
            raise StackError("kafka", str(error)) from error
        finally:
            admin.close()

    def read_recent_events(self, topic: str, limit: int = 25) -> List[Dict[str, Any]]:
        """Tail a topic. Returns [] rather than raising when nothing is there yet."""
        from kafka import KafkaConsumer, TopicPartition

        consumer = KafkaConsumer(bootstrap_servers=self._servers, enable_auto_commit=False,
                                 consumer_timeout_ms=2000, group_id=None)
        try:
            partition_ids = consumer.partitions_for_topic(topic) or set()
            if not partition_ids:
                return []
            partitions = [TopicPartition(topic, pid) for pid in partition_ids]
            consumer.assign(partitions)
            end_offsets = consumer.end_offsets(partitions)
            per_partition = max(1, limit // len(partitions))
            for partition in partitions:
                consumer.seek(partition, max(0, end_offsets[partition] - per_partition))
            events = [self._decode(record) for record in consumer]
            return sorted(events, key=lambda e: e["timestamp_ms"], reverse=True)[:limit]
        except Exception as error:
            logger.warning("could not tail %s: %s", topic, error)
            return []
        finally:
            consumer.close()

    def is_reachable(self) -> bool:
        from kafka.admin import KafkaAdminClient
        try:
            admin = KafkaAdminClient(bootstrap_servers=self._servers, client_id="megastore-health")
            admin.list_topics()
            admin.close()
            return True
        except Exception:
            return False

    @staticmethod
    def _decode(record) -> Dict[str, Any]:
        """Unwrap a Debezium envelope: `after` holds the new row, `op` the change type."""
        try:
            envelope = json.loads(record.value.decode("utf-8")) if record.value else {}
        except (ValueError, AttributeError, UnicodeDecodeError):
            envelope = {}
        row = envelope.get("after") or envelope.get("before") or {}
        return {
            "timestamp_ms": record.timestamp,
            "operation": envelope.get("op"),
            "table": (envelope.get("source") or {}).get("table"),
            "row": row if isinstance(row, dict) else {},
        }


# ══════════════════════════════════════════════════════════════════════════════
#  FLINK — statement execution and job control
# ══════════════════════════════════════════════════════════════════════════════
class FlinkSqlGateway:
    """Runs SQL statements on the Flink SQL Gateway and returns the submitted job id."""

    def __init__(self, settings: Settings):
        self._base_url = settings.flink_sql_gateway_url.rstrip("/")
        self._settings = settings
        self._timeout = settings.http_timeout_seconds

    def run_statements(self, statements: List[str], job_name: str) -> Optional[str]:
        """Execute statements in order in one session; returns the Flink job id."""
        session = self._open_session(job_name)
        job_id = None
        try:
            for statement in statements:
                rows = self._execute(session, statement)
                job_id = self._find_job_id(rows) or job_id
            return job_id
        finally:
            self._close_session(session)

    def is_reachable(self) -> bool:
        return _is_reachable(f"{self._base_url}/info")

    # ── internals ────────────────────────────────────────────────────────────
    def _open_session(self, job_name: str) -> str:
        payload = {"sessionName": job_name, "properties": {
            "execution.runtime-mode": "streaming",
            "parallelism.default": str(self._settings.flink_parallelism),
            "execution.checkpointing.interval": f"{self._settings.flink_checkpoint_interval_ms}ms",
        }}
        response = _request_json("flink", "POST", f"{self._base_url}/sessions",
                                 self._timeout, json=payload) or {}
        handle = response.get("sessionHandle")
        if not handle:
            raise StackError("flink", "SQL gateway returned no session handle")
        return handle

    def _close_session(self, session: str) -> None:
        try:
            _request_json("flink", "DELETE", f"{self._base_url}/sessions/{session}", self._timeout)
        except StackError as error:
            logger.warning("could not close flink session: %s", error)

    def _execute(self, session: str, statement: str) -> List[List[Any]]:
        response = _request_json("flink", "POST", f"{self._base_url}/sessions/{session}/statements",
                                 self._timeout, json={"statement": statement}) or {}
        operation = response.get("operationHandle")
        if not operation:
            raise StackError("flink", f"no operation handle for: {statement[:120]}")
        self._wait(session, operation, statement)
        return self._fetch_rows(session, operation)

    def _wait(self, session: str, operation: str, statement: str) -> None:
        deadline = time.monotonic() + 120
        url = f"{self._base_url}/sessions/{session}/operations/{operation}/status"
        while time.monotonic() < deadline:
            status = (_request_json("flink", "GET", url, self._timeout) or {}).get("status", "")
            if status == "ERROR":
                raise StackError("flink", f"statement failed: {statement[:200]}")
            if status in ("FINISHED", "CANCELED", "CLOSED"):
                return
            time.sleep(0.4)
        raise StackError("flink", f"statement timed out: {statement[:200]}")

    def _fetch_rows(self, session: str, operation: str) -> List[List[Any]]:
        url = f"{self._base_url}/sessions/{session}/operations/{operation}/result/0"
        page = _request_json("flink", "GET", url, self._timeout, params={"rowFormat": "JSON"}) or {}
        results = page.get("results") or {}
        return [entry.get("fields", []) for entry in results.get("data", [])]

    @staticmethod
    def _find_job_id(rows: List[List[Any]]) -> Optional[str]:
        """An INSERT returns a single row holding the Flink job id (32 hex characters)."""
        for row in rows:
            for value in row:
                if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{32}", value):
                    return value
        return None


class FlinkJobManager:
    """Reads job state and throughput, and cancels jobs."""

    def __init__(self, settings: Settings):
        self._base_url = settings.flink_job_manager_url.rstrip("/")
        self._timeout = settings.http_timeout_seconds

    def cluster_overview(self) -> Dict[str, Any]:
        return _request_json("flink", "GET", f"{self._base_url}/overview", self._timeout) or {}

    def list_jobs(self) -> List[Dict[str, Any]]:
        payload = _request_json("flink", "GET", f"{self._base_url}/jobs/overview",
                                self._timeout) or {}
        return payload.get("jobs", [])

    def job_state(self, job_id: str) -> str:
        job = next((j for j in self.list_jobs() if j.get("jid") == job_id), None)
        return job.get("state", "UNKNOWN") if job else "NOT_FOUND"

    def records_per_second(self, job_id: str) -> float:
        """Sum numRecordsOutPerSecond over the job's operators. 0.0 when not reporting yet."""
        try:
            detail = _request_json("flink", "GET", f"{self._base_url}/jobs/{job_id}",
                                   self._timeout) or {}
        except StackError:
            return 0.0
        total = 0.0
        for vertex in detail.get("vertices", []):
            vertex_id = vertex.get("id")
            if not vertex_id:
                continue
            try:
                metrics = _request_json(
                    "flink", "GET", f"{self._base_url}/jobs/{job_id}/vertices/{vertex_id}"
                                    f"/subtasks/metrics", self._timeout,
                    params={"get": "numRecordsOutPerSecond", "agg": "sum"}) or []
            except StackError:
                continue
            for metric in metrics:
                if metric.get("id") == "numRecordsOutPerSecond":
                    total += float(metric.get("sum", 0) or 0)
        return round(total, 2)

    def cancel_job(self, job_id: str) -> None:
        _request_json("flink", "PATCH", f"{self._base_url}/jobs/{job_id}?mode=cancel", self._timeout)

    def is_reachable(self) -> bool:
        return _is_reachable(f"{self._base_url}/overview")


# ══════════════════════════════════════════════════════════════════════════════
#  SNOWFLAKE — where results land
# ══════════════════════════════════════════════════════════════════════════════
class SnowflakeClient:
    """Read-only access to the warehouse: verify sink tables, fetch answer rows."""

    def __init__(self, settings: Settings):
        self._settings = settings

    def query(self, sql: str, params: tuple = ()) -> Dict[str, Any]:
        """Run a query; returns {"columns": [...], "rows": [[...], ...]}."""
        connection = self._connect()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(sql, params)
                columns = [description[0] for description in (cursor.description or [])]
                return {"columns": columns, "rows": [list(row) for row in cursor.fetchall()]}
            finally:
                cursor.close()
        except Exception as error:
            raise StackError("snowflake", f"{error}") from error
        finally:
            connection.close()

    def table_exists(self, full_table_name: str) -> bool:
        database, schema, table = full_table_name.split(".")
        result = self.query(
            f"SELECT COUNT(*) FROM {database}.INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s", (schema.upper(), table.upper()))
        return bool(result["rows"] and int(result["rows"][0][0]) > 0)

    def fetch_rows(self, full_table_name: str, limit: int = 50) -> Dict[str, Any]:
        return self.query(f"SELECT * FROM {full_table_name} LIMIT {int(limit)}")

    def is_reachable(self) -> bool:
        try:
            self.query("SELECT 1")
            return True
        except StackError:
            return False

    def _connect(self):
        try:
            import snowflake.connector
        except ImportError as error:
            raise StackError("snowflake", "snowflake-connector-python is not installed") from error
        settings = self._settings
        try:
            return snowflake.connector.connect(
                account=settings.snowflake_account, user=settings.snowflake_user,
                password=settings.snowflake_password, role=settings.snowflake_role,
                warehouse=settings.snowflake_warehouse, database=settings.snowflake_database,
                schema=settings.snowflake_schema)
        except Exception as error:
            raise StackError("snowflake", f"could not connect: {error}") from error


# ══════════════════════════════════════════════════════════════════════════════
#  SLACK and GITHUB
# ══════════════════════════════════════════════════════════════════════════════
class SlackClient:
    """Posts notifications to a channel, or to a webhook a pipeline sink declares."""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._timeout = settings.http_timeout_seconds

    def post_to_channel(self, channel: str, text_message: str) -> bool:
        response = _request_json(
            "slack", "POST", "https://slack.com/api/chat.postMessage", self._timeout,
            headers={"Authorization": f"Bearer {self._settings.slack_bot_token}"},
            json={"channel": channel or self._settings.slack_default_channel,
                  "text": text_message}) or {}
        if not response.get("ok"):
            logger.warning("slack rejected the message: %s", response.get("error"))
        return bool(response.get("ok"))

    def post_to_webhook(self, webhook_url: str, text_message: str) -> bool:
        try:
            return requests.post(webhook_url, json={"text": text_message},
                                 timeout=self._timeout).status_code < 400
        except requests.RequestException as error:
            raise StackError("slack", f"webhook post failed: {error}") from error

    def is_reachable(self) -> bool:
        try:
            response = _request_json(
                "slack", "POST", "https://slack.com/api/auth.test", self._timeout,
                headers={"Authorization": f"Bearer {self._settings.slack_bot_token}"}) or {}
            return bool(response.get("ok"))
        except StackError:
            return False


class GitHubClient:
    """Commits approved pipeline YAML, so the repository is the record of what ran."""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._timeout = settings.http_timeout_seconds
        self._headers = {"Authorization": f"Bearer {settings.github_token}",
                         "Accept": "application/vnd.github+json"}

    def file_path(self, pipeline_name: str) -> str:
        return f"pipelines/{pipeline_name}.yaml"

    def commit_pipeline(self, pipeline_name: str, yaml_text: str, message: str) -> str:
        """Create or update pipelines/<name>.yaml. Returns the commit SHA."""
        path = self.file_path(pipeline_name)
        payload = {
            "message": message,
            "content": base64.b64encode(yaml_text.encode("utf-8")).decode("ascii"),
            "branch": self._settings.github_branch,
        }
        existing_sha = self._current_sha(path)
        if existing_sha:
            payload["sha"] = existing_sha     # GitHub needs this to overwrite a file
        response = _request_json("github", "PUT", self._contents_url(path), self._timeout,
                                 headers=self._headers, json=payload) or {}
        commit_sha = (response.get("commit") or {}).get("sha", "")
        if not commit_sha:
            raise StackError("github", "commit response contained no sha")
        return commit_sha

    def is_reachable(self) -> bool:
        return _is_reachable("https://api.github.com/rate_limit")

    def _current_sha(self, path: str) -> Optional[str]:
        try:
            response = _request_json("github", "GET", self._contents_url(path), self._timeout,
                                     headers=self._headers,
                                     params={"ref": self._settings.github_branch}) or {}
            return response.get("sha")
        except StackError:
            return None

    def _contents_url(self, path: str) -> str:
        return (f"https://api.github.com/repos/{self._settings.github_owner}/"
                f"{self._settings.github_repo}/contents/{path}")


# ── one instance of each, shared by the whole process ────────────────────────
_settings = get_settings()

postgres_schema = PostgresSchemaReader(_settings)
debezium = DebeziumClient(_settings)
kafka = KafkaClient(_settings)
flink_sql = FlinkSqlGateway(_settings)
flink_jobs = FlinkJobManager(_settings)
snowflake = SnowflakeClient(_settings)
slack = SlackClient(_settings)
github = GitHubClient(_settings)
