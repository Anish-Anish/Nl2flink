"""
Live monitoring and platform health.

GET /api/v1/monitor/metrics   throughput of the running Flink jobs
GET /api/v1/monitor/events    the newest change events on the CDC topic
GET /api/v1/platform/status   which servers the backend can reach
GET /api/v1/platform/mcp      which MCP servers the agents can reach
"""
import logging
from datetime import datetime, timezone

from fastapi import APIRouter

from backend.api_models import (
    ChangeEvent, EventFeedResponse, McpStatusResponse, MonitorMetricsResponse,
    PipelineThroughput, PlatformStatusResponse, ServiceHealth,
)
from backend.database import PipelineStatus, count_live_pipelines, list_requests_by_status, session_scope
from backend.dependencies import (
    debezium, flink_jobs, flink_sql, github, kafka, model_generator, postgres_schema,
    settings, slack, snowflake, toolbox,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["monitoring"])


@router.get("/monitor/metrics", response_model=MonitorMetricsResponse)
def get_monitor_metrics() -> MonitorMetricsResponse:
    """Per-pipeline throughput, read live from the Flink JobManager."""
    with session_scope() as session:
        live_records = list_requests_by_status(session, PipelineStatus.LIVE)
        pipelines = [(record.pipeline_name, record.flink_job_id) for record in live_records]

    throughputs = []
    for name, job_id in pipelines:
        rate = 0.0
        if job_id:
            try:
                rate = flink_jobs.records_per_second(job_id)
            except Exception as error:
                logger.warning("no metrics for %s: %s", job_id, error)
        throughputs.append(PipelineThroughput(pipeline_name=name, flink_job_id=job_id,
                                              records_per_second=rate))

    overview = {}
    try:
        overview = flink_jobs.cluster_overview()
    except Exception as error:
        logger.warning("flink overview unavailable: %s", error)

    return MonitorMetricsResponse(
        active_pipelines=len(throughputs),
        total_records_per_second=round(sum(t.records_per_second for t in throughputs), 2),
        task_managers=overview.get("taskmanagers", 0),
        slots_available=overview.get("slots-available", 0),
        pipelines=throughputs)


@router.get("/monitor/events", response_model=EventFeedResponse)
def get_event_feed(table: str = "orders", limit: int = 20) -> EventFeedResponse:
    """Tail the CDC topic for one table and show the newest changes."""
    from backend.services.stack_clients import cdc_topic_name

    topic = cdc_topic_name("megastore.retail_events", settings.postgres_schema, table)
    try:
        raw_events = kafka.read_recent_events(topic, limit)
    except Exception as error:
        logger.warning("cannot read %s: %s", topic, error)
        raw_events = []

    events = [ChangeEvent(
        timestamp=_format_time(event["timestamp_ms"]),
        table=event.get("table") or table,
        operation=event.get("operation"),
        summary=_summarise_row(event.get("row") or {})) for event in raw_events]
    return EventFeedResponse(topic=topic, events=events)


@router.get("/platform/status", response_model=PlatformStatusResponse)
def get_platform_status() -> PlatformStatusResponse:
    """One row per server, so the UI can show what is up before anything is asked."""
    services = [
        ServiceHealth(name="Postgres", connected=_safe(postgres_schema.is_reachable),
                      detail="source database"),
        ServiceHealth(name="Debezium", connected=_safe(debezium.is_reachable),
                      detail="change data capture"),
        ServiceHealth(name="Kafka", connected=_safe(kafka.is_reachable),
                      detail="change event bus"),
        ServiceHealth(name="Flink SQL Gateway", connected=_safe(flink_sql.is_reachable),
                      detail="runs pipeline SQL"),
        ServiceHealth(name="Flink JobManager", connected=_safe(flink_jobs.is_reachable),
                      detail="job state and metrics"),
        ServiceHealth(name="Snowflake", connected=_safe(snowflake.is_reachable),
                      detail="results warehouse"),
        ServiceHealth(name="Slack", connected=_safe(slack.is_reachable),
                      detail="notifications"),
        ServiceHealth(name="GitHub", connected=_safe(github.is_reachable),
                      detail="pipeline repository"),
        ServiceHealth(name="Fine-tuned model", connected=_safe(model_generator.is_healthy),
                      detail=settings.model_name),
    ]
    with session_scope() as session:
        active = count_live_pipelines(session)
    return PlatformStatusResponse(services=services, active_pipelines=active)


@router.get("/platform/mcp", response_model=McpStatusResponse)
async def get_mcp_status() -> McpStatusResponse:
    """Which MCP servers the agents can reach, and how many tools each serves."""
    return McpStatusResponse(tools=await toolbox.status())


# ── helpers ──────────────────────────────────────────────────────────────────
def _safe(check) -> bool:
    try:
        return bool(check())
    except Exception:
        return False


def _format_time(timestamp_ms: int) -> str:
    try:
        return datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc).strftime("%H:%M:%S")
    except (TypeError, ValueError, OSError):
        return ""


def _summarise_row(row: dict, max_fields: int = 4) -> str:
    """A short 'key=value · key=value' line for the event feed."""
    parts = [f"{key}={value}" for key, value in list(row.items())[:max_fields]
             if value is not None]
    return " · ".join(parts)
