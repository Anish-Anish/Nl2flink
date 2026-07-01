"""
Request and response models for the HTTP API.

These are plain data — no HTML, no CSS class names. The Streamlit app turns them into
markup today; a React app can consume exactly the same JSON tomorrow with no backend
change.
"""
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from backend.database import PipelineRequest, PipelineStatus

# ── auth ─────────────────────────────────────────────────────────────────────
class LoginRequest(BaseModel):
    persona: Literal["business", "engineer"]


class UserSession(BaseModel):
    user_id: str
    name: str
    role: str
    persona: str
    token: str


# ── the progress trail the business user sees ────────────────────────────────
JOURNEY_LABELS = ["Question received", "AI built pipeline", "Pending with engineer",
                  "Engineer approved", "Flink deployed", "Live answer below"]

_JOURNEY_STATES = {
    PipelineStatus.GENERATING:     ["done", "active", "waiting", "waiting", "waiting", "waiting"],
    PipelineStatus.VALIDATING:     ["done", "active", "waiting", "waiting", "waiting", "waiting"],
    PipelineStatus.PENDING_REVIEW: ["done", "done", "active", "waiting", "waiting", "waiting"],
    PipelineStatus.APPROVED:       ["done", "done", "done", "done", "active", "waiting"],
    PipelineStatus.DEPLOYING:      ["done", "done", "done", "done", "active", "waiting"],
    PipelineStatus.LIVE:           ["done", "done", "done", "done", "done", "done"],
    PipelineStatus.REJECTED:       ["done", "done", "rejected", "waiting", "waiting", "waiting"],
    PipelineStatus.FAILED:         ["done", "rejected", "waiting", "waiting", "waiting", "waiting"],
}


class JourneyStep(BaseModel):
    label: str
    state: Literal["done", "active", "waiting", "rejected"]


def build_journey(status: str) -> List[JourneyStep]:
    states = _JOURNEY_STATES.get(PipelineStatus(status), _JOURNEY_STATES[PipelineStatus.GENERATING])
    return [JourneyStep(label=label, state=state) for label, state in zip(JOURNEY_LABELS, states)]


# ── business persona ─────────────────────────────────────────────────────────
class AskRequest(BaseModel):
    question: str = Field(min_length=3)
    requested_by: str = "Priya"
    requested_by_role: str = "CEO"


class AskResponse(BaseModel):
    request_id: str
    status: str


class DeployStep(BaseModel):
    """One line of the deployment log (also streamed live over SSE on approval)."""

    name: str
    label: str
    status: Literal["waiting", "running", "done", "failed"]
    detail: str = ""
    agent: str = ""


class PipelineStatusResponse(BaseModel):
    request_id: str
    question: str
    status: str
    journey_steps: List[JourneyStep]
    correction_attempts: int = 0
    trust_score: float = 0.0
    reviewed_by: Optional[str] = None
    flink_job_id: Optional[str] = None
    minutes_ago: int = 0
    rejection_reason: Optional[str] = None
    deploy_steps: List[DeployStep] = Field(default_factory=list)

    @classmethod
    def from_record(cls, record: PipelineRequest) -> "PipelineStatusResponse":
        return cls(
            request_id=record.id,
            question=record.question,
            status=record.status,
            journey_steps=build_journey(record.status),
            correction_attempts=record.correction_attempts or 0,
            trust_score=record.trust_score(),
            reviewed_by=record.reviewed_by,
            flink_job_id=record.flink_job_id,
            minutes_ago=record.age_minutes(),
            rejection_reason=record.rejection_reason,
            deploy_steps=[DeployStep(**step) for step in (record.deploy_steps or [])],
        )


class HistoryItem(BaseModel):
    request_id: str
    question: str
    status: str
    minutes_ago: int
    trust_score: float


class HistoryResponse(BaseModel):
    requests: List[HistoryItem]


class Suggestion(BaseModel):
    label: str
    question: str


class SuggestionsResponse(BaseModel):
    suggestions: List[Suggestion]


class AnswerKpi(BaseModel):
    """A headline number computed from the pipeline's result rows."""

    label: str
    value: str
    delta: Optional[str] = None
    direction: Literal["up", "down", "flat"] = "flat"


class AnswerResponse(BaseModel):
    """A live answer: the rows the Flink job wrote, plus headline numbers."""

    request_id: str
    title: str
    subtitle: str
    result_table: Optional[str] = None
    columns: List[str] = Field(default_factory=list)
    rows: List[List[Any]] = Field(default_factory=list)
    kpis: List[AnswerKpi] = Field(default_factory=list)
    row_count: int = 0
    reviewed_by: Optional[str] = None
    trust_score: float = 0.0
    is_live: bool = True
    message: Optional[str] = Field(default=None, description="Set when there are no rows yet")


# ── engineer persona ─────────────────────────────────────────────────────────
class ReviewQueueItem(BaseModel):
    request_id: str
    pipeline_name: str
    question: str
    requested_by: str
    requested_by_role: str
    minutes_ago: int
    confidence: float
    trust_score: float
    correction_attempts: int


class ReviewQueueResponse(BaseModel):
    total_pending: int
    queue: List[ReviewQueueItem]


class ModelInfo(BaseModel):
    model_name: str
    generation_time_ms: int
    confidence: float
    correction_attempts: int


class ReviewDetailResponse(BaseModel):
    """Everything the engineer needs to judge one generated pipeline."""

    request_id: str
    pipeline_name: str
    question: str
    pipeline_yaml: str
    flink_sql: str = ""
    source_tables: List[str] = Field(default_factory=list)
    sink_targets: List[str] = Field(default_factory=list)
    validation_checks: List[str] = Field(default_factory=list)
    validation_errors: List[str] = Field(default_factory=list)
    trust_score: float = 0.0
    model_info: ModelInfo


class EditPipelineRequest(BaseModel):
    pipeline_yaml: str
    edited_by: str = "Arjun"


class EditPipelineResponse(BaseModel):
    valid: bool
    validation_checks: List[str] = Field(default_factory=list)
    validation_errors: List[str] = Field(default_factory=list)
    flink_sql: str = ""


class ApproveRequest(BaseModel):
    approved_by: str = "Arjun"


class RejectRequest(BaseModel):
    rejected_by: str = "Arjun"
    reason: str = "Needs more detail on the time window."


class DeployedPipeline(BaseModel):
    request_id: str
    pipeline_name: str
    flink_job_id: Optional[str] = None
    state: str = "RUNNING"
    records_per_second: float = 0.0
    requested_by: str
    reviewed_by: Optional[str] = None
    minutes_ago: int = 0
    result_table: Optional[str] = None
    github_commit_sha: Optional[str] = None


class DeployedResponse(BaseModel):
    deployed: List[DeployedPipeline]


# ── monitoring ───────────────────────────────────────────────────────────────
class PipelineThroughput(BaseModel):
    pipeline_name: str
    flink_job_id: Optional[str] = None
    records_per_second: float = 0.0


class MonitorMetricsResponse(BaseModel):
    active_pipelines: int = 0
    total_records_per_second: float = 0.0
    task_managers: int = 0
    slots_available: int = 0
    pipelines: List[PipelineThroughput] = Field(default_factory=list)


class ChangeEvent(BaseModel):
    """One Debezium change event, as shown in the live event feed."""

    timestamp: str
    table: Optional[str] = None
    operation: Optional[str] = None
    summary: str


class EventFeedResponse(BaseModel):
    topic: Optional[str] = None
    events: List[ChangeEvent] = Field(default_factory=list)


class ServiceHealth(BaseModel):
    name: str
    connected: bool
    latency_ms: Optional[int] = None
    detail: str = ""


class PlatformStatusResponse(BaseModel):
    services: List[ServiceHealth] = Field(default_factory=list)
    active_pipelines: int = 0

    @property
    def all_connected(self) -> bool:
        return all(service.connected for service in self.services)


class McpToolStatus(BaseModel):
    name: str
    status: Literal["connected", "connecting", "unavailable"]
    tool_count: int = 0
    description: str = ""


class McpStatusResponse(BaseModel):
    tools: List[McpToolStatus] = Field(default_factory=list)


class ErrorResponse(BaseModel):
    error: str
    detail: Optional[Dict[str, Any]] = None
