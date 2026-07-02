"""
Business persona: ask a question, watch it build, read the answer.

POST /api/v1/pipelines/ask          start a request (the agent runs in the background)
GET  /api/v1/pipelines/{id}/status  where it is in the flow
GET  /api/v1/pipelines/{id}/answer  the live rows, once it is deployed
GET  /api/v1/pipelines/history      this user's recent questions
GET  /api/v1/pipelines/suggestions  example questions for the landing page
"""
import asyncio
import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from backend.api_models import (
    AnswerResponse, AskRequest, AskResponse, HistoryItem, HistoryResponse,
    PipelineStatusResponse, Suggestion, SuggestionsResponse,
)
from backend.database import (
    PipelineRequest, PipelineStatus, get_request, list_requests_by_requester, session_scope,
)
from backend.dependencies import answer_reader, authoring_agent

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/pipelines", tags=["business"])

_SUGGESTIONS = [
    Suggestion(label="Sales by category",
               question="Sales by category, excluding returns, in a 10-minute window "
                        "grouped by store and region"),
    Suggestion(label="High return stores",
               question="Alert when any store has more than 20 returns in 30 minutes"),
    Suggestion(label="Low inventory",
               question="Alert me when inventory falls below 5 units in any warehouse"),
    Suggestion(label="Platinum customer spend",
               question="PLATINUM customer spend by region this hour"),
    Suggestion(label="Channel GMV",
               question="Hourly GMV by channel, ONLINE versus IN_STORE"),
]


@router.post("/ask", response_model=AskResponse)
async def ask_question(body: AskRequest) -> AskResponse:
    """Record the question and start the authoring agent in the background."""
    request_id = f"req_{uuid.uuid4().hex[:10]}"
    with session_scope() as session:
        session.add(PipelineRequest(
            id=request_id, question=body.question.strip(),
            requested_by=body.requested_by, requested_by_role=body.requested_by_role,
            status=PipelineStatus.GENERATING.value))

    asyncio.create_task(build_pipeline_for_request(request_id))
    return AskResponse(request_id=request_id, status=PipelineStatus.GENERATING.value)


@router.get("/suggestions", response_model=SuggestionsResponse)
def get_suggestions() -> SuggestionsResponse:
    """Example questions shown before the user has asked anything."""
    return SuggestionsResponse(suggestions=_SUGGESTIONS)


@router.get("/history", response_model=HistoryResponse)
def get_history(requested_by: str = "Priya", limit: int = 10) -> HistoryResponse:
    """The requester's recent questions, newest first."""
    with session_scope() as session:
        records = list_requests_by_requester(session, requested_by, limit)
        return HistoryResponse(requests=[
            HistoryItem(request_id=r.id, question=r.question, status=r.status,
                        minutes_ago=r.age_minutes(), trust_score=r.trust_score())
            for r in records])


@router.get("/{request_id}/status", response_model=PipelineStatusResponse)
def get_status(request_id: str) -> PipelineStatusResponse:
    """Status plus the six-step journey the UI draws."""
    with session_scope() as session:
        return PipelineStatusResponse.from_record(_require(session, request_id))


@router.get("/{request_id}/answer", response_model=AnswerResponse)
def get_answer(request_id: str) -> AnswerResponse:
    """The rows the deployed pipeline has written so far."""
    with session_scope() as session:
        return answer_reader.read_answer(_require(session, request_id))


@router.delete("/{request_id}")
def delete_request(request_id: str) -> dict:
    """Remove a conversation from the user's history."""
    with session_scope() as session:
        session.delete(_require(session, request_id))
    return {"deleted": request_id}


# ── background work ──────────────────────────────────────────────────────────
async def build_pipeline_for_request(request_id: str) -> None:
    """
    Run the authoring agent and store what it produced.

    Ends in pending_review when the pipeline validated, or failed when the model could
    not produce a valid one within the allowed correction attempts.
    """
    with session_scope() as session:
        record = get_request(session, request_id)
        if record is None:
            return
        question = record.question
        record.status = PipelineStatus.VALIDATING.value

    try:
        state = await authoring_agent.run(question)
    except Exception as error:
        logger.exception("authoring failed for %s", request_id)
        _mark_failed(request_id, str(error))
        return

    result = state.get("result")
    with session_scope() as session:
        record = get_request(session, request_id)
        if record is None:
            return
        record.pipeline_yaml = state.get("pipeline_yaml", "")
        record.model_name = state.get("model_name", "")
        record.generation_time_ms = state.get("generation_time_ms", 0)
        record.correction_attempts = state.get("attempts", 0)
        record.validation_checks = state.get("checks", [])
        record.validation_errors = state.get("errors", [])
        record.confidence = estimate_confidence(state.get("attempts", 0), bool(state.get("valid")))

        if state.get("valid") and result and result.spec:
            record.pipeline_name = result.spec.name
            record.flink_sql = result.built.script() if result.built else ""
            record.result_table = result.spec.snowflake_result_table()
            record.status = PipelineStatus.PENDING_REVIEW.value
        else:
            record.status = PipelineStatus.FAILED.value
            record.rejection_reason = ("The pipeline could not be validated against the "
                                       "database. Try rephrasing your question.")
        record.updated_at = datetime.now(timezone.utc)


def estimate_confidence(correction_attempts: int, valid: bool) -> float:
    """
    A confidence score from how much correcting the pipeline needed.

    The fine-tuned model does not return token probabilities, so this is a deliberate
    proxy: a pipeline that validated first time scores highest, and each correction
    round lowers it.
    """
    if not valid:
        return 0.0
    return {0: 0.97, 1: 0.92, 2: 0.87}.get(correction_attempts, 0.80)


def _mark_failed(request_id: str, reason: str) -> None:
    with session_scope() as session:
        record = get_request(session, request_id)
        if record:
            record.status = PipelineStatus.FAILED.value
            record.validation_errors = [reason[:300]]
            record.rejection_reason = "Something went wrong while building your pipeline."


def _require(session, request_id: str) -> PipelineRequest:
    record = get_request(session, request_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"request '{request_id}' not found")
    return record
