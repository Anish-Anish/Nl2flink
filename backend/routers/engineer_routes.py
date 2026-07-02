"""
Engineer persona: review what the model wrote, edit it, approve or reject it.

GET  /api/v1/review/queue          pipelines waiting for approval
GET  /api/v1/review/{id}           one pipeline: YAML, Flink SQL, validation
PUT  /api/v1/review/{id}/pipeline  save an edited YAML and re-validate it
POST /api/v1/review/{id}/approve   deploy it, streaming each step back (SSE)
POST /api/v1/review/{id}/reject    send it back to the requester
GET  /api/v1/review/deployed       pipelines currently running on Flink
"""
import json
import logging
from datetime import datetime, timezone
from typing import AsyncIterator

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from backend.api_models import (
    ApproveRequest, DeployedPipeline, DeployedResponse, EditPipelineRequest,
    EditPipelineResponse, ModelInfo, RejectRequest, ReviewDetailResponse,
    ReviewQueueItem, ReviewQueueResponse,
)
from backend.database import (
    PipelineRequest, PipelineStatus, get_request, list_requests_by_status, session_scope,
)
from backend.dependencies import deployment_agent, flink_jobs, slack, validator
from backend.pipeline_spec import parse_pipeline_yaml

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/review", tags=["engineer"])


@router.get("/queue", response_model=ReviewQueueResponse)
def get_review_queue() -> ReviewQueueResponse:
    """Everything waiting for the engineer, newest first."""
    with session_scope() as session:
        records = list_requests_by_status(session, PipelineStatus.PENDING_REVIEW)
        items = [ReviewQueueItem(
            request_id=r.id, pipeline_name=r.pipeline_name or "generated_pipeline",
            question=r.question, requested_by=r.requested_by,
            requested_by_role=r.requested_by_role or "", minutes_ago=r.age_minutes(),
            confidence=r.confidence or 0.0, trust_score=r.trust_score(),
            correction_attempts=r.correction_attempts or 0) for r in records]
        return ReviewQueueResponse(total_pending=len(items), queue=items)


@router.get("/deployed", response_model=DeployedResponse)
def get_deployed_pipelines() -> DeployedResponse:
    """Running pipelines, with live throughput read from the Flink JobManager."""
    with session_scope() as session:
        records = list_requests_by_status(session, PipelineStatus.LIVE)
        deployed = []
        for record in records:
            deployed.append(DeployedPipeline(
                request_id=record.id, pipeline_name=record.pipeline_name,
                flink_job_id=record.flink_job_id,
                state=_job_state(record.flink_job_id),
                records_per_second=_job_throughput(record.flink_job_id),
                requested_by=record.requested_by, reviewed_by=record.reviewed_by,
                minutes_ago=record.age_minutes(), result_table=record.result_table,
                github_commit_sha=(record.github_commit_sha or "")[:7] or None))
        return DeployedResponse(deployed=deployed)


@router.get("/{request_id}", response_model=ReviewDetailResponse)
def get_review_detail(request_id: str) -> ReviewDetailResponse:
    """Everything the engineer needs to judge one generated pipeline."""
    with session_scope() as session:
        record = _require(session, request_id)
        source_tables, sink_targets = _describe_pipeline(record.pipeline_yaml)
        return ReviewDetailResponse(
            request_id=record.id,
            pipeline_name=record.pipeline_name or "generated_pipeline",
            question=record.question,
            pipeline_yaml=record.pipeline_yaml or "",
            flink_sql=record.flink_sql or "",
            source_tables=source_tables,
            sink_targets=sink_targets,
            validation_checks=record.validation_checks or [],
            validation_errors=record.validation_errors or [],
            trust_score=record.trust_score(),
            model_info=ModelInfo(
                model_name=record.model_name or "",
                generation_time_ms=record.generation_time_ms or 0,
                confidence=record.confidence or 0.0,
                correction_attempts=record.correction_attempts or 0))


@router.put("/{request_id}/pipeline", response_model=EditPipelineResponse)
def save_edited_pipeline(request_id: str, body: EditPipelineRequest) -> EditPipelineResponse:
    """Re-validate an engineer's edit and keep it only if it is still valid."""
    result = validator.validate_yaml(body.pipeline_yaml)
    if not result.valid:
        return EditPipelineResponse(valid=False, validation_checks=result.checks,
                                    validation_errors=result.errors)
    with session_scope() as session:
        record = _require(session, request_id)
        record.pipeline_yaml = body.pipeline_yaml
        record.pipeline_name = result.spec.name
        record.flink_sql = result.built.script() if result.built else ""
        record.result_table = result.spec.snowflake_result_table()
        record.validation_checks = result.checks
        record.validation_errors = []
        record.reviewed_by = body.edited_by
    return EditPipelineResponse(valid=True, validation_checks=result.checks,
                                flink_sql=result.built.script() if result.built else "")


@router.post("/{request_id}/reject")
def reject_pipeline(request_id: str, body: RejectRequest) -> dict:
    """Send the request back to the requester, and tell them on Slack."""
    with session_scope() as session:
        record = _require(session, request_id)
        record.status = PipelineStatus.REJECTED.value
        record.reviewed_by = body.rejected_by
        record.rejection_reason = body.reason
        question = record.question

    _notify(f"Your request \"{question}\" needs a change before it can go live: {body.reason}")
    return {"request_id": request_id, "status": PipelineStatus.REJECTED.value}


@router.post("/{request_id}/approve")
async def approve_and_deploy(request_id: str, body: ApproveRequest) -> StreamingResponse:
    """
    Approve a pipeline and deploy it, streaming each step as server-sent events.

    Every event is one JSON object: {name, label, status, detail, agent}.
    """
    with session_scope() as session:
        record = _require(session, request_id)
        if record.status != PipelineStatus.PENDING_REVIEW.value:
            raise HTTPException(status_code=409,
                                detail=f"request is '{record.status}', not pending review")
        record.status = PipelineStatus.APPROVED.value
        record.reviewed_by = body.approved_by
        pipeline_yaml = record.pipeline_yaml
        question, requested_by = record.question, record.requested_by

    result = validator.validate_yaml(pipeline_yaml)
    if not result.valid or result.built is None:
        _set_status(request_id, PipelineStatus.FAILED, errors=result.errors)
        raise HTTPException(status_code=422,
                            detail={"error": "pipeline no longer validates",
                                    "issues": result.errors})

    return StreamingResponse(
        _stream_deployment(request_id, result, pipeline_yaml, question,
                           requested_by, body.approved_by),
        media_type="text/event-stream")


async def _stream_deployment(request_id: str, result, pipeline_yaml: str, question: str,
                             requested_by: str, approved_by: str) -> AsyncIterator[str]:
    """Run the deployment agent and emit one SSE line per completed step."""
    _set_status(request_id, PipelineStatus.DEPLOYING)
    completed, failed = [], False

    async for step in deployment_agent.deploy(
            request_id=request_id, spec=result.spec, pipeline_yaml=pipeline_yaml,
            built=result.built, question=question, requested_by=requested_by,
            approved_by=approved_by):
        completed.append(step)
        failed = failed or step.status == "failed"
        yield f"data: {step.model_dump_json()}\n\n"

    _finish_deployment(request_id, completed, failed)
    summary = {"name": "finished", "label": "Pipeline is live — data is flowing"
                                            if not failed else "Deployment failed",
               "status": "done" if not failed else "failed",
               "detail": "", "agent": "deploy_agent"}
    yield f"data: {json.dumps(summary)}\n\n"


def _finish_deployment(request_id: str, steps, failed: bool) -> None:
    with session_scope() as session:
        record = get_request(session, request_id)
        if record is None:
            return
        record.deploy_steps = [step.model_dump() for step in steps]
        record.status = (PipelineStatus.FAILED if failed else PipelineStatus.LIVE).value
        if not failed:
            record.deployed_at = datetime.now(timezone.utc)
        for step in steps:
            if step.name == "commit_to_github" and step.detail:
                record.github_commit_sha = step.detail.replace("commit ", "").strip()
            if step.name == "submit_to_flink" and step.detail:
                record.flink_job_id = step.detail.replace("job ", "").strip()
            if step.name == "start_change_capture" and step.detail:
                record.debezium_connector = step.detail.split(" · ")[0]


# ── helpers ──────────────────────────────────────────────────────────────────
def _describe_pipeline(pipeline_yaml: str):
    """Source tables and sink targets, for the summary line on the review card."""
    try:
        spec = parse_pipeline_yaml(pipeline_yaml or "")
    except Exception:
        return [], []
    targets = [getattr(sink, "full_table_name", None) or getattr(sink, "channel", None)
               or getattr(sink, "table", None) or sink.type for sink in spec.sink]
    return spec.source.table_names(), [str(target) for target in targets]


def _job_state(job_id) -> str:
    if not job_id:
        return "UNKNOWN"
    try:
        return flink_jobs.job_state(job_id)
    except Exception:
        return "UNREACHABLE"


def _job_throughput(job_id) -> float:
    if not job_id:
        return 0.0
    try:
        return flink_jobs.records_per_second(job_id)
    except Exception:
        return 0.0


def _notify(message: str) -> None:
    try:
        slack.post_to_channel("", message)
    except Exception as error:
        logger.warning("could not send slack notification: %s", error)


def _set_status(request_id: str, status: PipelineStatus, errors=None) -> None:
    with session_scope() as session:
        record = get_request(session, request_id)
        if record:
            record.status = status.value
            if errors is not None:
                record.validation_errors = errors


def _require(session, request_id: str) -> PipelineRequest:
    record = get_request(session, request_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"request '{request_id}' not found")
    return record
