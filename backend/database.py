"""
Studio database: one table holding every pipeline request and what happened to it.

A request is created when a business user asks a question and is updated in place as
it moves through the flow:

    generating -> validating -> pending_review -> approved -> deploying -> live
                                      |
                                      +-> rejected / failed
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from enum import Enum
from typing import Iterator, List, Optional

from sqlalchemy import JSON, Column, DateTime, Float, Integer, String, Text, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from backend.config import get_settings

Base = declarative_base()
_engine = None
_SessionFactory = None


def _get_session_factory():
    """Build the engine on first use so importing this module needs no database."""
    global _engine, _SessionFactory
    if _SessionFactory is None:
        _engine = create_engine(get_settings().studio_db_url, pool_pre_ping=True)
        _SessionFactory = sessionmaker(bind=_engine, expire_on_commit=False)
    return _SessionFactory


class PipelineStatus(str, Enum):
    GENERATING = "generating"          # fine-tuned model is writing the pipeline
    VALIDATING = "validating"          # checking it against the live schema
    PENDING_REVIEW = "pending_review"  # waiting for the data engineer
    APPROVED = "approved"              # approved, deployment about to start
    DEPLOYING = "deploying"            # commit -> CDC -> Flink -> sink -> notify
    LIVE = "live"                      # Flink job running
    REJECTED = "rejected"              # sent back to the requester
    FAILED = "failed"                  # generation, validation or deployment gave up


#: Statuses where work is still in flight, so the UI keeps polling.
ACTIVE_STATUSES = [PipelineStatus.GENERATING, PipelineStatus.VALIDATING,
                   PipelineStatus.PENDING_REVIEW, PipelineStatus.APPROVED,
                   PipelineStatus.DEPLOYING]


class PipelineRequest(Base):
    """One business question and the pipeline generated from it."""

    __tablename__ = "pipeline_requests"

    id = Column(String(40), primary_key=True)
    question = Column(Text, nullable=False)
    requested_by = Column(String(80), nullable=False)
    requested_by_role = Column(String(80), default="")
    status = Column(String(30), default=PipelineStatus.GENERATING.value, index=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # ── what the fine-tuned model produced ───────────────────────────────────
    pipeline_name = Column(String(200), default="")
    pipeline_yaml = Column(Text, default="")
    model_name = Column(String(120), default="")
    generation_time_ms = Column(Integer, default=0)
    correction_attempts = Column(Integer, default=0)
    confidence = Column(Float, default=0.0)

    # ── validation ───────────────────────────────────────────────────────────
    validation_checks = Column(JSON, default=list)   # ["All 3 source tables exist", ...]
    validation_errors = Column(JSON, default=list)   # populated only when it failed

    # ── review ───────────────────────────────────────────────────────────────
    reviewed_by = Column(String(80))
    rejection_reason = Column(Text)

    # ── deployment ───────────────────────────────────────────────────────────
    flink_job_id = Column(String(64))
    flink_sql = Column(Text, default="")
    github_commit_sha = Column(String(64))
    debezium_connector = Column(String(160))
    result_table = Column(String(200))               # Snowflake table answers read from
    deployed_at = Column(DateTime(timezone=True))
    deploy_steps = Column(JSON, default=list)        # [{name, status, detail}, ...]

    def age_minutes(self) -> int:
        created = self.created_at or datetime.now(timezone.utc)
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        return int((datetime.now(timezone.utc) - created).total_seconds() // 60)

    def trust_score(self) -> float:
        """Model confidence as the 0-10 score the UI shows."""
        return round((self.confidence or 0.0) * 10, 1)


def initialise_database() -> None:
    """Create the table if it is not there yet. Called once on API startup."""
    _get_session_factory()
    Base.metadata.create_all(_engine)


@contextmanager
def session_scope() -> Iterator[Session]:
    """Transactional session: commits on success, rolls back on error."""
    session = _get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ── queries used by the routers ──────────────────────────────────────────────
def get_request(session: Session, request_id: str) -> Optional[PipelineRequest]:
    return session.get(PipelineRequest, request_id)


def list_requests_by_requester(session: Session, requester: str, limit: int = 20
                               ) -> List[PipelineRequest]:
    return (session.query(PipelineRequest)
            .filter(PipelineRequest.requested_by == requester)
            .order_by(PipelineRequest.created_at.desc())
            .limit(limit).all())


def list_requests_by_status(session: Session, status: PipelineStatus) -> List[PipelineRequest]:
    return (session.query(PipelineRequest)
            .filter(PipelineRequest.status == status.value)
            .order_by(PipelineRequest.created_at.desc()).all())


def count_live_pipelines(session: Session) -> int:
    return (session.query(PipelineRequest)
            .filter(PipelineRequest.status == PipelineStatus.LIVE.value).count())
