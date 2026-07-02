"""
Builds the live answer a business user sees from the rows the Flink job wrote.

The pipeline's Snowflake sink is the source of truth, so an answer is just:
read the sink table, hand back the columns and rows, and compute a few headline
numbers from them. Nothing is invented — every KPI is derived from real rows.

Agent actions go through MCP; this is a plain read on the request path, so it calls
the Snowflake client directly and stays fast.
"""
import logging
from typing import Any, List, Optional

from backend.api_models import AnswerKpi, AnswerResponse
from backend.database import PipelineRequest
from backend.services.stack_clients import SnowflakeClient, StackError

logger = logging.getLogger(__name__)

_MAX_ROWS = 50
_MAX_KPI_COLUMNS = 3


class AnswerReader:
    """Reads a pipeline's results and shapes them for the answer card."""

    def __init__(self, snowflake: SnowflakeClient):
        self._snowflake = snowflake

    def read_answer(self, record: PipelineRequest) -> AnswerResponse:
        answer = AnswerResponse(
            request_id=record.id,
            title=_title_for(record),
            subtitle="",
            result_table=record.result_table,
            reviewed_by=record.reviewed_by,
            trust_score=record.trust_score(),
            is_live=record.status == "live",
        )

        if not record.result_table:
            answer.subtitle = "This pipeline sends alerts rather than writing a results table."
            answer.message = ("Results are delivered straight to Slack, email or Redis by this "
                              "pipeline, so there is no table to show here.")
            return answer

        try:
            result = self._snowflake.fetch_rows(record.result_table, _MAX_ROWS)
        except StackError as error:
            logger.warning("could not read %s: %s", record.result_table, error)
            answer.subtitle = f"Reading {record.result_table}"
            answer.message = f"The results table could not be read right now ({error.message})."
            return answer

        answer.columns = result["columns"]
        answer.rows = result["rows"]
        answer.row_count = len(result["rows"])
        answer.kpis = build_kpis(result["columns"], result["rows"])
        answer.subtitle = (f"Live from {record.result_table} · {answer.row_count} row"
                           f"{'s' if answer.row_count != 1 else ''}")
        if not answer.rows:
            answer.message = ("The pipeline is running but has not produced rows yet — the first "
                              "window has not closed.")
        return answer


def build_kpis(columns: List[str], rows: List[List[Any]]) -> List[AnswerKpi]:
    """
    Headline numbers straight from the result rows.

    Row count first, then the total of each numeric column (up to three), which is what
    a windowed aggregation is almost always counting or summing.
    """
    kpis = [AnswerKpi(label="Rows in window", value=f"{len(rows):,}")]
    if not rows:
        return kpis

    for index, column_name in enumerate(columns):
        if len(kpis) > _MAX_KPI_COLUMNS:
            break
        values = [row[index] for row in rows if index < len(row)]
        numbers = [float(value) for value in values if isinstance(value, (int, float))]
        if len(numbers) != len(values) or not numbers:
            continue
        kpis.append(AnswerKpi(label=f"Total {column_name.lower().replace('_', ' ')}",
                              value=format_number(sum(numbers))))
    return kpis


def format_number(value: float) -> str:
    """Compact Indian-style formatting: 1.2L / 12.3K / 1,234."""
    if abs(value) >= 100_000:
        return f"{value / 100_000:.1f}L"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:,.0f}" if float(value).is_integer() else f"{value:,.2f}"


def _title_for(record: PipelineRequest) -> str:
    name = (record.pipeline_name or "").replace("_", " ").strip()
    return name.title() if name else (record.question[:70] or "Your answer")
