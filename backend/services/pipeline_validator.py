"""
Checks a generated pipeline before a human ever sees it.

Four things are verified, in order — later checks only run when earlier ones passed,
because checking columns on a pipeline that is not valid YAML just produces noise:

    1. the YAML parses and matches the pipeline DSL
    2. every table it captures exists in Postgres, with the primary key it declares
    3. every step and column it references really exists
    4. it compiles to Flink SQL

Failures come back as short sentences. Those sentences are what the agent feeds to the
fine-tuned model when it asks it to correct itself, and what the engineer sees on the
review card.
"""
import logging
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from backend.config import Settings
from backend.pipeline_spec import (
    AllColumnsFrom, AntiJoinStep, DeduplicationStep, FilterStep, PipelineSpec,
    PostgresWriteSink, SessionWindowStep, SnowflakeSink, StreamJoinStep, TopNStep,
    WindowAggregationStep, parse_pipeline_yaml,
)
from backend.services.flink_sql_builder import BuiltPipeline, FlinkSqlBuilder
from backend.services.stack_clients import PostgresSchemaReader, PostgresTable

logger = logging.getLogger(__name__)


class ValidationResult(BaseModel):
    """What validation found: green checks for the engineer, errors for the model."""

    valid: bool = False
    checks: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    spec: Optional[PipelineSpec] = None
    built: Optional[BuiltPipeline] = None


class PipelineValidator:
    """Validates pipeline YAML against the live database and the Flink SQL builder."""

    def __init__(self, settings: Settings, schema_reader: PostgresSchemaReader):
        self._builder = FlinkSqlBuilder(settings)
        self._schema_reader = schema_reader

    def describe_tables(self, table_names: List[str]) -> Dict[str, PostgresTable]:
        """Columns of the given tables — used by the agent when it builds a retry prompt."""
        return self._schema_reader.describe_tables(table_names)

    def validate_yaml(self, yaml_text: str) -> ValidationResult:
        try:
            spec = parse_pipeline_yaml(yaml_text)
        except Exception as error:
            return ValidationResult(valid=False, errors=[f"YAML: {error}"])
        result = self.validate_spec(spec)
        result.checks.insert(0, "YAML syntax valid")
        return result

    def validate_spec(self, spec: PipelineSpec) -> ValidationResult:
        checks: List[str] = []
        tables = self._schema_reader.describe_tables(spec.source.table_names())

        errors = self._check_tables(spec, tables, checks)
        if errors:
            return ValidationResult(valid=False, checks=checks, errors=errors, spec=spec)

        errors = self._check_step_graph(spec, checks)
        if errors:
            return ValidationResult(valid=False, checks=checks, errors=errors, spec=spec)

        errors = self._check_columns(spec, tables, checks) + self._check_sink_rules(spec, checks)
        if errors:
            return ValidationResult(valid=False, checks=checks, errors=errors, spec=spec)

        try:
            built = self._builder.build(spec, tables)
        except Exception as error:
            return ValidationResult(valid=False, checks=checks,
                                    errors=[f"cannot compile to Flink SQL: {error}"], spec=spec)

        checks.append("Compiles to Flink SQL")
        checks.append(f"{len(spec.sink)} sink{'s' if len(spec.sink) != 1 else ''} connected")
        return ValidationResult(valid=True, checks=checks, spec=spec, built=built)

    # ── individual checks ────────────────────────────────────────────────────
    def _check_tables(self, spec: PipelineSpec, tables: Dict[str, PostgresTable],
                      checks: List[str]) -> List[str]:
        errors = [f"table '{name}' does not exist in the database"
                  for name in spec.source.table_names() if name not in tables]
        for captured in spec.source.tables:
            table = tables.get(captured.table)
            if table and not table.has_column(captured.primary_key):
                errors.append(f"primary key '{captured.primary_key}' is not a column of "
                              f"'{captured.table}'")
        if not errors:
            count = len(spec.source.tables)
            checks.append(f"All {count} source table{'s' if count != 1 else ''} exist in schema")
            checks.append("Primary keys valid")
        return errors

    def _check_step_graph(self, spec: PipelineSpec, checks: List[str]) -> List[str]:
        """A step may only read from a source table or a step defined before it."""
        errors: List[str] = []
        known = set(spec.source.table_names())
        for step in spec.transforms:
            for upstream in spec.upstream_of(step):
                if upstream not in known:
                    errors.append(f"step '{step.id}' reads from '{upstream}', which is not a "
                                  f"source table and is not defined before it")
            if step.id in known:
                errors.append(f"duplicate step id '{step.id}'")
            known.add(step.id)
        for sink in spec.sink:
            if sink.source not in known:
                errors.append(f"sink '{sink.id}' reads from '{sink.source}', which is not defined")
        if not errors:
            checks.append("Step references resolve, no circular joins")
        return errors

    def _check_columns(self, spec: PipelineSpec, tables: Dict[str, PostgresTable],
                       checks: List[str]) -> List[str]:
        """Every column named by a filter, join, grouping or aggregation must exist."""
        errors: List[str] = []
        columns_of = _column_index(spec, tables)

        def require(step_id: str, column: str, what: str, step_name: str) -> None:
            available = columns_of.get(step_id)
            if available and column not in available:
                errors.append(f"{what} '{column}' does not exist on '{step_id}' "
                              f"(used by step '{step_name}')")

        for step in spec.transforms:
            if isinstance(step, FilterStep):
                for condition in step.conditions:
                    if not condition.expression:
                        require(step.source, condition.field, "filter field", step.id)
            elif isinstance(step, (StreamJoinStep, AntiJoinStep)):
                require(step.left, step.join_key.left, "left join key", step.id)
                require(step.right, step.join_key.right, "right join key", step.id)
                for field in getattr(step, "output_fields", []) or []:
                    if isinstance(field, AllColumnsFrom):
                        continue
                    relation, separator, column = str(field).rpartition(".")
                    if separator:
                        require(relation, column, "join output field", step.id)
            elif isinstance(step, WindowAggregationStep):
                for group_column in step.group_by:
                    require(step.source, group_column, "group_by field", step.id)
                for aggregation in step.aggregations:
                    require(step.source, aggregation.field, "aggregation field", step.id)
                if step.window.watermark_field:
                    require(step.source, step.window.watermark_field, "watermark field", step.id)
            elif isinstance(step, SessionWindowStep):
                for key in step.session_key:
                    require(step.source, key, "session key", step.id)
            elif isinstance(step, DeduplicationStep):
                for key in step.key:
                    require(step.source, key, "deduplication key", step.id)
            elif isinstance(step, TopNStep):
                require(step.source, step.order_by, "top_n order column", step.id)

        if not errors:
            checks.append("All referenced fields exist on their tables")
        return errors

    def _check_sink_rules(self, spec: PipelineSpec, checks: List[str]) -> List[str]:
        """Flink needs a key to know which row an upsert replaces."""
        errors: List[str] = []
        for sink in spec.sink:
            upsert = getattr(sink, "write_mode", None) and sink.write_mode.value in ("UPSERT", "UPDATE")
            if upsert and isinstance(sink, SnowflakeSink) and not sink.upsert_key:
                errors.append(f"sink '{sink.id}' writes in {sink.write_mode.value} mode but "
                              f"declares no upsert_key")
            if upsert and isinstance(sink, PostgresWriteSink) and not sink.match_key:
                errors.append(f"sink '{sink.id}' writes in {sink.write_mode.value} mode but "
                              f"declares no match_key")
        if not errors:
            checks.append("Sink write modes valid")
        return errors


def _column_index(spec: PipelineSpec, tables: Dict[str, PostgresTable]) -> Dict[str, set]:
    """Column names available on each relation, following the transform chain."""
    index: Dict[str, set] = {name: set(table.column_names()) for name, table in tables.items()}
    for step in spec.transforms:
        upstream = index.get(getattr(step, "source", ""), set())
        if isinstance(step, (FilterStep, DeduplicationStep, TopNStep)):
            index[step.id] = set(upstream)
        elif hasattr(step, "computed_fields"):
            index[step.id] = upstream | {f.alias for f in step.computed_fields}
        elif isinstance(step, AntiJoinStep):
            index[step.id] = set(index.get(step.left, set()))
        elif isinstance(step, StreamJoinStep):
            index[step.id] = index.get(step.left, set()) | index.get(step.right, set())
        elif isinstance(step, WindowAggregationStep):
            index[step.id] = ({"window_start", "window_end"} | set(step.group_by)
                              | {a.alias for a in step.aggregations})
        elif isinstance(step, SessionWindowStep):
            index[step.id] = {"window_start", "window_end"} | set(step.session_key)
        else:
            index[step.id] = set()
    return index
