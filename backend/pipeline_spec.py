"""
Pydantic model of the YAML the fine-tuned model produces.

The model always emits this shape (verified against all 2,917 examples in dataset/):

    pipeline:
      name: low_inventory_alert
      version: "1.0"
      source:     { type: postgres_cdc, connection: ..., tables: [...], kafka_topic: ... }
      transforms: [ {id, type, ...}, ... ]
      sink:       [ {id, type, source, ...}, ... ]

Parsing raw YAML into these classes is the first validation layer: if it does not
fit, the model is asked to correct itself.
"""
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


# ── vocabularies ─────────────────────────────────────────────────────────────
class CaptureMode(str, Enum):
    CDC_ONLY = "cdc_only"                              # stream changes from now on
    FULL_SNAPSHOT_THEN_CDC = "full_snapshot_then_cdc"  # copy the table first


class Operator(str, Enum):
    EQUALS = "EQUALS"
    NOT_EQUALS = "NOT_EQUALS"
    GREATER_THAN = "GREATER_THAN"
    GREATER_THAN_OR_EQUALS = "GREATER_THAN_OR_EQUALS"
    LESS_THAN = "LESS_THAN"
    LESS_THAN_OR_EQUALS = "LESS_THAN_OR_EQUALS"
    IN = "IN"
    NOT_IN = "NOT_IN"
    LIKE = "LIKE"
    IS_NULL = "IS_NULL"
    IS_NOT_NULL = "IS_NOT_NULL"


class WindowType(str, Enum):
    TUMBLING = "TUMBLING"
    SLIDING = "SLIDING"
    SESSION = "SESSION"


class TimeUnit(str, Enum):
    SECONDS = "SECONDS"
    MINUTES = "MINUTES"
    HOURS = "HOURS"
    DAYS = "DAYS"


class AggregationFunction(str, Enum):
    COUNT = "COUNT"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    COUNT_IF = "COUNT_IF"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    LAST = "LAST"


class WriteMode(str, Enum):
    APPEND = "APPEND"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    UPSERT = "UPSERT"


class DslModel(BaseModel):
    """Unknown keys are kept rather than rejected — the generator is a language model."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)


# ── source ───────────────────────────────────────────────────────────────────
class CapturedTable(DslModel):
    table: str
    primary_key: str
    capture_mode: CaptureMode = CaptureMode.CDC_ONLY


class PostgresCdcSource(DslModel):
    type: str = "postgres_cdc"
    connection: str = "megastore_primary"
    tables: List[CapturedTable] = Field(min_length=1)
    kafka_topic: str = "megastore.retail_events"

    def table_names(self) -> List[str]:
        return [t.table for t in self.tables]


# ── transform building blocks ────────────────────────────────────────────────
class Condition(DslModel):
    """One filter predicate. Exactly one right-hand side is used, in this order:
    expression (raw SQL) > field_ref (another column) > values (IN) > value."""

    field: str
    operator: Operator
    value: Optional[Any] = None
    values: Optional[List[Any]] = None
    field_ref: Optional[str] = None
    expression: Optional[str] = None
    logical_operator: str = "AND"


class ComputedField(DslModel):
    alias: str
    expression: str


class Window(DslModel):
    """TUMBLING/SLIDING use `size`; SESSION uses `gap`. `unit` applies to both."""

    type: WindowType
    unit: TimeUnit
    size: Optional[int] = None
    slide: Optional[int] = None
    slide_unit: Optional[TimeUnit] = None
    gap: Optional[int] = None
    watermark_field: Optional[str] = None
    allowed_lateness: Optional[Union[int, str]] = None


class Aggregation(DslModel):
    alias: str
    function: AggregationFunction
    field: str
    condition: Optional[str] = None  # required by COUNT_IF


class JoinKey(DslModel):
    left: str
    right: str


class TimeConstraint(DslModel):
    """Bounds an anti-join, e.g. 'no matching order within 15 minutes'."""

    left_field: str
    right_field: str
    window_minutes: int


class AllColumnsFrom(DslModel):
    """`{all_from: <step id>}` in output_fields — keep every column of that step."""

    all_from: str


OutputField = Union[str, AllColumnsFrom]


# ── transforms ───────────────────────────────────────────────────────────────
class FilterStep(DslModel):
    """Keeps matching rows -> WHERE."""

    id: str
    type: Literal["filter"] = "filter"
    source: str
    conditions: List[Condition] = Field(min_length=1)


class MapStep(DslModel):
    """Adds derived columns -> extra SELECT expressions."""

    id: str
    type: Literal["map"] = "map"
    source: str
    computed_fields: List[ComputedField] = Field(min_length=1)


class StreamJoinStep(DslModel):
    """Joins two streams -> INNER/LEFT JOIN."""

    id: str
    type: Literal["stream_join"] = "stream_join"
    left: str
    right: str
    join_key: JoinKey
    join_type: str = "INNER"
    output_fields: List[OutputField] = Field(default_factory=list)


class AntiJoinStep(DslModel):
    """Left rows with no match on the right -> NOT EXISTS."""

    id: str
    type: Literal["anti_join"] = "anti_join"
    left: str
    right: str
    join_key: JoinKey
    time_constraint: Optional[TimeConstraint] = None


class WindowAggregationStep(DslModel):
    """Groups rows inside a time window -> Flink window function + GROUP BY."""

    id: str
    type: Literal["window_aggregation"] = "window_aggregation"
    source: str
    window: Window
    group_by: List[str] = Field(default_factory=list)
    aggregations: List[Aggregation] = Field(min_length=1)


class SessionWindowStep(DslModel):
    """Groups rows into activity sessions separated by an inactivity gap."""

    id: str
    type: Literal["session_window"] = "session_window"
    source: str
    session_key: List[str] = Field(min_length=1)
    gap: int
    gap_unit: TimeUnit = TimeUnit.MINUTES
    watermark_field: Optional[str] = None


class DeduplicationStep(DslModel):
    """Keeps the first row per key -> ROW_NUMBER() = 1."""

    id: str
    type: Literal["deduplication"] = "deduplication"
    source: str
    key: List[str] = Field(min_length=1)
    window: Optional[Window] = None


class TopNStep(DslModel):
    """Keeps the best N rows -> ROW_NUMBER() <= n."""

    id: str
    type: Literal["top_n"] = "top_n"
    source: str
    order_by: str
    order: str = "DESC"
    n: int = Field(gt=0)


Transform = Union[FilterStep, MapStep, StreamJoinStep, AntiJoinStep,
                  WindowAggregationStep, SessionWindowStep, DeduplicationStep, TopNStep]


# ── sinks ────────────────────────────────────────────────────────────────────
def _as_list(value: Any) -> Any:
    """The model writes a single key as a bare string; callers want a list."""
    return [value] if isinstance(value, str) else value


class SnowflakeSink(DslModel):
    """Warehouse table — this is what a business answer is read from."""

    id: str
    type: Literal["snowflake"] = "snowflake"
    source: str
    connection: str = "megastore_snowflake"
    database: str = "MEGASTORE_DW"
    schema_name: str = Field(default="ANALYTICS", alias="schema")
    table: str
    write_mode: WriteMode = WriteMode.APPEND
    upsert_key: Optional[List[str]] = None

    _list_upsert_key = field_validator("upsert_key", mode="before")(_as_list)

    @property
    def full_table_name(self) -> str:
        return f"{self.database}.{self.schema_name}.{self.table}"


class SlackSink(DslModel):
    id: str
    type: Literal["slack"] = "slack"
    source: str
    channel: str
    webhook_url: str
    message_template: Optional[str] = None


class RedisSink(DslModel):
    id: str
    type: Literal["redis"] = "redis"
    source: str
    connection: str = "megastore_redis"
    key_template: str
    value_format: str = "JSON"
    ttl_seconds: Optional[int] = None
    sorted_set: Optional[Dict[str, str]] = None


class EmailSink(DslModel):
    id: str
    type: Literal["email"] = "email"
    source: str
    to: List[str] = Field(default_factory=list)
    to_field: Optional[str] = None
    subject_template: str
    body_template: str


class PostgresWriteSink(DslModel):
    """Writes rows back into an operational Postgres table."""

    id: str
    type: Literal["postgres_write"] = "postgres_write"
    source: str
    connection: str = "megastore_primary"
    table: str
    write_mode: WriteMode = WriteMode.INSERT
    fields_mapping: Dict[str, str] = Field(default_factory=dict)  # target -> source
    match_key: Optional[List[str]] = None

    _list_match_key = field_validator("match_key", mode="before")(_as_list)


class NotificationSink(DslModel):
    id: str
    type: Literal["notification_service"] = "notification_service"
    source: str
    channel: str = "PUSH"
    notification_type: str
    template_id: str
    recipient_field: str
    metadata_fields: List[str] = Field(default_factory=list)


Sink = Union[SnowflakeSink, SlackSink, RedisSink, EmailSink, PostgresWriteSink, NotificationSink]

#: Sinks Flink cannot write to directly — their rows go to a Kafka topic that the
#: backend's sink dispatcher reads and delivers (see services/pipeline_deployer.py).
DELIVERY_SINK_TYPES = {"slack", "email", "redis", "notification_service"}


# ── the whole pipeline ───────────────────────────────────────────────────────
class PipelineSpec(DslModel):
    name: str
    version: str = "1.0"
    description: Optional[str] = None
    source: PostgresCdcSource
    transforms: List[Transform] = Field(default_factory=list)
    sink: List[Sink] = Field(min_length=1)

    @field_validator("sink", mode="before")
    @classmethod
    def _single_sink_to_list(cls, value: Any) -> Any:
        return [value] if isinstance(value, dict) else value

    def step_ids(self) -> List[str]:
        return [t.id for t in self.transforms]

    def find_step(self, step_id: str) -> Optional[Transform]:
        return next((t for t in self.transforms if t.id == step_id), None)

    def upstream_of(self, transform: Transform) -> List[str]:
        """Which relations a step reads from — joins read two, everything else one."""
        if isinstance(transform, (StreamJoinStep, AntiJoinStep)):
            return [transform.left, transform.right]
        return [transform.source]

    def snowflake_result_table(self) -> Optional[str]:
        """The warehouse table a business answer should be read from."""
        for sink in self.sink:
            if isinstance(sink, SnowflakeSink):
                return sink.full_table_name
        return None


class PipelineDocument(DslModel):
    """The whole YAML file: one `pipeline:` key."""

    pipeline: PipelineSpec


def parse_pipeline_yaml(yaml_text: str) -> PipelineSpec:
    """Parse generated YAML into a PipelineSpec. Raises ValueError with a readable reason."""
    try:
        document = yaml.safe_load(yaml_text)
    except yaml.YAMLError as error:
        raise ValueError(f"not valid YAML: {str(error)[:200]}") from error
    if not isinstance(document, dict) or "pipeline" not in document:
        raise ValueError("YAML must have a single top-level 'pipeline:' key")
    return PipelineDocument.model_validate(document).pipeline


def render_pipeline_yaml(spec: PipelineSpec) -> str:
    """Serialise a spec back to YAML — used for the editor and the GitHub commit."""
    document = {"pipeline": spec.model_dump(exclude_none=True, by_alias=True, mode="json")}
    return yaml.safe_dump(document, sort_keys=False, allow_unicode=True)
