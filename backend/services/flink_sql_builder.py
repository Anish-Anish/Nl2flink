"""
Turns a PipelineSpec into the Flink SQL that runs it.

The generated script is always in this order:

    SET ...                     job name, parallelism, checkpointing
    CREATE TEMPORARY TABLE ...  one Kafka source per captured Postgres table
    CREATE TEMPORARY VIEW  ...  one per transform, named after its step id
    CREATE TEMPORARY TABLE ...  one per sink
    INSERT INTO ...             one per sink (several are wrapped in a statement set,
                                so the whole pipeline is a single Flink job)

Sinks split in two:
  * snowflake / postgres_write -> written directly through the JDBC connector
  * slack / email / redis / notification_service -> written to a Kafka egress topic,
    because Flink has no connector for a Slack channel or an SMTP server. The backend's
    sink dispatcher reads that topic and performs the delivery.
"""
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from backend.config import Settings
from backend.pipeline_spec import (
    AllColumnsFrom, AntiJoinStep, Condition, DeduplicationStep, DELIVERY_SINK_TYPES,
    FilterStep, MapStep, PipelineSpec, PostgresWriteSink, SessionWindowStep,
    SnowflakeSink, StreamJoinStep, TimeUnit, TopNStep, WindowAggregationStep, WindowType,
)
from backend.services.stack_clients import PostgresTable, cdc_topic_name, egress_topic_name

# ── Postgres type -> Flink type (matches the Debezium converters we configure) ───
_FLINK_TYPES = {
    "bigint": "BIGINT", "integer": "INT", "smallint": "SMALLINT",
    "boolean": "BOOLEAN", "real": "FLOAT", "double precision": "DOUBLE",
    "numeric": "DOUBLE", "decimal": "DOUBLE", "money": "DOUBLE",
    "character varying": "STRING", "character": "STRING", "text": "STRING",
    "uuid": "STRING", "json": "STRING", "jsonb": "STRING",
    "date": "DATE", "time without time zone": "TIME(3)",
}
_OPERATOR_SYMBOLS = {
    "EQUALS": "=", "NOT_EQUALS": "<>", "GREATER_THAN": ">", "GREATER_THAN_OR_EQUALS": ">=",
    "LESS_THAN": "<", "LESS_THAN_OR_EQUALS": "<=", "LIKE": "LIKE",
}
_INTERVAL_UNITS = {TimeUnit.SECONDS: "SECOND", TimeUnit.MINUTES: "MINUTE",
                   TimeUnit.HOURS: "HOUR", TimeUnit.DAYS: "DAY"}
_COUNT_FUNCTIONS = {"COUNT", "COUNT_DISTINCT", "COUNT_IF"}
_ROW_NUMBER = "row_num"
WINDOW_COLUMNS = ["window_start", "window_end"]


def flink_type(postgres_type: str) -> str:
    """Flink SQL type for a Postgres information_schema data_type."""
    normalised = (postgres_type or "").strip().lower()
    if normalised.startswith("timestamp with"):
        return "TIMESTAMP_LTZ(3)"
    if normalised.startswith("timestamp"):
        return "TIMESTAMP(3)"
    return _FLINK_TYPES.get(normalised, "STRING")


def is_timestamp(flink_column_type: str) -> bool:
    return flink_column_type.startswith("TIMESTAMP")


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def sql_interval(size: int, unit: TimeUnit) -> str:
    return f"INTERVAL '{int(size)}' {_INTERVAL_UNITS[unit]}"


def sql_condition(condition: Condition) -> str:
    """One filter predicate as SQL."""
    if condition.expression:
        return f"({condition.expression})"
    field, operator = condition.field, condition.operator.value
    if operator == "IS_NULL":
        return f"{field} IS NULL"
    if operator == "IS_NOT_NULL":
        return f"{field} IS NOT NULL"
    if operator in ("IN", "NOT_IN"):
        values = condition.values if condition.values is not None else condition.value
        values = values if isinstance(values, (list, tuple)) else [values]
        keyword = "IN" if operator == "IN" else "NOT IN"
        return f"{field} {keyword} ({', '.join(sql_literal(v) for v in values)})"
    right = condition.field_ref or sql_literal(condition.value)
    return f"{field} {_OPERATOR_SYMBOLS[operator]} {right}"


def sql_where(conditions: List[Condition]) -> str:
    """Combine conditions using each one's own logical_operator (the first one's is ignored)."""
    parts = [sql_condition(conditions[0])]
    for condition in conditions[1:]:
        parts.append(f"{condition.logical_operator or 'AND'} {sql_condition(condition)}")
    return " ".join(parts)


def sql_aggregation(aggregation) -> str:
    function, field = aggregation.function.value, aggregation.field
    if function == "COUNT_DISTINCT":
        expression = f"COUNT(DISTINCT {field})"
    elif function == "COUNT_IF":           # Flink has no COUNT_IF; use a conditional COUNT
        expression = f"COUNT(CASE WHEN {aggregation.condition} THEN {field} END)"
    elif function == "LAST":
        expression = f"LAST_VALUE({field})"
    else:
        expression = f"{function}({field})"
    return f"{expression} AS `{aggregation.alias}`"


class Column(BaseModel):
    name: str
    type: str = "STRING"


class BuiltPipeline(BaseModel):
    """The compiled form of one pipeline."""

    pipeline_name: str
    statements: List[str] = Field(default_factory=list)
    source_topics: Dict[str, str] = Field(default_factory=dict)   # table -> CDC topic
    egress_topics: Dict[str, str] = Field(default_factory=dict)   # sink id -> egress topic
    sink_targets: List[str] = Field(default_factory=list)         # database tables written to

    def required_topics(self) -> List[str]:
        return list(self.source_topics.values()) + list(self.egress_topics.values())

    def script(self) -> str:
        """The whole job as one readable SQL script (shown on the review card)."""
        return ";\n\n".join(self.statements) + ";"


class FlinkSqlBuilder:
    """Builds the Flink SQL for one pipeline."""

    def __init__(self, settings: Settings):
        self._settings = settings

    def build(self, spec: PipelineSpec, tables: Dict[str, PostgresTable]) -> BuiltPipeline:
        self._spec = spec
        self._tables = tables
        self._columns: Dict[str, List[Column]] = {}
        self._resolve_columns()

        built = BuiltPipeline(
            pipeline_name=spec.name,
            source_topics={t.table: cdc_topic_name(spec.source.kafka_topic,
                                                   self._settings.postgres_schema, t.table)
                           for t in spec.source.tables},
        )
        statements = self._session_settings(spec)
        statements += [self._source_table(table, built.source_topics[table.name], spec)
                       for table in tables.values()]
        statements += [self._transform_view(step) for step in spec.transforms]

        inserts = []
        for sink in spec.sink:
            statements.append(self._sink_table(sink, spec, built))
            inserts.append(self._insert(sink))
        statements.append(inserts[0] if len(inserts) == 1
                          else "EXECUTE STATEMENT SET\nBEGIN\n" + ";\n".join(inserts) + ";\nEND")

        built.statements = statements
        return built

    # ── session ──────────────────────────────────────────────────────────────
    def _session_settings(self, spec: PipelineSpec) -> List[str]:
        return [
            f"SET 'pipeline.name' = '{spec.name}'",
            f"SET 'parallelism.default' = '{self._settings.flink_parallelism}'",
            f"SET 'execution.checkpointing.interval' = "
            f"'{self._settings.flink_checkpoint_interval_ms}ms'",
        ]

    # ── source tables ────────────────────────────────────────────────────────
    def _source_table(self, table: PostgresTable, topic: str, spec: PipelineSpec) -> str:
        lines = [f"  `{c.name}` {flink_type(c.data_type)}" for c in table.columns]

        event_time = self._event_time_column(table)
        if event_time:
            lines.append(f"  WATERMARK FOR `{event_time}` AS `{event_time}` - INTERVAL '5' SECOND")
        captured = next((t for t in spec.source.tables if t.table == table.name), None)
        if captured and table.has_column(captured.primary_key):
            lines.append(f"  PRIMARY KEY (`{captured.primary_key}`) NOT ENFORCED")

        return self._create_table(table.name, lines, {
            "connector": "kafka",
            "topic": topic,
            "properties.bootstrap.servers": self._settings.kafka_bootstrap_servers,
            "properties.group.id": f"megastore-studio-{spec.name}-{table.name}",
            "scan.startup.mode": "earliest-offset",
            "format": "debezium-json",
            "debezium-json.schema-include": "false",
            "debezium-json.ignore-parse-errors": "true",
        })

    def _event_time_column(self, table: PostgresTable) -> Optional[str]:
        """The column the watermark sits on: one the pipeline names, else updated_at/created_at."""
        timestamps = [c.name for c in table.columns if is_timestamp(flink_type(c.data_type))]
        for declared in self._declared_watermarks():
            if declared in timestamps:
                return declared
        for preferred in ("updated_at", "created_at", "occurred_at", "event_time"):
            if preferred in timestamps:
                return preferred
        return timestamps[0] if timestamps else None

    def _declared_watermarks(self) -> List[str]:
        fields = []
        for step in self._spec.transforms:
            if isinstance(step, WindowAggregationStep) and step.window.watermark_field:
                fields.append(step.window.watermark_field)
            elif isinstance(step, SessionWindowStep) and step.watermark_field:
                fields.append(step.watermark_field)
        return fields

    # ── transform views ──────────────────────────────────────────────────────
    def _transform_view(self, step) -> str:
        return f"CREATE TEMPORARY VIEW `{step.id}` AS\n{self._transform_query(step)}"

    def _transform_query(self, step) -> str:
        if isinstance(step, FilterStep):
            return f"  SELECT *\n  FROM `{step.source}`\n  WHERE {sql_where(step.conditions)}"

        if isinstance(step, MapStep):
            computed = ",\n         ".join(f"{f.expression} AS `{f.alias}`"
                                           for f in step.computed_fields)
            return f"  SELECT *,\n         {computed}\n  FROM `{step.source}`"

        if isinstance(step, StreamJoinStep):
            return (f"  SELECT {self._join_output(step)}\n"
                    f"  FROM `{step.left}`\n"
                    f"  {step.join_type} JOIN `{step.right}`\n"
                    f"    ON `{step.left}`.`{step.join_key.left}` = "
                    f"`{step.right}`.`{step.join_key.right}`")

        if isinstance(step, AntiJoinStep):
            predicates = [f"`{step.right}`.`{step.join_key.right}` = "
                          f"`{step.left}`.`{step.join_key.left}`"]
            if step.time_constraint:
                bound = step.time_constraint
                predicates.append(
                    f"`{step.right}`.`{bound.right_field}` BETWEEN "
                    f"`{step.left}`.`{bound.left_field}` AND `{step.left}`.`{bound.left_field}` + "
                    f"{sql_interval(bound.window_minutes, TimeUnit.MINUTES)}")
            return (f"  SELECT `{step.left}`.*\n  FROM `{step.left}`\n"
                    f"  WHERE NOT EXISTS (\n    SELECT 1 FROM `{step.right}`\n"
                    f"    WHERE " + "\n      AND ".join(predicates) + "\n  )")

        if isinstance(step, WindowAggregationStep):
            grouping = WINDOW_COLUMNS + list(step.group_by)
            selected = [f"`{c}`" for c in grouping] + [sql_aggregation(a) for a in step.aggregations]
            select_list = ",\n         ".join(selected)
            group_list = ", ".join(f"`{c}`" for c in grouping)
            return (f"  SELECT {select_list}\n"
                    f"  FROM {self._window_function(step)}\n"
                    f"  GROUP BY {group_list}")

        if isinstance(step, SessionWindowStep):
            grouping = WINDOW_COLUMNS + list(step.session_key)
            keys = ", ".join(f"`{k}`" for k in step.session_key)
            return (f"  SELECT {', '.join(f'`{c}`' for c in grouping)}\n"
                    f"  FROM TABLE(SESSION(\n"
                    f"         TABLE `{step.source}` PARTITION BY {keys},\n"
                    f"         DESCRIPTOR(`{self._time_column(step.source, step.watermark_field)}`),\n"
                    f"         {sql_interval(step.gap, step.gap_unit)}))\n"
                    f"  GROUP BY {', '.join(f'`{c}`' for c in grouping)}")

        if isinstance(step, DeduplicationStep):
            keys = ", ".join(f"`{k}`" for k in step.key)
            order_column = self._order_column(step.source)
            return (f"  SELECT {self._column_list(step.source)}\n  FROM (\n"
                    f"    SELECT *, ROW_NUMBER() OVER (\n"
                    f"      PARTITION BY {keys} ORDER BY `{order_column}` ASC) AS `{_ROW_NUMBER}`\n"
                    f"    FROM `{step.source}`\n  )\n  WHERE `{_ROW_NUMBER}` = 1")

        if isinstance(step, TopNStep):
            return (f"  SELECT {self._column_list(step.source)}\n  FROM (\n"
                    f"    SELECT *, ROW_NUMBER() OVER (\n"
                    f"      ORDER BY `{step.order_by}` {step.order}) AS `{_ROW_NUMBER}`\n"
                    f"    FROM `{step.source}`\n  )\n  WHERE `{_ROW_NUMBER}` <= {step.n}")

        raise ValueError(f"step '{step.id}' has an unsupported type")

    def _window_function(self, step: WindowAggregationStep) -> str:
        """TUMBLE / HOP / SESSION window table function for a window aggregation."""
        window = step.window
        descriptor = f"DESCRIPTOR(`{self._time_column(step.source, window.watermark_field)}`)"
        if window.type == WindowType.TUMBLING:
            return (f"TABLE(TUMBLE(\n         TABLE `{step.source}`,\n         {descriptor},\n"
                    f"         {sql_interval(window.size, window.unit)}))")
        if window.type == WindowType.SLIDING:
            return (f"TABLE(HOP(\n         TABLE `{step.source}`,\n         {descriptor},\n"
                    f"         {sql_interval(window.slide, window.slide_unit or window.unit)},\n"
                    f"         {sql_interval(window.size, window.unit)}))")
        partition = (f" PARTITION BY {', '.join(f'`{c}`' for c in step.group_by)}"
                     if step.group_by else "")
        return (f"TABLE(SESSION(\n         TABLE `{step.source}`{partition},\n"
                f"         {descriptor},\n         {sql_interval(window.gap, window.unit)}))")

    def _join_output(self, step: StreamJoinStep) -> str:
        if not step.output_fields:
            return "*"
        rendered = []
        for field in step.output_fields:
            if isinstance(field, AllColumnsFrom):
                rendered.append(f"`{field.all_from}`.*")
                continue
            relation, separator, column = str(field).rpartition(".")
            rendered.append(f"`{relation}`.`{column}`" if separator else f"`{column}`")
        return ",\n         ".join(rendered)

    # ── sink tables ──────────────────────────────────────────────────────────
    def _sink_table(self, sink, spec: PipelineSpec, built: BuiltPipeline) -> str:
        if isinstance(sink, SnowflakeSink):
            lines = self._column_lines(sink.source)
            if sink.write_mode.value in ("UPSERT", "UPDATE") and sink.upsert_key:
                lines.append(f"  PRIMARY KEY ({', '.join(f'`{k}`' for k in sink.upsert_key)}) "
                             f"NOT ENFORCED")
            built.sink_targets.append(sink.full_table_name)
            return self._create_table(sink.id, lines, {
                "connector": "jdbc",
                "url": (f"jdbc:snowflake://{self._settings.snowflake_account}"
                        f".snowflakecomputing.com/?db={sink.database}&schema={sink.schema_name}"
                        f"&warehouse={self._settings.snowflake_warehouse}"),
                "table-name": f"{sink.schema_name}.{sink.table}",
                "username": self._settings.snowflake_user,
                "password": self._settings.snowflake_password,
                "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
                "sink.buffer-flush.max-rows": "500",
                "sink.buffer-flush.interval": "5s",
            })

        if isinstance(sink, PostgresWriteSink):
            if sink.fields_mapping:
                upstream = {c.name: c.type for c in self._columns.get(sink.source, [])}
                lines = [f"  `{target}` {upstream.get(source, 'STRING')}"
                         for target, source in sink.fields_mapping.items()]
            else:
                lines = self._column_lines(sink.source)
            if sink.write_mode.value in ("UPSERT", "UPDATE") and sink.match_key:
                lines.append(f"  PRIMARY KEY ({', '.join(f'`{k}`' for k in sink.match_key)}) "
                             f"NOT ENFORCED")
            built.sink_targets.append(f"{self._settings.postgres_db}."
                                      f"{self._settings.postgres_schema}.{sink.table}")
            return self._create_table(sink.id, lines, {
                "connector": "jdbc",
                "url": (f"jdbc:postgresql://{self._settings.postgres_host}:"
                        f"{self._settings.postgres_port}/{self._settings.postgres_db}"),
                "table-name": f"{self._settings.postgres_schema}.{sink.table}",
                "username": self._settings.postgres_user,
                "password": self._settings.postgres_password,
                "driver": "org.postgresql.Driver",
            })

        # Delivery sink: Flink writes JSON to a Kafka topic, the dispatcher delivers it.
        topic = egress_topic_name(spec.name, sink.id)
        built.egress_topics[sink.id] = topic
        return self._create_table(sink.id, self._column_lines(sink.source), {
            "connector": "kafka",
            "topic": topic,
            "properties.bootstrap.servers": self._settings.kafka_bootstrap_servers,
            "format": "json",
            "json.timestamp-format.standard": "ISO-8601",
        })

    def _insert(self, sink) -> str:
        if isinstance(sink, PostgresWriteSink) and sink.fields_mapping:
            projection = ",\n         ".join(f"`{source}` AS `{target}`"
                                             for target, source in sink.fields_mapping.items())
        else:
            projection = self._column_list(sink.source)
        return f"INSERT INTO `{sink.id}`\n  SELECT {projection}\n  FROM `{sink.source}`"

    @staticmethod
    def _create_table(name: str, column_lines: List[str], properties: Dict[str, str]) -> str:
        options = ",\n".join(f"  '{key}' = '{value}'" for key, value in properties.items())
        return (f"CREATE TEMPORARY TABLE `{name}` (\n" + ",\n".join(column_lines) +
                f"\n) WITH (\n{options}\n)")

    # ── column resolution ────────────────────────────────────────────────────
    def _resolve_columns(self) -> None:
        """Work out the columns each relation produces, source tables first."""
        for table in self._tables.values():
            self._columns[table.name] = [Column(name=c.name, type=flink_type(c.data_type))
                                         for c in table.columns]
        for step in self._spec.transforms:
            self._columns[step.id] = self._columns_of(step)

    def _columns_of(self, step) -> List[Column]:
        upstream = self._columns.get(getattr(step, "source", ""), [])

        if isinstance(step, (FilterStep, DeduplicationStep, TopNStep)):
            return list(upstream)

        if isinstance(step, MapStep):
            return upstream + [Column(name=f.alias, type="STRING") for f in step.computed_fields]

        if isinstance(step, AntiJoinStep):
            return list(self._columns.get(step.left, []))

        if isinstance(step, StreamJoinStep):
            left = self._columns.get(step.left, [])
            right = self._columns.get(step.right, [])
            if not step.output_fields:
                return _unique(left + right)
            selected: List[Column] = []
            for field in step.output_fields:
                if isinstance(field, AllColumnsFrom):
                    selected += self._columns.get(field.all_from, [])
                    continue
                relation, _, column_name = str(field).rpartition(".")
                pool = self._columns.get(relation, left + right)
                found = next((c for c in pool + left + right if c.name == column_name), None)
                selected.append(found or Column(name=column_name))
            return _unique(selected)

        if isinstance(step, WindowAggregationStep):
            columns = [Column(name=c, type="TIMESTAMP(3)") for c in WINDOW_COLUMNS]
            columns += [next((c for c in upstream if c.name == g), Column(name=g))
                        for g in step.group_by]
            for aggregation in step.aggregations:
                function = aggregation.function.value
                if function in _COUNT_FUNCTIONS:
                    column_type = "BIGINT"
                elif function in ("SUM", "AVG"):
                    column_type = "DOUBLE"
                else:
                    source = next((c for c in upstream if c.name == aggregation.field), None)
                    column_type = source.type if source else "DOUBLE"
                columns.append(Column(name=aggregation.alias, type=column_type))
            return _unique(columns)

        if isinstance(step, SessionWindowStep):
            columns = [Column(name=c, type="TIMESTAMP(3)") for c in WINDOW_COLUMNS]
            columns += [next((c for c in upstream if c.name == k), Column(name=k))
                        for k in step.session_key]
            return _unique(columns)

        return []

    def _column_lines(self, step_id: str) -> List[str]:
        columns = self._columns.get(step_id, [])
        return [f"  `{c.name}` {c.type}" for c in columns] or ["  `payload` STRING"]

    def _column_list(self, step_id: str) -> str:
        names = [c.name for c in self._columns.get(step_id, [])]
        return ",\n         ".join(f"`{name}`" for name in names) if names else "*"

    def _time_column(self, step_id: str, declared: Optional[str]) -> str:
        """Event-time column for a window. Windows cannot run without one."""
        columns = self._columns.get(step_id, [])
        if declared and (not columns or any(c.name == declared for c in columns)):
            return declared
        for column in columns:
            if is_timestamp(column.type):
                return column.name
        if declared:
            return declared
        raise ValueError(f"step '{step_id}' has no timestamp column to use as event time")

    def _order_column(self, step_id: str) -> str:
        """Ordering column for deduplication — a timestamp if there is one, else anything stable."""
        columns = self._columns.get(step_id, [])
        for column in columns:
            if is_timestamp(column.type):
                return column.name
        if columns:
            return columns[0].name
        raise ValueError(f"step '{step_id}' produces no columns to deduplicate on")


def _unique(columns: List[Column]) -> List[Column]:
    seen, result = set(), []
    for column in columns:
        if column.name not in seen:
            seen.add(column.name)
            result.append(column)
    return result
