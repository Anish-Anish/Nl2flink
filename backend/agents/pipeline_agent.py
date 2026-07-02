"""
The agent that turns a question into a validated pipeline (LangGraph).

    ground_in_schema  ->  generate  ->  validate  --valid-->  END
                                            ^                  |
                                            |               invalid
                                            +---- correct <----+

  ground_in_schema  asks the postgres_schema MCP server what tables exist
  generate          asks the fine-tuned model for the pipeline YAML
  validate          runs PipelineValidator (schema + Flink SQL compilation)
  correct           sends the errors and the real schema back to the model,
                    up to `max_correction_attempts` times

The graph never deploys anything — it stops at a pipeline a human can review.
"""
import logging
from typing import Any, Dict, List, Optional

from langgraph.graph import END, StateGraph
from typing_extensions import TypedDict

from backend.config import Settings
from backend.services.model_generator import ModelGenerator
from backend.services.pipeline_validator import PipelineValidator, ValidationResult

logger = logging.getLogger(__name__)


class AuthoringState(TypedDict, total=False):
    """What flows between the nodes."""

    question: str
    available_tables: List[str]
    pipeline_yaml: str
    model_name: str
    generation_time_ms: int
    attempts: int              # how many correction rounds have run
    checks: List[str]
    errors: List[str]
    valid: bool
    result: Optional[ValidationResult]


class PipelineAuthoringAgent:
    """Builds and runs the authoring graph."""

    def __init__(self, settings: Settings, generator: ModelGenerator,
                 validator: PipelineValidator, toolbox) -> None:
        self._settings = settings
        self._generator = generator
        self._validator = validator
        self._toolbox = toolbox
        self._graph = self._build_graph()

    async def run(self, question: str) -> AuthoringState:
        """Generate, validate and (if needed) correct a pipeline for one question."""
        return await self._graph.ainvoke({"question": question, "attempts": 0})

    # ── graph ────────────────────────────────────────────────────────────────
    def _build_graph(self):
        graph = StateGraph(AuthoringState)
        graph.add_node("ground_in_schema", self._ground_in_schema)
        graph.add_node("generate", self._generate)
        graph.add_node("validate", self._validate)
        graph.add_node("correct", self._correct)

        graph.set_entry_point("ground_in_schema")
        graph.add_edge("ground_in_schema", "generate")
        graph.add_edge("generate", "validate")
        graph.add_conditional_edges("validate", self._after_validation,
                                    {"correct": "correct", "finish": END})
        graph.add_edge("correct", "validate")
        return graph.compile()

    def _after_validation(self, state: AuthoringState) -> str:
        """Stop when it is valid, or when the model has had enough attempts."""
        if state.get("valid"):
            return "finish"
        if state.get("attempts", 0) >= self._settings.max_correction_attempts:
            return "finish"
        return "correct"

    # ── nodes ────────────────────────────────────────────────────────────────
    async def _ground_in_schema(self, state: AuthoringState) -> Dict[str, Any]:
        """Ask the schema MCP server which tables exist, so corrections can be grounded."""
        try:
            tables = await self._toolbox.call_tool("list_postgres_tables")
        except Exception as error:
            logger.warning("schema grounding unavailable: %s", error)
            tables = []
        return {"available_tables": _as_string_list(tables)}

    async def _generate(self, state: AuthoringState) -> Dict[str, Any]:
        """First attempt: the question exactly as asked, which is how the model was tuned."""
        generated = self._generator.generate(state["question"])
        return {"pipeline_yaml": generated.yaml_text,
                "model_name": generated.model_name,
                "generation_time_ms": generated.generation_time_ms}

    async def _validate(self, state: AuthoringState) -> Dict[str, Any]:
        result = self._validator.validate_yaml(state["pipeline_yaml"])
        if not result.valid:
            logger.info("validation failed (attempt %d): %s",
                        state.get("attempts", 0), result.errors)
        return {"valid": result.valid, "checks": result.checks,
                "errors": result.errors, "result": result}

    async def _correct(self, state: AuthoringState) -> Dict[str, Any]:
        """Send the errors and the schema of the referenced tables back to the model."""
        tables = self._validator_tables(state)
        corrected = self._generator.correct(
            question=state["question"],
            previous_yaml=state["pipeline_yaml"],
            errors=state.get("errors", []),
            tables=tables)
        return {"pipeline_yaml": corrected.yaml_text,
                "generation_time_ms": corrected.generation_time_ms,
                "attempts": state.get("attempts", 0) + 1}

    def _validator_tables(self, state: AuthoringState):
        """Columns of the tables the failed pipeline referenced, for the retry prompt."""
        result = state.get("result")
        if result and result.spec:
            return self._validator.describe_tables(result.spec.source.table_names())
        return self._validator.describe_tables(state.get("available_tables", [])[:10])


def _as_string_list(value: Any) -> List[str]:
    """MCP tools return JSON; accept a list, or a JSON string holding one."""
    import json
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return [str(item) for item in parsed] if isinstance(parsed, list) else []
        except ValueError:
            return []
    return []
