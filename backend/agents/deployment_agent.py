"""
The agent that takes an approved pipeline live (LangGraph).

Runs once the engineer clicks approve, one node per real action, every action
performed through an MCP server:

    commit_to_github -> start_change_capture -> submit_to_flink
                     -> verify_result_table  -> notify_requester

Each node appends a DeployStep to the state, and `deploy()` yields those steps as they
complete, so the engineer sees the deployment log fill in live over SSE.

If a node fails the graph stops there and the last step is marked failed — nothing
downstream runs, so a pipeline is never reported live when Flink refused it.
"""
import json
import logging
from typing import Any, AsyncIterator, Dict, List, Optional

from langgraph.graph import END, StateGraph
from typing_extensions import TypedDict

from backend.api_models import DeployStep
from backend.pipeline_spec import PipelineSpec
from backend.services.flink_sql_builder import BuiltPipeline

logger = logging.getLogger(__name__)


class DeploymentState(TypedDict, total=False):
    request_id: str
    pipeline_name: str
    pipeline_yaml: str
    question: str
    requested_by: str
    approved_by: str
    statements: List[str]
    required_topics: List[str]
    result_table: Optional[str]
    slack_channel: str

    # filled in as the graph runs
    github_commit_sha: Optional[str]
    debezium_connector: Optional[str]
    flink_job_id: Optional[str]
    steps: List[Dict[str, Any]]
    failed: bool


class PipelineDeploymentAgent:
    """Builds and runs the deployment graph."""

    def __init__(self, toolbox, slack_channel: str) -> None:
        self._toolbox = toolbox
        self._slack_channel = slack_channel
        self._graph = self._build_graph()

    async def deploy(self, request_id: str, spec: PipelineSpec, pipeline_yaml: str,
                     built: BuiltPipeline, question: str, requested_by: str,
                     approved_by: str) -> AsyncIterator[DeployStep]:
        """Run the deployment, yielding each step as it finishes."""
        initial: DeploymentState = {
            "request_id": request_id,
            "pipeline_name": spec.name,
            "pipeline_yaml": pipeline_yaml,
            "question": question,
            "requested_by": requested_by,
            "approved_by": approved_by,
            "statements": built.statements,
            "required_topics": built.required_topics(),
            "result_table": spec.snowflake_result_table(),
            "slack_channel": self._slack_channel,
            "steps": [],
            "failed": False,
        }
        async for update in self._graph.astream(initial):
            for node_state in update.values():
                for step in node_state.get("steps", []):
                    yield DeployStep(**step)

    def final_state_fields(self, steps: List[DeployStep]) -> Dict[str, Any]:
        """Pull the ids worth storing out of the finished step list."""
        by_name = {step.name: step for step in steps}
        return {
            "github_commit_sha": _detail_value(by_name.get("commit_to_github")),
            "flink_job_id": _detail_value(by_name.get("submit_to_flink")),
        }

    # ── graph ────────────────────────────────────────────────────────────────
    def _build_graph(self):
        graph = StateGraph(DeploymentState)
        order = ["commit_to_github", "start_change_capture", "submit_to_flink",
                 "verify_result_table", "notify_requester"]
        graph.add_node("commit_to_github", self._commit_to_github)
        graph.add_node("start_change_capture", self._start_change_capture)
        graph.add_node("submit_to_flink", self._submit_to_flink)
        graph.add_node("verify_result_table", self._verify_result_table)
        graph.add_node("notify_requester", self._notify_requester)

        graph.set_entry_point(order[0])
        for current, following in zip(order, order[1:]):
            graph.add_conditional_edges(current, self._continue_or_stop,
                                        {"continue": following, "stop": END})
        graph.add_edge(order[-1], END)
        return graph.compile()

    @staticmethod
    def _continue_or_stop(state: DeploymentState) -> str:
        return "stop" if state.get("failed") else "continue"

    # ── nodes ────────────────────────────────────────────────────────────────
    async def _commit_to_github(self, state: DeploymentState) -> Dict[str, Any]:
        return await self._run_step(
            state, name="commit_to_github", agent="git_commit_agent",
            label="Committing pipeline to GitHub…",
            action=lambda: self._toolbox.call_tool(
                "commit_pipeline_yaml",
                pipeline_name=state["pipeline_name"],
                pipeline_yaml=state["pipeline_yaml"],
                message=f"Add pipeline {state['pipeline_name']} "
                        f"(approved by {state['approved_by']})"),
            describe=lambda result: f"commit {_field(result, 'short_sha')}")

    async def _start_change_capture(self, state: DeploymentState) -> Dict[str, Any]:
        async def action():
            await self._toolbox.call_tool("ensure_kafka_topics",
                                          topic_names=state["required_topics"])
            return await self._toolbox.call_tool("register_debezium_connector",
                                                 pipeline_yaml=state["pipeline_yaml"])

        return await self._run_step(
            state, name="start_change_capture", agent="cdc_agent",
            label="Starting Postgres change capture into Kafka…",
            action=action,
            describe=lambda result: f"{_field(result, 'connector')} · "
                                    f"{_field(result, 'state')}")

    async def _submit_to_flink(self, state: DeploymentState) -> Dict[str, Any]:
        return await self._run_step(
            state, name="submit_to_flink", agent="deploy_agent",
            label="Submitting the job to Flink…",
            action=lambda: self._toolbox.call_tool(
                "submit_flink_pipeline",
                statements=state["statements"], job_name=state["pipeline_name"]),
            describe=lambda result: f"job {_field(result, 'job_id')}")

    async def _verify_result_table(self, state: DeploymentState) -> Dict[str, Any]:
        result_table = state.get("result_table")
        if not result_table:
            return _completed_step(state, name="verify_result_table", agent="deploy_agent",
                                   label="Verifying the sink…",
                                   detail="no warehouse sink — results are delivered directly")
        return await self._run_step(
            state, name="verify_result_table", agent="deploy_agent",
            label="Verifying the Snowflake sink table…",
            action=lambda: self._toolbox.call_tool("check_snowflake_table_exists",
                                                   full_table_name=result_table),
            describe=lambda result: (f"{result_table} ready" if _truthy(result)
                                     else f"{result_table} not found"))

    async def _notify_requester(self, state: DeploymentState) -> Dict[str, Any]:
        message = (f"Your request is live: \"{state['question']}\"\n"
                   f"Pipeline `{state['pipeline_name']}` was approved by "
                   f"{state['approved_by']} and is now running on Flink.")
        return await self._run_step(
            state, name="notify_requester", agent="notify_agent",
            label="Notifying the requester on Slack…",
            action=lambda: self._toolbox.call_tool(
                "send_slack_message", channel=state["slack_channel"], message=message),
            describe=lambda result: f"sent to {state['slack_channel']}")

    # ── shared node behaviour ────────────────────────────────────────────────
    async def _run_step(self, state: DeploymentState, name: str, agent: str, label: str,
                        action, describe) -> Dict[str, Any]:
        """Run one MCP action and record it as a DeployStep, failing the graph on error."""
        try:
            result = await action()
        except Exception as error:
            logger.exception("deployment step %s failed", name)
            return {"failed": True, "steps": [DeployStep(
                name=name, label=label, status="failed",
                detail=str(error)[:300], agent=agent).model_dump()]}

        step = DeployStep(name=name, label=label, status="done",
                          detail=describe(result), agent=agent)
        return {"steps": [step.model_dump()], **_captured_ids(name, result)}


# ── helpers for reading MCP tool results ─────────────────────────────────────
def _decode(result: Any) -> Any:
    """MCP tools return JSON text; give back the decoded value when possible."""
    if isinstance(result, str):
        try:
            return json.loads(result)
        except ValueError:
            return result
    return result


def _field(result: Any, key: str) -> str:
    decoded = _decode(result)
    return str(decoded.get(key, "")) if isinstance(decoded, dict) else str(decoded)


def _truthy(result: Any) -> bool:
    decoded = _decode(result)
    return decoded is True or str(decoded).lower() == "true"


def _captured_ids(step_name: str, result: Any) -> Dict[str, Any]:
    """Keep the ids later steps and the database need."""
    decoded = _decode(result)
    if not isinstance(decoded, dict):
        return {}
    if step_name == "commit_to_github":
        return {"github_commit_sha": decoded.get("commit_sha")}
    if step_name == "start_change_capture":
        return {"debezium_connector": decoded.get("connector")}
    if step_name == "submit_to_flink":
        return {"flink_job_id": decoded.get("job_id")}
    return {}


def _completed_step(state: DeploymentState, name: str, agent: str, label: str,
                    detail: str) -> Dict[str, Any]:
    return {"steps": [DeployStep(name=name, label=label, status="done",
                                 detail=detail, agent=agent).model_dump()]}


def _detail_value(step: Optional[DeployStep]) -> Optional[str]:
    return step.detail.split()[-1] if step and step.detail else None
