"""
The shared objects the routers use, created once for the process.

Importing this module gives every router the same validator, agent and clients,
without any dependency-injection machinery.
"""
from backend.agents.deployment_agent import PipelineDeploymentAgent
from backend.agents.mcp_client import mcp_toolbox
from backend.agents.pipeline_agent import PipelineAuthoringAgent
from backend.config import get_settings
from backend.services import stack_clients
from backend.services.answer_reader import AnswerReader
from backend.services.flink_sql_builder import FlinkSqlBuilder
from backend.services.model_generator import ModelGenerator
from backend.services.pipeline_validator import PipelineValidator

settings = get_settings()

# stack clients (one per server)
postgres_schema = stack_clients.postgres_schema
debezium = stack_clients.debezium
kafka = stack_clients.kafka
flink_sql = stack_clients.flink_sql
flink_jobs = stack_clients.flink_jobs
snowflake = stack_clients.snowflake
slack = stack_clients.slack
github = stack_clients.github

# services
model_generator = ModelGenerator(settings)
sql_builder = FlinkSqlBuilder(settings)
validator = PipelineValidator(settings, postgres_schema)
answer_reader = AnswerReader(snowflake)

# agents (both act through the MCP servers in mcp_servers/)
toolbox = mcp_toolbox
authoring_agent = PipelineAuthoringAgent(settings, model_generator, validator, toolbox)
deployment_agent = PipelineDeploymentAgent(toolbox, settings.slack_default_channel)
