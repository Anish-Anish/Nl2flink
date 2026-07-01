"""
All settings in one place. Every value can be overridden with an environment
variable of the same name (see .env.example).
"""
from functools import lru_cache
from typing import List

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ── Postgres: the operational database Debezium captures from ────────────
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "megastore_retail"
    postgres_user: str = "megastore"
    postgres_password: str = "megastore"
    postgres_schema: str = "public"

    # ── The studio's own database (pipeline requests, approvals, deployments) ─
    studio_db_url: str = "postgresql+psycopg2://megastore:megastore@localhost:5432/megastore_studio"

    # ── Debezium (Kafka Connect REST) ────────────────────────────────────────
    debezium_url: str = "http://localhost:8083"
    debezium_publication: str = "megastore_publication"

    # ── Kafka ────────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_partitions: int = 3
    kafka_replication_factor: int = 1

    # ── Flink (SQL Gateway runs statements, JobManager reports on jobs) ───────
    flink_sql_gateway_url: str = "http://localhost:8084/v1"
    flink_job_manager_url: str = "http://localhost:8081"
    flink_parallelism: int = 4
    flink_checkpoint_interval_ms: int = 60000

    # ── Snowflake: where results land and where answers are read from ────────
    snowflake_account: str = "megastore"
    snowflake_user: str = "megastore_service"
    snowflake_password: str = ""
    snowflake_role: str = "MEGASTORE_ENGINEER"
    snowflake_warehouse: str = "MEGASTORE_WH"
    snowflake_database: str = "MEGASTORE_DW"
    snowflake_schema: str = "ANALYTICS"

    # ── Slack ────────────────────────────────────────────────────────────────
    slack_bot_token: str = ""
    slack_default_channel: str = "#ops-alerts"

    # ── GitHub: approved pipeline YAML is committed here ─────────────────────
    github_token: str = ""
    github_owner: str = "megastore"
    github_repo: str = "megastore-pipelines"
    github_branch: str = "main"

    # ── The fine-tuned model server (model_server/model_server.py) ───────────
    model_server_url: str = "http://localhost:9000"
    model_max_new_tokens: int = 512
    model_timeout_seconds: float = 300.0
    model_name: str = "Qwen2.5-14B (MegaStore-LoRA)"

    # ── Agent behaviour ──────────────────────────────────────────────────────
    max_correction_attempts: int = 3

    # ── API ──────────────────────────────────────────────────────────────────
    # Streamlit today, React later — both call the same /api/v1 endpoints.
    cors_origins: List[str] = ["http://localhost:8501", "http://localhost:3000",
                              "http://localhost:5173"]
    http_timeout_seconds: float = 20.0

    @property
    def postgres_url(self) -> str:
        return (f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
                f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}")


@lru_cache
def get_settings() -> Settings:
    return Settings()
