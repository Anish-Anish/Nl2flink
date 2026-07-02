"""
Calls the fine-tuned model server and builds the instructions it is asked to answer.

Nothing under training/ or model_server/ is changed by this file. It only speaks the
HTTP contract model_server.py already exposes:

    POST /generate  {"query": ..., "max_new_tokens": ...} -> {"yaml": "..."}
    GET  /health                                          -> {"ok": true}

The server builds the `### Instruction: / ### Response:` wrapper itself, so whatever we
put in `query` becomes the instruction.

Prompting rule: the first attempt sends the question exactly as a user typed it,
because that is the form the model was fine-tuned on. Schema context and validation
errors are only added when asking it to correct a pipeline it already got wrong.
"""
import logging
import time
from typing import Dict, List

import requests
from pydantic import BaseModel

from backend.config import Settings
from backend.services.stack_clients import PostgresTable, StackError

logger = logging.getLogger(__name__)


class GeneratedPipeline(BaseModel):
    yaml_text: str
    model_name: str
    generation_time_ms: int


class ModelGenerator:
    """The fine-tuned 14B model that writes pipelines."""

    def __init__(self, settings: Settings):
        self._settings = settings

    def generate(self, question: str) -> GeneratedPipeline:
        """First attempt — the question on its own, as the model was trained."""
        return self._call(question)

    def correct(self, question: str, previous_yaml: str, errors: List[str],
                tables: Dict[str, PostgresTable]) -> GeneratedPipeline:
        """Ask the model to fix a pipeline that failed validation."""
        return self._call(build_correction_instruction(question, previous_yaml, errors, tables))

    def is_healthy(self) -> bool:
        try:
            url = f"{self._settings.model_server_url.rstrip('/')}/health"
            return requests.get(url, timeout=3).ok
        except requests.RequestException:
            return False

    def _call(self, instruction: str) -> GeneratedPipeline:
        url = f"{self._settings.model_server_url.rstrip('/')}/generate"
        started = time.monotonic()
        try:
            response = requests.post(
                url,
                json={"query": instruction, "max_new_tokens": self._settings.model_max_new_tokens},
                timeout=self._settings.model_timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as error:
            raise StackError("model server", f"{error}") from error

        yaml_text = strip_code_fence((response.json() or {}).get("yaml", "").strip())
        if not yaml_text:
            raise StackError("model server", "returned an empty response")

        elapsed_ms = int((time.monotonic() - started) * 1000)
        logger.info("model generated %d characters in %d ms", len(yaml_text), elapsed_ms)
        return GeneratedPipeline(yaml_text=yaml_text, model_name=self._settings.model_name,
                                 generation_time_ms=elapsed_ms)


def build_correction_instruction(question: str, previous_yaml: str, errors: List[str],
                                 tables: Dict[str, PostgresTable]) -> str:
    """The retry prompt: the original question, what was wrong, and the real schema."""
    problems = "\n".join(f"- {error}" for error in errors)
    schema = "\n".join(table.describe() for table in tables.values()) or "(no tables resolved)"
    return (
        f"{question}\n\n"
        f"The pipeline below was generated for that request but failed validation.\n"
        f"Fix these problems and return the corrected pipeline YAML only.\n\n"
        f"Problems:\n{problems}\n\n"
        f"Available tables and columns:\n{schema}\n\n"
        f"Previous pipeline:\n{previous_yaml}"
    )


def strip_code_fence(text: str) -> str:
    """Remove a ```yaml fence if the model wrapped its answer in one."""
    if not text.startswith("```"):
        return text
    lines = text.splitlines()[1:]
    if lines and lines[-1].strip().startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines).strip()
