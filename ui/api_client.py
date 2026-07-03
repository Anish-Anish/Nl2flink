"""
Every call the UI makes to the backend.

This is the only file in ui/ that knows about HTTP. Each function maps to one
/api/v1 endpoint and returns the JSON as-is, or None when the backend is unreachable,
so the views can show a "backend offline" message instead of crashing.
"""
import json
import os
from typing import Any, Dict, Iterator, List, Optional

import requests

BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
API = f"{BASE_URL}/api/v1"
TIMEOUT = 15


def _get(path: str, params: Optional[dict] = None) -> Optional[Any]:
    try:
        response = requests.get(f"{API}{path}", params=params, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException:
        return None


def _send(method: str, path: str, body: Optional[dict] = None) -> Optional[Any]:
    try:
        response = requests.request(method, f"{API}{path}", json=body, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException:
        return None


# ── auth ─────────────────────────────────────────────────────────────────────
def login(persona: str) -> Optional[Dict[str, Any]]:
    return _send("POST", "/auth/login", {"persona": persona})


# ── business persona ─────────────────────────────────────────────────────────
def ask_question(question: str, requested_by: str, role: str) -> Optional[Dict[str, Any]]:
    return _send("POST", "/pipelines/ask",
                 {"question": question, "requested_by": requested_by, "requested_by_role": role})


def get_pipeline_status(request_id: str) -> Optional[Dict[str, Any]]:
    return _get(f"/pipelines/{request_id}/status")


def get_answer(request_id: str) -> Optional[Dict[str, Any]]:
    return _get(f"/pipelines/{request_id}/answer")


def get_history(requested_by: str) -> Optional[Dict[str, Any]]:
    return _get("/pipelines/history", {"requested_by": requested_by})


def get_suggestions() -> Optional[Dict[str, Any]]:
    return _get("/pipelines/suggestions")


def delete_request(request_id: str) -> Optional[Dict[str, Any]]:
    return _send("DELETE", f"/pipelines/{request_id}")


# ── engineer persona ─────────────────────────────────────────────────────────
def get_review_queue() -> Optional[Dict[str, Any]]:
    return _get("/review/queue")


def get_review_detail(request_id: str) -> Optional[Dict[str, Any]]:
    return _get(f"/review/{request_id}")


def save_edited_pipeline(request_id: str, pipeline_yaml: str,
                         edited_by: str) -> Optional[Dict[str, Any]]:
    return _send("PUT", f"/review/{request_id}/pipeline",
                 {"pipeline_yaml": pipeline_yaml, "edited_by": edited_by})


def reject_pipeline(request_id: str, rejected_by: str, reason: str) -> Optional[Dict[str, Any]]:
    return _send("POST", f"/review/{request_id}/reject",
                 {"rejected_by": rejected_by, "reason": reason})


def approve_pipeline(request_id: str, approved_by: str) -> Iterator[Dict[str, Any]]:
    """
    Approve and deploy, yielding one dict per deployment step.

    The endpoint answers with server-sent events; each event is a single JSON object
    on a `data:` line.
    """
    response = requests.post(f"{API}/review/{request_id}/approve",
                             json={"approved_by": approved_by}, stream=True, timeout=300)
    response.raise_for_status()
    for raw_line in response.iter_lines(decode_unicode=True):
        if raw_line and raw_line.startswith("data:"):
            yield json.loads(raw_line[5:].strip())


def get_deployed_pipelines() -> Optional[Dict[str, Any]]:
    return _get("/review/deployed")


# ── monitoring ───────────────────────────────────────────────────────────────
def get_monitor_metrics() -> Optional[Dict[str, Any]]:
    return _get("/monitor/metrics")


def get_event_feed(table: str = "orders", limit: int = 20) -> Optional[Dict[str, Any]]:
    return _get("/monitor/events", {"table": table, "limit": limit})


def get_platform_status() -> Optional[Dict[str, Any]]:
    return _get("/platform/status")


def get_mcp_status() -> Optional[Dict[str, Any]]:
    return _get("/platform/mcp")
