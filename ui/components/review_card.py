"""
One pipeline waiting for the engineer: the YAML, the Flink SQL it compiles to,
the validation result, and the approve / edit / reject actions.
"""
import html

import streamlit as st
from streamlit_ace import st_ace

from ui.api_client import approve_pipeline, reject_pipeline, save_edited_pipeline
from ui.components.answer_card import trust_badge

_STEP_ICONS = {"done": "✓", "running": "⠿", "failed": "✕"}


def render_review_card(item: dict, detail: dict):
    request_id = item["request_id"]
    editing = st.session_state.get("editing_card") == request_id

    with st.container(key=f"review_{request_id}"):
        _header(item, detail)

        heading = ("EDITING — change the YAML and click Save"
                   if editing else "GENERATED PIPELINE · read-only — click Edit to modify")
        st.markdown(f'<div class="rc-yaml-wrap"><div class="rc-yaml-head">{heading}</div></div>',
                    unsafe_allow_html=True)
        edited_yaml = st_ace(
            value=detail.get("pipeline_yaml", ""), language="yaml",
            theme="tomorrow_night_bright", keybinding="vscode", font_size=13, tab_size=2,
            wrap=True, show_gutter=True, readonly=not editing, auto_update=True,
            height=220, key=f"ace_{request_id}")

        _validation_badges(detail)
        _flink_sql_expander(request_id, detail)

        deploy_log = st.empty()
        if editing:
            _edit_actions(request_id, edited_yaml)
        else:
            _review_actions(request_id, deploy_log)


def _header(item: dict, detail: dict):
    confidence = int((item.get("confidence") or 0) * 100)
    initial = (item.get("requested_by") or "?")[0].upper()
    corrections = item.get("correction_attempts", 0)
    correction_badge = (f'<span class="badge badge-amber">self-corrected ×{corrections}</span>'
                        if corrections else "")
    tables = ", ".join(detail.get("source_tables", [])) or "—"
    sinks = ", ".join(detail.get("sink_targets", [])) or "—"

    st.markdown(
        '<div class="rc-header"><div>'
        f'<div class="rc-title">{html.escape(item["pipeline_name"])}</div>'
        '<div class="rc-meta">'
        f'<span class="rc-requester"><span class="avatar av-p">{initial}</span> '
        f'{html.escape(item["requested_by"])} ({html.escape(item.get("requested_by_role", ""))}) '
        f'· {item["minutes_ago"]} mins ago</span>'
        f'<span class="badge badge-brand">AI confidence: {confidence}%</span>'
        f'{correction_badge}</div></div>'
        '<div style="display:flex;flex-direction:column;align-items:flex-end;gap:6px">'
        f'{trust_badge(item.get("trust_score"))}'
        '<span class="badge badge-amber">Pending</span></div></div>'
        f'<div class="business-question">"{html.escape(item["question"])}"</div>'
        f'<div class="rc-meta" style="margin-top:6px">'
        f'<span>Reads: {html.escape(tables)}</span><span>Writes: {html.escape(sinks)}</span>'
        f'</div>',
        unsafe_allow_html=True)


def _validation_badges(detail: dict):
    checks = "".join(f'<span class="badge badge-green">{html.escape(check)}</span>'
                     for check in detail.get("validation_checks", []))
    errors = "".join(f'<span class="badge badge-red">{html.escape(error)}</span>'
                     for error in detail.get("validation_errors", []))
    st.markdown(f'<div class="validation-row">{checks}{errors}</div>', unsafe_allow_html=True)


def _flink_sql_expander(request_id: str, detail: dict):
    """The SQL this pipeline compiles to — what Flink will actually run."""
    flink_sql = detail.get("flink_sql")
    if not flink_sql:
        return
    with st.expander("Flink SQL this pipeline compiles to"):
        st.code(flink_sql, language="sql")


def _edit_actions(request_id: str, edited_yaml: str):
    save_column, cancel_column, _ = st.columns([1.6, 1, 4], vertical_alignment="center")
    with save_column:
        save_clicked = st.button("Save & re-validate", key=f"save_{request_id}",
                                 type="primary", use_container_width=True)
    with cancel_column:
        cancel_clicked = st.button("Cancel", key=f"cancel_{request_id}",
                                   use_container_width=True)

    if save_clicked:
        result = save_edited_pipeline(request_id, edited_yaml, st.session_state.user_name)
        if result and result.get("valid"):
            st.session_state.editing_card = None
            st.toast("Edited pipeline re-validated")
            st.rerun()
        else:
            errors = (result or {}).get("validation_errors", ["backend unreachable"])
            st.error("Still invalid:\n\n" + "\n".join(f"- {error}" for error in errors))
    if cancel_clicked:
        st.session_state.editing_card = None
        st.rerun()


def _review_actions(request_id: str, deploy_log):
    approve_column, edit_column, reject_column, _ = st.columns(
        [2, 1.4, 1, 4], vertical_alignment="center")
    with approve_column:
        approve_clicked = st.button("Approve & Deploy", key=f"approve_{request_id}",
                                    type="primary", use_container_width=True)
    with edit_column:
        edit_clicked = st.button("Edit pipeline", key=f"edit_{request_id}",
                                 use_container_width=True)
    with reject_column:
        reject_clicked = st.button("Reject", key=f"reject_{request_id}",
                                   use_container_width=True)

    if edit_clicked:
        st.session_state.editing_card = request_id
        st.rerun()
    if reject_clicked:
        reject_pipeline(request_id, st.session_state.user_name,
                        "Needs more detail on the time window.")
        st.toast("Sent back to the requester")
        st.rerun()
    if approve_clicked:
        _stream_deployment(request_id, deploy_log)


def _stream_deployment(request_id: str, container):
    """Render the deployment log as the backend streams each step."""
    import time

    lines = []
    try:
        for step in approve_pipeline(request_id, st.session_state.user_name):
            icon = _STEP_ICONS.get(step["status"], "·")
            detail = (f'<span class="dp-detail">{html.escape(step.get("detail", ""))}</span>'
                      if step.get("detail") else "")
            agent = (f'<span class="dp-agent">[{html.escape(step.get("agent", ""))}]</span>'
                     if step.get("agent") else "")
            css = "done" if step["status"] == "done" else step["status"]
            lines.append(f'<div class="dp-step {css}">{icon} '
                         f'{html.escape(step["label"])}{detail}{agent}</div>')
            container.markdown(f'<div class="deploy-progress">{"".join(lines)}</div>',
                               unsafe_allow_html=True)
            if step["name"] == "finished":
                st.toast("Pipeline deployed" if step["status"] == "done"
                         else "Deployment failed")
                time.sleep(2)
                st.rerun()
    except Exception as error:
        st.error(f"Deployment could not start: {error}")
