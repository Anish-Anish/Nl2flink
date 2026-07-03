"""Data engineer persona (Arjun): MCP status strip, then review / deployed / monitor."""
import html

import streamlit as st

from ui.api_client import get_deployed_pipelines, get_mcp_status, get_review_detail, get_review_queue
from ui.components.monitor_panel import render_monitor
from ui.components.review_card import render_review_card


def render_engineer():
    tab = st.session_state.get("engineer_tab", "review")
    queue = get_review_queue() or {"total_pending": 0, "queue": []}

    _mcp_strip()
    _tabs(tab, queue["total_pending"])

    if tab == "review":
        _review_tab(queue)
    elif tab == "deployed":
        _deployed_tab()
    else:
        render_monitor()


def _mcp_strip():
    """Which MCP servers the agents can reach right now."""
    status = get_mcp_status()
    if not status:
        return
    chips = ['<span class="mcp-title">MCP SERVERS</span>']
    for tool in status["tools"]:
        connected = tool["status"] == "connected"
        dot = '<span class="live-dot"></span>' if connected else '<span class="live-dot amber"></span>'
        detail = f'{tool["tool_count"]} tools' if connected else tool["status"]
        chips.append(f'<span class="mcp-chip">{dot}{html.escape(tool["name"])} '
                     f'<span class="lat">· {detail}</span></span>')
    st.markdown(f'<div class="mcp-topbar">{"".join(chips)}</div>', unsafe_allow_html=True)


def _tabs(active: str, pending: int):
    badge = f'<span class="etab-badge">{pending}</span>' if pending else ""
    css = lambda name: "active" if active == name else ""
    st.markdown(
        '<div class="eng-tabs">'
        f'<a class="etab {css("review")}" href="?persona=engineer&tab=review" target="_self">'
        f'Pipeline Review {badge}</a>'
        f'<a class="etab {css("deployed")}" href="?persona=engineer&tab=deployed" '
        f'target="_self">Deployed</a>'
        f'<a class="etab {css("monitor")}" href="?persona=engineer&tab=monitor" '
        f'target="_self">Live Monitor</a></div>', unsafe_allow_html=True)


def _review_tab(queue: dict):
    header, refresh = st.columns([6, 1], vertical_alignment="center")
    with header:
        st.markdown('<div class="section-label" style="margin:0">PENDING YOUR APPROVAL</div>',
                    unsafe_allow_html=True)
    with refresh:
        if st.button("Refresh", key="refresh_queue", use_container_width=True):
            st.rerun()

    if not queue["queue"]:
        st.markdown('<div class="eng-panel"><div style="text-align:center;padding:48px;'
                    'color:#6B7385"><p style="font-size:13px">No pipelines awaiting review</p>'
                    '</div></div>', unsafe_allow_html=True)
        return

    st.markdown('<div class="eng-panel" style="padding-top:6px">', unsafe_allow_html=True)
    for item in queue["queue"]:
        detail = get_review_detail(item["request_id"])
        if detail:
            render_review_card(item, detail)
    st.markdown('</div>', unsafe_allow_html=True)


def _deployed_tab():
    deployed = (get_deployed_pipelines() or {"deployed": []})["deployed"]
    cards = '<div class="eng-panel"><div class="section-label">RUNNING PIPELINES</div>'
    for pipeline in deployed:
        running = pipeline["state"].upper() == "RUNNING"
        badge = ('<span class="badge badge-green"><span class="live-dot"></span> Running</span>'
                 if running else f'<span class="badge badge-amber">{pipeline["state"]}</span>')
        cards += (
            '<div class="dep-card"><div class="dep-head"><div>'
            f'<div class="dep-name">{html.escape(pipeline["pipeline_name"] or "pipeline")}</div>'
            f'<div class="dep-meta">{badge}<span>Job {pipeline["flink_job_id"] or "—"} · '
            f'approved by {html.escape(pipeline["reviewed_by"] or "—")} · '
            f'{pipeline["minutes_ago"]} mins ago</span></div></div>'
            f'<div class="dep-rps">{pipeline["records_per_second"]:,.0f} records/s</div></div>'
            f'<div class="dep-tables">Requested by '
            f'<strong>{html.escape(pipeline["requested_by"])}</strong> · '
            f'Sink: {html.escape(pipeline["result_table"] or "alerts only")} · '
            f'commit {pipeline["github_commit_sha"] or "—"}</div></div>')
    if not deployed:
        cards += ('<div style="text-align:center;padding:40px;color:#6B7385;font-size:13px">'
                  'Nothing deployed yet</div>')
    st.markdown(cards + "</div>", unsafe_allow_html=True)
