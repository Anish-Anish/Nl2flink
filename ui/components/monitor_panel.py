"""Live monitor: Flink throughput and the raw change events flowing through Kafka."""
import html

import streamlit as st

from ui.api_client import get_event_feed, get_monitor_metrics

_CDC_TABLES = ["orders", "order_items", "returns", "inventory", "payments"]


@st.fragment(run_every=3)
def render_monitor():
    """Refreshes only this panel every 3 seconds — no full page reload."""
    metrics = get_monitor_metrics()
    if metrics is None:
        st.warning("Backend unreachable — no live metrics")
        return

    cards = [
        (f'{metrics["active_pipelines"]}', "Active pipelines"),
        (f'{metrics["total_records_per_second"]:,.0f}', "Records/sec"),
        (f'{metrics["task_managers"]}', "Task managers"),
        (f'{metrics["slots_available"]}', "Free slots"),
    ]
    grid = "".join(f'<div class="m-card"><div class="m-val">{value}</div>'
                   f'<div class="m-lbl">{label}</div></div>' for value, label in cards)

    pipelines = "".join(
        f'<div class="pipeline-row"><span class="pulse-dot"></span>'
        f'<span class="pr-name">{html.escape(pipeline["pipeline_name"] or "pipeline")}</span>'
        f'<span class="pr-stats">{pipeline["records_per_second"]:,.0f} records/s</span></div>'
        for pipeline in metrics.get("pipelines", [])) or \
        '<div class="pipeline-row"><span class="pr-name">No pipelines running yet</span></div>'

    table = st.selectbox("CDC table", _CDC_TABLES, key="monitor_table", label_visibility="collapsed")
    feed = get_event_feed(table) or {"events": [], "topic": ""}
    feed_lines = [
        f'<span class="feed-ts">[{html.escape(event["timestamp"])}]</span> '
        f'<span class="feed-store">{html.escape(event.get("table") or "")}</span> · '
        f'<span class="feed-cat">{html.escape(event.get("operation") or "")}</span> · '
        f'<span class="feed-amt">{html.escape(event["summary"])}</span>'
        for event in feed["events"]] or ["<span class='feed-ts'>waiting for change events…</span>"]

    st.markdown(
        '<div class="eng-panel">'
        f'<div class="monitor-grid">{grid}</div>'
        '<div class="section-label">PIPELINE THROUGHPUT</div>'
        f'{pipelines}'
        f'<div class="section-label">CHANGE EVENTS · {html.escape(feed.get("topic") or "")}</div>'
        f'<div class="feed-box">{"<br>".join(feed_lines)}</div>'
        '</div>', unsafe_allow_html=True)
