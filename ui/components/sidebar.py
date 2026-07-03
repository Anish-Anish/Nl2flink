"""Left sidebar: brand, a New question button, and the requester's recent questions."""
import html

import streamlit as st

from ui.api_client import get_history

_STATUS_STYLE = {
    "live":           ("#16A34A", "Live"),
    "generating":     ("#4338CA", "Building…"),
    "validating":     ("#4338CA", "Checking…"),
    "pending_review": ("#D97706", "Awaiting engineer"),
    "approved":       ("#4338CA", "Connecting…"),
    "deploying":      ("#4338CA", "Connecting…"),
    "rejected":       ("#DC2626", "Needs revision"),
    "failed":         ("#DC2626", "Could not build"),
}


def render_sidebar():
    st.markdown('<div class="sidebar-brand">MegaStore<span> Pulse</span></div>',
                unsafe_allow_html=True)

    if st.button("New question", key="new_question", use_container_width=True):
        if st.session_state.persona != "business":
            from ui.components.topbar import sign_in
            sign_in("business")
        st.session_state.active_request_id = None
        st.session_state._last_opened = None
        st.query_params.clear()
        st.query_params["persona"] = "business"
        st.rerun()

    history = get_history(st.session_state.user_name or "Priya") or {"requests": []}

    st.markdown('<div class="conv-list-head">RECENT QUESTIONS</div>', unsafe_allow_html=True)
    items = []
    for request in history["requests"]:
        request_id = request["request_id"]
        colour, label = _STATUS_STYLE.get(request["status"], ("#DDE0EB", request["status"]))
        active = "active" if request_id == st.session_state.active_request_id else ""
        items.append(
            f'<div class="conv-item {active}">'
            f'<a class="conv-item-link" href="?persona=business&open={request_id}" target="_self">'
            f'<div class="conv-q">{html.escape(request["question"])}</div>'
            f'<div class="conv-meta"><span class="conv-dot" style="background:{colour}"></span>'
            f'<span>{label} · {_ago(request["minutes_ago"])}</span></div></a>'
            f'<a class="conv-delete-btn" href="?persona=business&delete={request_id}" '
            f'target="_self" title="Delete">×</a></div>')
    if not items:
        items = ['<div class="conv-item"><div class="conv-q">No questions yet</div></div>']
    st.markdown(f'<div class="conv-list">{"".join(items)}</div>', unsafe_allow_html=True)


def _ago(minutes: int) -> str:
    if not minutes:
        return "just now"
    if minutes < 60:
        return f"{minutes} mins ago"
    hours = minutes // 60
    return "1 hr ago" if hours == 1 else f"{hours} hrs ago"
