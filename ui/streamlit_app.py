"""
MegaStore Pulse — the Streamlit front end.

Two personas share one app:
  Business (Priya)       ask a question, watch it build, read the live answer
  Data engineer (Arjun)  review the generated pipeline, approve it, watch it run

The backend must be running first:

    uvicorn backend.main:app --port 8000 --reload
    streamlit run ui/streamlit_app.py

The UI only talks to /api/v1 (see ui/api_client.py), so it can be replaced by a React
app without any backend change.
"""
import os
import sys

import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ui.api_client import delete_request                       # noqa: E402
from ui.components.sidebar import render_sidebar               # noqa: E402
from ui.components.topbar import render_topbar, sign_in        # noqa: E402
from ui.utils.state import init_state                          # noqa: E402
from ui.views.business_view import render_business             # noqa: E402
from ui.views.engineer_view import render_engineer             # noqa: E402

st.set_page_config(page_title="MegaStore Pulse", layout="wide",
                   initial_sidebar_state="expanded")


def _load_styles():
    path = os.path.join(os.path.dirname(__file__), "assets", "style.css")
    try:
        with open(path) as stylesheet:
            st.markdown(f"<style>{stylesheet.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        pass


def _apply_query_params():
    """The URL is the source of truth for persona, open conversation and tab."""
    params = st.query_params

    delete_id = params.get("delete")
    if delete_id:
        delete_request(delete_id)
        if st.session_state.active_request_id == delete_id:
            st.session_state.active_request_id = None
            st.session_state._last_opened = None
        st.query_params.clear()
        st.query_params["persona"] = st.session_state.persona
        st.toast("Question deleted")
        st.rerun()

    persona = params.get("persona")
    if persona in ("business", "engineer") and persona != st.session_state.persona:
        sign_in(persona)

    tab = params.get("tab")
    if tab in ("review", "deployed", "monitor"):
        st.session_state.engineer_tab = tab

    question = params.get("q")
    if question is not None and question != st.session_state._last_question:
        st.session_state._pending_question = question
        st.session_state._last_question = question

    opened = params.get("open")
    if opened and opened != st.session_state._last_opened:
        st.session_state.active_request_id = opened
        st.session_state._last_opened = opened


def main():
    init_state()
    _load_styles()
    if not st.session_state.logged_in:
        sign_in(st.session_state.persona)
    _apply_query_params()

    render_topbar()
    with st.sidebar:
        render_sidebar()

    if st.session_state.persona == "business":
        render_business()
    else:
        render_engineer()


if __name__ == "__main__":
    main()
