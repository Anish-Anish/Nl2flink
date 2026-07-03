"""
Business persona (Priya): ask in plain English, watch it build, read the answer.

No pipeline, YAML, Flink or Kafka wording appears on this side of the product.
"""
import html
from urllib.parse import quote

import streamlit as st

from ui.api_client import ask_question, get_answer, get_pipeline_status, get_suggestions
from ui.components.answer_card import render_answer_card
from ui.components.journey_trail import render_journey

_STILL_WORKING = ("generating", "validating", "pending_review", "approved", "deploying")


def render_business():
    if st.session_state.get("_pending_question"):
        question = st.session_state._pending_question
        st.session_state._pending_question = None
        _submit(question)

    if st.session_state.active_request_id:
        _conversation(st.session_state.active_request_id)
    else:
        _landing()

    typed = st.chat_input("Ask a business question… e.g. which stores are trending up right now?")
    if typed and typed.strip():
        _submit(typed.strip())


def _landing():
    """Greeting plus example questions, shown before anything has been asked."""
    first_name = (st.session_state.user_name or "there").split()[0]
    suggestions = (get_suggestions() or {}).get("suggestions", [])
    chips = "".join(
        f'<a class="sugg-card" href="?persona=business&q={quote(s["question"])}" target="_self">'
        f'<span class="sugg-txt">{html.escape(s["label"])}</span></a>' for s in suggestions)

    st.markdown(
        '<div class="chat-landing">'
        f'<div class="chat-greeting">Hi {html.escape(first_name)} — what would you like to know?</div>'
        '<div class="chat-sub">Ask anything about your stores, sales, inventory or customers in '
        'plain English. A data engineer only steps in to approve the answer.</div>'
        '<div class="chips-label" style="text-align:center;margin-top:26px">TRY ASKING</div>'
        f'<div class="chat-suggests">{chips}</div></div>', unsafe_allow_html=True)


@st.fragment(run_every=2)
def _conversation(request_id: str):
    """Re-renders only the conversation while the answer is still being built."""
    status = get_pipeline_status(request_id)
    if status is None:
        st.warning("Backend unreachable — showing the last known state")
        return
    _render_conversation(request_id, status)


def _render_conversation(request_id: str, status: dict):
    st.markdown('<div class="chat-thread">', unsafe_allow_html=True)
    st.markdown(
        f'<div class="chat-row user"><div class="bubble-user">'
        f'{html.escape(status["question"])}</div>'
        f'<div class="avatar-msg" style="background:{st.session_state.avatar_color}">'
        f'{st.session_state.avatar_initial}</div></div>', unsafe_allow_html=True)
    st.markdown('<div class="chat-row asst"><div class="avatar-msg ai">AI</div>'
                '<div class="bubble-asst">', unsafe_allow_html=True)

    render_journey(status["journey_steps"])
    _state_message(request_id, status)

    st.markdown('</div></div></div>', unsafe_allow_html=True)


def _state_message(request_id: str, status: dict):
    state = status["status"]

    if state in ("generating", "validating"):
        attempts = status.get("correction_attempts", 0)
        _banner(f"The model is correcting a small issue (attempt {attempts})…" if attempts
                else "The model is building your answer — this takes a few seconds.")

    elif state == "pending_review":
        _banner("<strong>A data engineer is reviewing your request</strong> — the live answer "
                "appears the moment it is approved.", amber=True)

    elif state in ("approved", "deploying"):
        _banner("<strong>Approved.</strong> Connecting your live data feed…")

    elif state == "live":
        answer = get_answer(request_id)
        if answer:
            render_answer_card(answer)

    elif state in ("rejected", "failed"):
        reason = status.get("rejection_reason") or "Please rephrase your question and try again."
        st.markdown(
            f'<div style="background:#FEE2E2;border:1px solid #FECACA;border-radius:13px;'
            f'padding:13px 18px;font-size:13px;color:#991B1B">'
            f'<strong>Your request needs adjustment:</strong> {html.escape(reason)}</div>',
            unsafe_allow_html=True)


def _banner(message_html: str, amber: bool = False):
    style = "proc-banner amber" if amber else "proc-banner"
    st.markdown(f'<div class="{style}"><div class="proc-spinner"></div>'
                f'<div>{message_html}</div></div>', unsafe_allow_html=True)


def _submit(question: str):
    with st.spinner("Sending your question to the model…"):
        response = ask_question(question, st.session_state.user_name or "Priya",
                                st.session_state.user_role or "CEO")
    if response:
        st.session_state.active_request_id = response["request_id"]
        st.session_state._last_opened = response["request_id"]
        st.rerun()
    else:
        st.error("Couldn't reach the backend. Is it running on port 8000?")
