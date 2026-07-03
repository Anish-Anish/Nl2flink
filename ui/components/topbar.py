"""Top bar: brand, platform health, persona switch, current user."""
import streamlit as st

from ui.api_client import get_platform_status, login

_FALLBACK_USERS = {
    "business": {"user_id": "priya_001", "name": "Priya", "role": "CEO",
                 "persona": "business", "token": "offline"},
    "engineer": {"user_id": "arjun_001", "name": "Arjun", "role": "Data Engineer",
                 "persona": "engineer", "token": "offline"},
}
_AVATARS = {"business": ("P", "#7C3AED"), "engineer": ("A", "#0891B2")}


def sign_in(persona: str):
    """Sign in as a persona; fall back to a local identity when the backend is down."""
    user = login(persona) or _FALLBACK_USERS[persona]
    initial, colour = _AVATARS[persona]
    st.session_state.update(
        persona=user["persona"], user_id=user["user_id"], user_name=user["name"],
        user_role=user["role"], token=user["token"], avatar_initial=initial,
        avatar_color=colour, logged_in=True)


def render_topbar():
    status = get_platform_status()
    is_business = st.session_state.persona == "business"

    with st.container(key="topbar"):
        brand, health, business_button, engineer_button, user = st.columns(
            [2.4, 3.4, 1.2, 1.7, 2.6], vertical_alignment="center")
        with brand:
            st.markdown('<div class="topbar-brand">MegaStore<span> Pulse</span></div>',
                        unsafe_allow_html=True)
        with health:
            st.markdown(_health_line(status), unsafe_allow_html=True)
        with business_button:
            if st.button("Business", key="persona_business",
                         type="primary" if is_business else "secondary",
                         use_container_width=True):
                _switch("business")
        with engineer_button:
            if st.button("Data Engineer", key="persona_engineer",
                         type="primary" if not is_business else "secondary",
                         use_container_width=True):
                _switch("engineer")
        with user:
            st.markdown(
                '<div class="topbar-user-wrap">'
                f'<span class="topbar-avatar" style="background:{st.session_state.avatar_color}">'
                f'{st.session_state.avatar_initial}</span>'
                f'<span class="topbar-user">{st.session_state.user_name} '
                f'({st.session_state.user_role})</span></div>', unsafe_allow_html=True)


def _health_line(status) -> str:
    if status is None:
        return ('<div class="topbar-live"><span class="live-dot amber"></span> '
                'Backend offline — start the API on port 8000</div>')
    down = [service["name"] for service in status["services"] if not service["connected"]]
    active = status["active_pipelines"]
    if not down:
        return (f'<div class="topbar-live"><span class="pulse-dot"></span> '
                f'All systems connected · {active} pipelines live</div>')
    return (f'<div class="topbar-live"><span class="live-dot amber"></span> '
            f'{active} pipelines live · waiting on {", ".join(down)}</div>')


def _switch(persona: str):
    sign_in(persona)
    active_id = st.session_state.get("active_request_id")
    st.query_params.clear()
    st.query_params["persona"] = persona
    if persona == "business" and active_id:
        st.query_params["open"] = active_id
    if persona == "engineer":
        st.session_state.engineer_tab = "review"
        st.query_params["tab"] = "review"
    st.rerun()
