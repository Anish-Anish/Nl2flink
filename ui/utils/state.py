"""Session state defaults for the Streamlit app."""
DEFAULTS = {
    "persona": "business",
    "user_id": None,
    "user_name": None,
    "user_role": None,
    "token": None,
    "avatar_initial": "P",
    "avatar_color": "#7C3AED",
    "logged_in": False,
    "active_request_id": None,     # None = landing page (chat box + suggestions)
    "engineer_tab": "review",
    "editing_card": None,          # request id of the card being edited
    "_last_question": "",          # query-parameter change detection
    "_last_opened": None,
    "_pending_question": None,     # a suggestion chip was clicked
}


def init_state():
    import streamlit as st
    for key, value in DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = value
