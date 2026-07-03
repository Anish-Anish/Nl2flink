"""The six-step progress trail shown while a question is being turned into a pipeline."""
import streamlit as st

_ICONS = {"done": "✓", "active": "●", "rejected": "✕"}


def render_journey(steps: list):
    """steps come straight from the API as [{"label": ..., "state": ...}]."""
    parts = []
    for index, step in enumerate(steps):
        state = step["state"]
        icon = _ICONS.get(state, str(index + 1))
        parts.append(f'<div class="j-step {state}"><div class="j-icon">{icon}</div>'
                     f'<div class="j-label">{step["label"]}</div></div>')
        if index < len(steps) - 1:
            next_state = steps[index + 1]["state"]
            connector = ("done" if state == "done" and next_state == "done"
                         else "active" if state == "done" and next_state == "active" else "")
            parts.append(f'<div class="j-connector {connector}"></div>')

    st.markdown('<div class="journey-trail">'
                '<div class="journey-head-row">'
                '<div class="journey-head">HOW YOUR ANSWER IS BEING BUILT</div></div>'
                f'<div class="journey-steps">{"".join(parts)}</div></div>',
                unsafe_allow_html=True)
