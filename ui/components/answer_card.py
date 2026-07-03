"""
Renders a live answer.

The API returns plain data — columns, rows and KPIs — so all of the markup is built
here. That is what lets a React UI use the same endpoint later.
"""
import html

import streamlit as st


def trust_badge(trust_score) -> str:
    if not trust_score:
        return ""
    style = ("trust-badge" if trust_score >= 9
             else "trust-badge mid" if trust_score >= 7.5 else "trust-badge low")
    return f'<span class="{style}">★ Trust {trust_score}/10</span>'


def render_answer_card(answer: dict):
    """Header, KPI row, result table and any message the backend attached."""
    header = (
        '<div class="answer-header"><div>'
        f'<div class="answer-title">{html.escape(answer.get("title", "Your answer"))}</div>'
        f'<div class="answer-sub">{html.escape(answer.get("subtitle", ""))}</div>'
        f'{_reviewed_line(answer)}</div>'
        f'<div class="answer-meta">{trust_badge(answer.get("trust_score"))}'
        f'{_live_badge(answer)}</div></div>'
    )

    body = _kpi_row(answer.get("kpis", []))
    if answer.get("rows"):
        body += _result_table(answer["columns"], answer["rows"])
    if answer.get("message"):
        body += (f'<div class="insight-strip" style="background:var(--brand-soft);'
                 f'border-top-color:#C7D2FE"><div class="insight-text" '
                 f'style="color:var(--brand)">{html.escape(answer["message"])}</div></div>')

    st.markdown(f'<div class="answer-card">{header}{body}</div>', unsafe_allow_html=True)


def _reviewed_line(answer: dict) -> str:
    reviewer = answer.get("reviewed_by")
    if not reviewer:
        return ""
    return (f'<div class="answer-sub" style="margin-top:4px">Reviewed &amp; approved by '
            f'<strong>{html.escape(reviewer)}</strong> (Data Engineer)</div>')


def _live_badge(answer: dict) -> str:
    if not answer.get("is_live"):
        return ""
    return ('<span class="badge badge-green">'
            '<span class="pulse-dot" style="width:5px;height:5px"></span> Live</span>')


def _kpi_row(kpis: list) -> str:
    if not kpis:
        return ""
    cells = "".join(
        f'<div class="kpi-cell"><div class="kpi-val">{html.escape(str(kpi["value"]))}</div>'
        f'<div class="kpi-lbl">{html.escape(kpi["label"])}</div>'
        f'{_delta(kpi)}</div>' for kpi in kpis)
    return f'<div class="kpi-row">{cells}</div>'


def _delta(kpi: dict) -> str:
    if not kpi.get("delta"):
        return ""
    return (f'<div class="kpi-delta {kpi.get("direction", "flat")}">'
            f'{html.escape(kpi["delta"])}</div>')


def _result_table(columns: list, rows: list) -> str:
    head = "".join(f"<th>{html.escape(str(column))}</th>" for column in columns)
    body = "".join(
        "<tr>" + "".join(f"<td>{_cell(value)}</td>" for value in row) + "</tr>"
        for row in rows)
    return ('<div class="detail-label">RESULTS FROM THE PIPELINE</div>'
            f'<table class="answer-table"><thead><tr>{head}</tr></thead>'
            f'<tbody>{body}</tbody></table>')


def _cell(value) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        return html.escape(f"{value:,.2f}")
    if isinstance(value, int):
        return html.escape(f"{value:,}")
    return html.escape(str(value))
