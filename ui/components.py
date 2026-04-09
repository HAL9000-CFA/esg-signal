"""
Reusable Streamlit UI components for the ESG Signal briefing.

All components are pure display functions — they write directly to the current
Streamlit context via st.* calls and return None.
"""

from typing import List
from urllib.parse import urlparse

import streamlit as st

from pipeline.models import (
    CommitmentCheck,
    CredibilityReport,
    EsgDcfMapping,
    FactorScore,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_FLAG_HEX = {"green": "#28a745", "amber": "#ffc107", "red": "#dc3545"}
_FLAG_LABEL = {"green": "GREEN", "amber": "AMBER", "red": "RED"}

_STREAM_LABELS = {
    "disclosure": "Disclosure Quality",
    "regulatory": "Regulatory Record",
    "talent": "Talent Signal",
    "words_money": "Words vs Money",
    "supply_chain": "Supply Chain",
}
_STREAM_WEIGHTS = {
    "disclosure": 0.30,
    "regulatory": 0.25,
    "talent": 0.20,
    "words_money": 0.15,
    "supply_chain": 0.10,
}

_WM_FLAG_ICON = {
    "consistent": "✅",
    "plausible": "🟡",
    "gap": "🔴",
    "unverifiable": "⬜",
}


# ---------------------------------------------------------------------------
# Confidence badge
# ---------------------------------------------------------------------------


def confidence_badge(flag: str, size: str = "normal") -> None:
    """
    Render an inline coloured confidence badge.

    Args:
        flag:  "green" | "amber" | "red"
        size:  "normal" (inline) | "large" (heading-level)
    """
    colour = _FLAG_HEX.get(flag, "#6c757d")
    label = _FLAG_LABEL.get(flag, flag.upper())
    font_size = "1.15rem" if size == "large" else "0.85rem"
    padding = "7px 16px" if size == "large" else "3px 10px"
    st.markdown(
        f'<span style="background:{colour};color:white;padding:{padding};'
        f"border-radius:4px;font-weight:700;font-size:{font_size};"
        f'letter-spacing:0.04em">{label}</span>',
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Score bar
# ---------------------------------------------------------------------------


def score_bar(score: float, flag: str) -> None:
    """Render a coloured percentage bar with the numeric score at the right end."""
    colour = _FLAG_HEX.get(flag, "#6c757d")
    pct = max(0, min(100, int(score * 100)))
    st.markdown(
        f'<div style="background:#e9ecef;border-radius:4px;height:18px;width:100%;margin:4px 0">'
        f'<div style="background:{colour};width:{pct}%;height:100%;border-radius:4px;'
        f'display:flex;align-items:center;justify-content:flex-end;padding-right:6px">'
        f'<span style="color:white;font-size:0.72rem;font-weight:700">{score:.2f}</span>'
        f"</div></div>",
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Factor panel
# ---------------------------------------------------------------------------


def factor_panel(fs: FactorScore) -> None:
    """
    Render a collapsible panel for one ESG factor.

    Displays: score bar, confidence badge, stream score breakdown (with weights),
    Claude narrative, evidence notes, Words vs Money check table, and source URLs.
    """
    header = f"{fs.factor_name}" f"  —  score: **{fs.score:.2f}**"
    with st.expander(header, expanded=False):
        col_badge, col_bar = st.columns([1, 4])
        with col_badge:
            confidence_badge(fs.flag)
        with col_bar:
            score_bar(fs.score, fs.flag)

        # --- Stream scores ---
        st.markdown("**Evidence Streams**")
        for key, label in _STREAM_LABELS.items():
            val = fs.stream_scores.get(key)
            if val is None:
                continue
            weight = _STREAM_WEIGHTS.get(key, 0.0)
            c1, c2, c3 = st.columns([3, 1, 3])
            c1.write(f"**{label}** ({int(weight * 100)}%)")
            c2.write(f"`{val:.2f}`")
            c3.progress(float(val))

        # --- Narrative ---
        if fs.narrative:
            st.markdown("---")
            st.markdown("**Analysis Narrative**")
            st.write(fs.narrative)

        # --- Evidence notes ---
        if fs.evidence:
            st.markdown("**Evidence Notes**")
            for note in fs.evidence:
                st.caption(f"• {note}")

        # --- Words vs Money checks ---
        if fs.words_money_checks:
            st.markdown("**Words vs Money Checks**")
            words_money_table(fs.words_money_checks)

        # --- Sources ---
        if fs.sources:
            st.markdown("**Sources**")
            for url in fs.sources:
                source_citation(url)


# ---------------------------------------------------------------------------
# Words vs Money table
# ---------------------------------------------------------------------------


def words_money_table(checks: List[CommitmentCheck]) -> None:
    """
    Render a table of Words vs Money commitment checks.

    Each row shows the commitment text, claimed amount, horizon, category,
    the matching financial figure, and the consistency flag.
    """
    rows = []
    for c in checks:
        text = c.commitment_text
        if len(text) > 90:
            text = text[:87] + "…"
        claimed = f"{c.currency} {c.claimed_amount:,.0f}" if c.claimed_amount is not None else "—"
        actual = f"{c.financials_value:,.0f}" if c.financials_value is not None else "—"
        rows.append(
            {
                "Commitment": text,
                "Claimed": claimed,
                "Year": str(c.horizon_year) if c.horizon_year else "—",
                "Category": c.category,
                "Actual (financial)": actual,
                "Flag": _WM_FLAG_ICON.get(c.flag, c.flag),
                "Notes": c.notes[:70] + ("…" if len(c.notes) > 70 else ""),
            }
        )
    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.caption("No commitment checks found.")


# ---------------------------------------------------------------------------
# Source citation
# ---------------------------------------------------------------------------


def source_citation(url: str) -> None:
    """Render a clickable source URL showing only the domain as the link text."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or url
    except Exception:
        domain = url
    st.markdown(f"- [{domain}]({url})")


# ---------------------------------------------------------------------------
# DCF mapping panel
# ---------------------------------------------------------------------------


def dcf_mapping_panel(mapping: EsgDcfMapping) -> None:
    """
    Render a collapsible panel for one ESG-factor DCF mapping.

    Shows: credibility flag, mapped line items, and low/mid/high scenario
    impact ranges with citation.
    """
    flag = mapping.credibility_flag or "amber"
    with st.expander(f"{mapping.factor_name}  —  credibility: **{flag.upper()}**", expanded=False):
        confidence_badge(flag)
        st.markdown("")  # spacing

        # Mapped line items
        if mapping.mapped_line_items:
            st.markdown(f"**Mapped DCF lines ({len(mapping.mapped_line_items)})**")
            for item in mapping.mapped_line_items:
                st.caption(f"• [{item.sheet_name}] row {item.row_index}: {item.label}")
        else:
            st.caption("No DCF line items mapped for this factor.")

        # Scenario ranges
        if mapping.scenario_low is not None:
            st.markdown("**Scenario Impact Range**")
            c1, c2, c3 = st.columns(3)
            ccy = mapping.scenario_currency
            c1.metric("Low", f"{ccy} {mapping.scenario_low:,.0f}")
            c2.metric("Mid", f"{ccy} {mapping.scenario_mid:,.0f}")
            c3.metric("High", f"{ccy} {mapping.scenario_high:,.0f}")
            if mapping.scenario_source:
                st.caption(f"Source: {mapping.scenario_source}")

        if mapping.financial_impacts:
            st.caption(f"Financial impact categories: {', '.join(mapping.financial_impacts)}")


# ---------------------------------------------------------------------------
# Overall credibility summary header
# ---------------------------------------------------------------------------


def credibility_header(report: CredibilityReport) -> None:
    """
    Render the top-of-page summary: company name, overall score, and flag.
    """
    st.markdown(f"## {report.company_name}")
    if report.sasb_industry:
        st.caption(f"SASB Industry: {report.sasb_industry}")

    col_badge, col_bar, col_meta = st.columns([1, 3, 2])
    with col_badge:
        confidence_badge(report.overall_flag, size="large")
    with col_bar:
        st.markdown("**Overall Credibility Score**")
        score_bar(report.overall_score, report.overall_flag)
    with col_meta:
        st.metric("Score", f"{report.overall_score:.2f}")
        st.caption(f"Ticker: {report.ticker or '—'}  |  Factors: {len(report.factor_scores)}")

    if report.errors:
        for err in report.errors:
            st.warning(err)
