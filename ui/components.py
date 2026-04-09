"""
Reusable Streamlit UI components for the ESG Signal briefing.

All components are pure display functions — they write directly to the current
Streamlit context via st.* calls and return None.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional
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

# Used in st.dataframe (no HTML rendering) — plain Unicode symbols, not emoji
_WM_FLAG_ICON = {
    "consistent": "✓ consistent",
    "plausible": "~ plausible",
    "gap": "✗ gap",
    "unverifiable": "– unverifiable",
}

# Used in unsafe_allow_html markdown contexts (pipeline status panel, etc.)
_BI = {
    "check": '<i class="bi bi-check-circle-fill"      style="color:#28a745"></i>',
    "warn": '<i class="bi bi-exclamation-circle-fill" style="color:#ffc107"></i>',
    "error": '<i class="bi bi-x-circle-fill"           style="color:#dc3545"></i>',
    "neutral": '<i class="bi bi-dash-circle"             style="color:#adb5bd"></i>',
    "info": '<i class="bi bi-info-circle-fill"        style="color:#007bff"></i>',
    "warning": '<i class="bi bi-exclamation-triangle-fill" style="color:#ffc107"></i>',
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

        # --- Coverage / confidence summary ---
        cov = getattr(fs, "coverage", 1.0)
        conf = getattr(fs, "confidence", cov)
        cov_col, conf_col = st.columns(2)
        cov_col.metric(
            "Coverage",
            f"{cov:.0%}",
            help="Structural: fraction of scoreable streams that returned data",
        )
        conf_col.metric(
            "Confidence",
            f"{conf:.0%}",
            help="Quality-weighted: accounts for evidence strength within active streams",
        )

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
                st.markdown(
                    f'<p style="font-size:0.88rem;color:#444;margin:2px 0">&#8226; {note}</p>',
                    unsafe_allow_html=True,
                )

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
    Render the top-of-page summary: company name, overall score, flag, and interpretation guide.
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

    # Coverage / confidence summary — structural breadth and quality-weighted signal strength
    all_coverages = [getattr(fs, "coverage", 1.0) for fs in report.factor_scores]
    all_confidences = [
        getattr(fs, "confidence", getattr(fs, "coverage", 1.0)) for fs in report.factor_scores
    ]
    mean_coverage = sum(all_coverages) / len(all_coverages) if all_coverages else 0.0
    mean_confidence = sum(all_confidences) / len(all_confidences) if all_confidences else 0.0
    active_label = f"{mean_coverage:.0%} coverage · {mean_confidence:.0%} confidence"

    with st.expander("What does this score mean?", expanded=False):
        st.markdown(
            f"""
**The Commitment Credibility Score (0–1)** measures the gap between a company's
stated ESG positions and the verifiable evidence supporting them. It is not a
measure of ESG performance — it measures whether stated commitments are backed
by corroborating signals across five independent evidence streams.

| Score | Flag | Interpretation |
|---|---|---|
| ≥ 0.80 + coverage ≥ 75% | **Green** | Strong, cross-validated commitment — disclosure, regulatory record, and financial signals broadly align |
| 0.40 – 0.79 | **Amber** | Partial evidence — commitments are plausible but not fully corroborated; gaps or thin data in one or more streams |
| < 0.40 | **Red** | Weak or contradicted — limited disclosure, regulatory concerns, or material misalignment between words and capital |

**This report: {active_label}.**
Coverage measures how many streams contributed data (structural breadth). Confidence is quality-weighted — it accounts for evidence strength within active streams (e.g. QUANTIFIED disclosure > VAGUE, large talent sample > small, verifiable WvM commitments > absent).
{'Scores based on fewer than 3 of 4 active streams should be treated as indicative, not conclusive.' if mean_coverage < 0.75 else 'Coverage is sufficient for reasonable confidence in the ratings.'}

**Evidence streams** (weights in brackets):
- **Disclosure Quality** (30%) — SASB-material factor grading from annual/sustainability report: Quantified, Vague, or Undisclosed
- **Regulatory Record** (25%) — Mandatory filings: EPA GHGRP/ECHO/NRC (US), EA Pollution Inventory (UK), EU ETS (EU)
- **Talent Signal** (20%) — ESG-related hiring activity as a proxy for operational investment (requires ≥ 5 postings)
- **Words vs Money** (15%) — Stated financial commitments cross-checked against actual capex/opex line items
- **Supply Chain** (10%) — Cross-cutting stream derived from the supply chain management disclosure grade where the industry defines this as a material factor; marked as material but unmeasured otherwise

A stream marked *excluded* means evidence was absent or insufficient — it does not indicate poor performance.
            """
        )


# ---------------------------------------------------------------------------
# Pipeline observability — task progress panel (shown during Airflow run)
# ---------------------------------------------------------------------------

_TASK_DISPLAY_ORDER = [
    "fetch_data",
    "run_talent",
    "run_relevance_filter",
    "run_credibility_scorer",
    "run_dcf_mapper",
    "audit_summary",
]

_TASK_LABELS = {
    "fetch_data": "Data Gathering",
    "run_talent": "Talent Signal",
    "run_relevance_filter": "SASB Relevance Filter",
    "run_credibility_scorer": "Credibility Scorer",
    "run_dcf_mapper": "DCF Mapper",
    "audit_summary": "Audit Summary",
}

_TASK_DESCRIPTION = {
    "fetch_data": "EDGAR / Companies House · PDF · EPA / EA regulators",
    "run_talent": "Indeed RSS · SerpAPI · ghost listing detection",
    "run_relevance_filter": "SASB map · SIC → material factors",
    "run_credibility_scorer": "5 evidence streams · Claude narrative",
    "run_dcf_mapper": "Excel row labels · scenario ranges",
    "audit_summary": "Cost · token · cache summary",
}

_STATE_ICON = {
    "success": '<i class="bi bi-check-circle-fill" style="color:#28a745"></i>',
    "running": '<i class="bi bi-arrow-repeat"       style="color:#007bff"></i>',
    "failed": '<i class="bi bi-x-circle-fill"      style="color:#dc3545"></i>',
    "upstream_failed": '<i class="bi bi-x-circle-fill"      style="color:#dc3545"></i>',
    "queued": '<i class="bi bi-clock"              style="color:#ffc107"></i>',
    "up_for_retry": '<i class="bi bi-arrow-clockwise"    style="color:#fd7e14"></i>',
    "skipped": '<i class="bi bi-skip-forward-fill"  style="color:#6c757d"></i>',
    None: '<i class="bi bi-circle"             style="color:#adb5bd"></i>',
    "none": '<i class="bi bi-circle"             style="color:#adb5bd"></i>',
}

_STATE_COLOUR = {
    "success": "#28a745",
    "running": "#007bff",
    "failed": "#dc3545",
    "upstream_failed": "#dc3545",
    "queued": "#ffc107",
    "up_for_retry": "#fd7e14",
    "skipped": "#6c757d",
    None: "#adb5bd",
    "none": "#adb5bd",
}


def _fmt_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s}s"


def _elapsed_since(start_date_str: Optional[str]) -> str:
    """Return elapsed time since a task started (for running tasks)."""
    if not start_date_str:
        return ""
    try:
        start = datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        elapsed = (now - start).total_seconds()
        return f"{elapsed:.0f}s…"
    except Exception:
        return ""


def _key_output(task_id: str, xcom: Optional[dict]) -> str:
    """Extract a brief human-readable summary from a task's XCom output."""
    if not xcom:
        return ""
    try:
        if task_id == "fetch_data":
            statuses = xcom.get("source_statuses", {})
            parts = []
            for src, status in statuses.items():
                short = "ok" if "success" in status else "fail"
                parts.append(f"{src}: {short}")
            return "  ·  ".join(parts[:4]) + ("  …" if len(parts) > 4 else "")

        if task_id == "run_relevance_filter":
            industry = xcom.get("sasb_industry") or "unknown industry"
            n = len(xcom.get("material_factors") or [])
            return f"{industry} → {n} material factors"

        if task_id == "run_talent":
            postings = xcom.get("total_postings", 0)
            ratio = xcom.get("senior_ratio", 0.0)
            ghosts = xcom.get("ghost_count", 0)
            return f"{postings} postings  ·  {ratio:.0%} senior  ·  {ghosts} ghost flags"

        if task_id == "run_credibility_scorer":
            score = xcom.get("overall_score")
            flag = xcom.get("overall_flag", "")
            n = len(xcom.get("factor_scores") or [])
            if score is not None:
                return f"Overall: {score:.2f} ({flag.upper()})  ·  {n} factors scored"

        if task_id == "run_dcf_mapper":
            if xcom.get("skipped"):
                return "Skipped — no DCF file provided"
            mapped = len(xcom.get("mappings") or [])
            unmapped = len(xcom.get("unmapped_factors") or [])
            return f"{mapped} factors mapped  ·  {unmapped} unmapped"

        if task_id == "audit_summary":
            calls = xcom.get("total_calls", 0)
            cost = xcom.get("actual_cost_usd", 0.0)
            tokens = xcom.get("total_tokens", 0)
            cached = xcom.get("cached_calls", 0)
            return (
                f"{calls} LLM calls  ·  {tokens:,} tokens  ·  "
                f"${cost:.4f} actual cost  ·  {cached} cached"
            )
    except Exception:
        pass
    return ""


def task_progress_panel(
    task_instances: List[dict],
    partial_xcoms: Dict[str, dict],
) -> None:
    """
    Render a live pipeline progress table during Airflow run polling.

    Args:
        task_instances: list of task instance dicts from the Airflow REST API
        partial_xcoms:  dict mapping task_id → XCom return_value dict
                        (populated incrementally as tasks complete)
    """
    state_map: Dict[str, dict] = {t["task_id"]: t for t in task_instances}

    st.markdown("**Pipeline Progress**")

    for task_id in _TASK_DISPLAY_ORDER:
        ti = state_map.get(task_id, {})
        state = (ti.get("state") or "none").lower()
        icon = _STATE_ICON.get(state, '<i class="bi bi-circle" style="color:#adb5bd"></i>')
        colour = _STATE_COLOUR.get(state, "#adb5bd")
        label = _TASK_LABELS.get(task_id, task_id)
        desc = _TASK_DESCRIPTION.get(task_id, "")

        # Duration
        if state == "success":
            duration = _fmt_duration(ti.get("duration"))
        elif state in ("running", "queued"):
            duration = _elapsed_since(ti.get("start_date"))
        else:
            duration = "—"

        # Key output summary
        output_line = _key_output(task_id, partial_xcoms.get(task_id))

        col_icon, col_main, col_dur = st.columns([1, 7, 2])
        with col_icon:
            st.markdown(
                f'<div style="font-size:1.4rem;text-align:center;padding-top:4px">{icon}</div>',
                unsafe_allow_html=True,
            )
        with col_main:
            st.markdown(
                f'<span style="font-weight:700;color:{colour}">{label}</span>'
                f'<span style="color:#6c757d;font-size:0.82rem;margin-left:8px">{desc}</span>',
                unsafe_allow_html=True,
            )
            if output_line:
                st.caption(output_line)
        with col_dur:
            st.markdown(
                f'<div style="text-align:right;color:#6c757d;font-size:0.82rem;'
                f'padding-top:6px">{duration}</div>',
                unsafe_allow_html=True,
            )

    st.markdown("")


# ---------------------------------------------------------------------------
# Pipeline trace panel — shown in results to explain provenance
# ---------------------------------------------------------------------------


def pipeline_trace_panel(
    fetch_result: Optional[dict],
    relevance_dict: Optional[dict],
    talent_dict: Optional[dict],
    audit_dict: Optional[dict],
) -> None:
    """
    Render an expandable data-provenance section after a successful run.

    Shows what each pipeline stage fetched and produced so the analyst can
    verify the data trail behind the credibility scores.
    """
    with st.expander("Pipeline Trace — data provenance", expanded=False):

        # --- Data Gathering ---
        st.markdown("**1. Data Gathering**")
        if fetch_result:
            statuses = fetch_result.get("source_statuses", {})
            reg_paths = fetch_result.get("regulatory_paths", {})
            if statuses:
                rows = []
                for src, status in statuses.items():
                    ok = "success" in status
                    rows.append(
                        {
                            "Source": src,
                            "Status": "[ok] " + status if ok else "[fail] " + status,
                        }
                    )
                st.dataframe(rows, use_container_width=True, hide_index=True)
            if reg_paths:
                st.caption(
                    "Regulatory files: " + ", ".join(f"{k} → `{v}`" for k, v in reg_paths.items())
                )
        else:
            st.caption("No data gathering result available.")

        st.markdown("---")

        # --- SASB Relevance Filter ---
        st.markdown("**2. SASB Relevance Filter**")
        if relevance_dict:
            industry = relevance_dict.get("sasb_industry") or "No match — fallback factors used"
            sic = relevance_dict.get("sic_code") or "—"
            factors = relevance_dict.get("material_factors") or []
            col1, col2 = st.columns(2)
            col1.metric("SASB Industry", industry)
            col2.metric("SIC Code", sic)
            if factors:
                st.markdown(f"**{len(factors)} material factors:**")
                factor_names = [f.get("name", f.get("factor_id", "?")) for f in factors]
                st.caption("  ·  ".join(factor_names))
            errors = relevance_dict.get("errors") or []
            for e in errors:
                st.warning(e)
        else:
            st.caption("No relevance filter result available.")

        st.markdown("---")

        # --- Talent Signal ---
        st.markdown("**3. Talent Signal**")
        if talent_dict:
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Postings", talent_dict.get("total_postings", 0))
            col2.metric(
                "Senior Ratio",
                f"{talent_dict.get('senior_ratio', 0.0):.0%}",
            )
            col3.metric("Ghost Flags", talent_dict.get("ghost_count", 0))
            factor_scores = talent_dict.get("factor_scores") or {}
            if factor_scores:
                st.caption(
                    "Factor scores: "
                    + "  ·  ".join(f"{k}: {v:.2f}" for k, v in factor_scores.items())
                )
            talent_errors = talent_dict.get("errors") or []
            for e in talent_errors:
                st.markdown(
                    f'{_BI["warning"]} <span style="font-size:0.82rem;color:#856404">{e}</span>',
                    unsafe_allow_html=True,
                )
        else:
            st.caption("No talent signal result available.")

        st.markdown("---")

        # --- Audit Summary ---
        st.markdown("**4. Audit Summary**")
        if audit_dict:
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("LLM Calls", audit_dict.get("total_calls", 0))
            col2.metric("Total Tokens", f"{audit_dict.get('total_tokens', 0):,}")
            col3.metric("Actual Cost", f"${audit_dict.get('actual_cost_usd', 0.0):.4f}")
            col4.metric("Cached Calls", audit_dict.get("cached_calls", 0))
            models = audit_dict.get("models_used") or []
            if models:
                st.caption("Models used: " + ", ".join(models))
        else:
            st.caption("Audit summary not available.")
