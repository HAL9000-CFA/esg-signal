"""
Words vs Money — issue #15.

For each material ESG factor, extracts stated financial commitments from the
sustainability report text (e.g. "invest £500 M in renewables by 2030") and
cross-checks them against actual capex / opex line items in raw_financials.

Two-stage pipeline
------------------
1. Commitment extraction  — one Claude call per factor, returns structured JSON.
2. Financial comparison   — pure deterministic Python arithmetic, no LLM.

Per-commitment verdict
----------------------
    consistent    annualised stated amount ≤ 1.5× annual capex/opex line item
    plausible     annualised amount ≤ 3× annual capex/opex  (ambitious but feasible)
    gap           annualised amount  > 3× annual capex/opex  (material credibility gap)
    unverifiable  no monetary figure stated, or no matching financial line item available

Stream score: mean of per-commitment scores (neutral 0.50 if none found).

Score mapping:
    consistent    → 0.80
    plausible     → 0.60
    gap           → 0.10
    unverifiable  → 0.50

Public API
----------
    result = check(profile, factor, run_id=None)
    # result.score  — float 0.0–1.0
    # result.commitment_checks  — List[CommitmentCheck]
    # result.errors  — List[str]
"""

import json
import logging
from textwrap import dedent
from typing import Dict, List, Optional, Tuple

from pipeline.llm_client import call_claude
from pipeline.models import CommitmentCheck, CompanyProfile, MaterialFactor, WordsMoneyResult

LOGGER = logging.getLogger(__name__)

_MODEL = "claude-opus-4-5"
_AGENT = "words_vs_money"

# ---------------------------------------------------------------------------
# Claude extraction prompt
# ---------------------------------------------------------------------------

_EXTRACTION_SYSTEM = """You are a financial analyst extracting ESG investment commitments from annual and sustainability reports.

Extract every explicit monetary commitment related to the given ESG factor.
Return a JSON array. Each element must have exactly these fields:
  "text"     - verbatim quote (≤200 chars) of the commitment
  "amount"   - numeric value of the stated sum as a plain number (null if not stated)
  "currency" - ISO 4217 code: USD, GBP, EUR, etc. (null if unknown)
  "year"     - target year as integer, e.g. 2030 (null if not stated)
  "category" - one of: capex, opex, reduction_target, other

Rules:
- Only include commitments with a specific monetary figure OR a specific percentage reduction target
- Do not paraphrase — quote the source text directly
- Expand abbreviations: "£500m" → amount=500000000, currency="GBP"
- If no relevant commitments exist for this factor, return []
- Return valid JSON only, no prose, no markdown fences"""

# ---------------------------------------------------------------------------
# Score map and financial key routing
# ---------------------------------------------------------------------------

_FLAG_SCORES: Dict[str, float] = {
    "consistent": 0.80,
    "plausible": 0.60,
    "gap": 0.10,
    "unverifiable": 0.50,
}

# Which raw_financials keys are relevant for each commitment category.
# Tried in order; first non-None value is used.
_CATEGORY_TO_FINANCIALS: Dict[str, List[str]] = {
    "capex": ["capex", "total_assets", "revenue"],
    "opex": ["total_opex", "revenue"],
    "reduction_target": ["capex", "total_opex", "revenue"],
    "other": ["capex", "total_opex", "revenue"],
}

# Report text is truncated to this length to keep the Claude call affordable.
# Sustainability reports can exceed 200 K chars; the most relevant sections
# are typically in the first 80 K chars.
_MAX_REPORT_CHARS = 80_000

# Current base year for annualising multi-year commitments.
_BASE_YEAR = 2026

# Annualised amount thresholds relative to the chosen financial line item.
# Ratios above GAP_THRESHOLD flag a material credibility gap.
_GAP_THRESHOLD = 3.0
_CONSISTENT_THRESHOLD = 1.5


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def check(
    profile: CompanyProfile,
    factor: MaterialFactor,
    run_id: Optional[str] = None,
) -> WordsMoneyResult:
    """
    Run the Words vs Money check for one ESG factor.

    Args:
        profile:  CompanyProfile from DataGatherer — must include annual_report_text
                  and raw_financials (capex, total_opex, revenue keys expected).
        factor:   The material ESG factor being checked.
        run_id:   Airflow run ID forwarded to the audit log (optional).

    Returns:
        WordsMoneyResult with score (0.0–1.0), per-commitment checks, and errors.
    """
    errors: List[str] = []
    report_text = (profile.annual_report_text or "").strip()
    raw_financials = profile.raw_financials or {}

    if not report_text:
        errors.append("annual_report_text is empty — cannot extract commitments")
        return WordsMoneyResult(
            ticker=profile.ticker,
            factor_id=factor.factor_id,
            commitment_checks=[],
            score=0.5,
            errors=errors,
        )

    # Step 1: Claude extracts structured commitments from the report text.
    raw_commitments, extract_errors = _extract_commitments(
        report_text=report_text,
        factor=factor,
        company=profile.name,
        run_id=run_id,
    )
    errors.extend(extract_errors)

    if raw_commitments is None:
        # Extraction failed entirely — return neutral.
        return WordsMoneyResult(
            ticker=profile.ticker,
            factor_id=factor.factor_id,
            commitment_checks=[],
            score=0.5,
            errors=errors,
        )

    if not raw_commitments:
        # No commitments found for this factor — neutral, not a gap.
        return WordsMoneyResult(
            ticker=profile.ticker,
            factor_id=factor.factor_id,
            commitment_checks=[],
            score=0.5,
            errors=errors,
        )

    # Step 2: Pure Python comparison against financial line items.
    checks = _compare(raw_commitments, raw_financials)

    score = _compute_score(checks)

    return WordsMoneyResult(
        ticker=profile.ticker,
        factor_id=factor.factor_id,
        commitment_checks=checks,
        score=score,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Step 1 — Claude extraction (structured only, no arithmetic)
# ---------------------------------------------------------------------------


def _extract_commitments(
    report_text: str,
    factor: MaterialFactor,
    company: str,
    run_id: Optional[str],
) -> Tuple[Optional[List[dict]], List[str]]:
    """
    Ask Claude to extract ESG commitments for one factor from the report text.

    Returns (commitments_list, errors).  commitments_list is None if the call
    fails entirely, [] if the report contains no relevant commitments.
    """
    truncated = report_text[:_MAX_REPORT_CHARS]
    prompt = dedent(
        f"""
        Company: {company}
        ESG factor: {factor.name} ({factor.dimension})

        Sustainability report text (may be truncated):
        ---
        {truncated}
        ---

        Extract all monetary ESG commitments related to "{factor.name}".
        Return a JSON array as described in your instructions.
    """
    ).strip()

    try:
        raw = call_claude(
            agent=_AGENT,
            model=_MODEL,
            version=_MODEL,
            purpose=f"Commitment extraction: {factor.name}",
            system=_EXTRACTION_SYSTEM,
            prompt=prompt,
            max_tokens=1024,
            temperature=0.0,
            run_id=run_id,
        )
    except Exception as exc:
        LOGGER.warning("Claude extraction failed for %s: %s", factor.name, exc)
        return None, [f"Commitment extraction failed: {exc}"]

    # Parse the JSON response.
    try:
        parsed = json.loads(raw.strip())
        if not isinstance(parsed, list):
            raise ValueError("Expected a JSON array")
        return parsed, []
    except (json.JSONDecodeError, ValueError) as exc:
        LOGGER.warning("Could not parse extraction response for %s: %s", factor.name, exc)
        return None, [f"Could not parse commitment extraction response: {exc}"]


# ---------------------------------------------------------------------------
# Step 2 — Pure Python comparison (no LLM)
# ---------------------------------------------------------------------------


def _compare(
    raw_commitments: List[dict],
    raw_financials: Dict,
) -> List[CommitmentCheck]:
    """
    Compare each extracted commitment against financial line items.

    All arithmetic is deterministic Python — no LLM involvement.
    """
    checks: List[CommitmentCheck] = []

    for item in raw_commitments:
        amount = _safe_float(item.get("amount"))
        currency = item.get("currency") or "UNKNOWN"
        category = item.get("category") or "other"
        year = _safe_int(item.get("year"))
        text = str(item.get("text") or "")[:200]

        if amount is None:
            checks.append(
                CommitmentCheck(
                    commitment_text=text,
                    claimed_amount=None,
                    currency=currency,
                    horizon_year=year,
                    category=category,
                    financials_label=None,
                    financials_value=None,
                    flag="unverifiable",
                    notes="No monetary amount stated — cannot verify against financials",
                )
            )
            continue

        # Find the best matching financial line item for this category.
        fin_key, fin_val = _find_financial(category, raw_financials)

        if fin_val is None or fin_val <= 0:
            checks.append(
                CommitmentCheck(
                    commitment_text=text,
                    claimed_amount=amount,
                    currency=currency,
                    horizon_year=year,
                    category=category,
                    financials_label=fin_key,
                    financials_value=fin_val,
                    flag="unverifiable",
                    notes=(
                        f"Stated {currency} {amount:,.0f} — "
                        f"no matching financial line item available"
                    ),
                )
            )
            continue

        # Annualise the stated commitment over its horizon.
        # Many ESG commitments are cumulative multi-year figures (e.g. "by 2030").
        # We spread them over the remaining years so the comparison against the
        # annual financial figure is fair.
        annualised = _annualise(amount, year)

        ratio = annualised / fin_val

        if ratio > _GAP_THRESHOLD:
            flag = "gap"
            notes = (
                f"Stated {currency} {amount:,.0f}"
                + (f" by {year}" if year else "")
                + f" → annualised {currency} {annualised:,.0f}/yr"
                + f" vs {fin_key} {fin_val:,.0f} (ratio {ratio:.1f}× — material gap)"
            )
        elif ratio > _CONSISTENT_THRESHOLD:
            flag = "plausible"
            notes = (
                f"Stated {currency} {amount:,.0f}"
                + (f" by {year}" if year else "")
                + f" → annualised {currency} {annualised:,.0f}/yr"
                + f" vs {fin_key} {fin_val:,.0f} (ratio {ratio:.1f}× — ambitious but plausible)"
            )
        else:
            flag = "consistent"
            notes = (
                f"Stated {currency} {amount:,.0f}"
                + (f" by {year}" if year else "")
                + f" → annualised {currency} {annualised:,.0f}/yr"
                + f" vs {fin_key} {fin_val:,.0f} (ratio {ratio:.2f}× — consistent)"
            )

        checks.append(
            CommitmentCheck(
                commitment_text=text,
                claimed_amount=amount,
                currency=currency,
                horizon_year=year,
                category=category,
                financials_label=fin_key,
                financials_value=fin_val,
                flag=flag,
                notes=notes,
            )
        )

    return checks


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_financial(category: str, raw_financials: Dict) -> Tuple[Optional[str], Optional[float]]:
    """Return (key, value) for the first non-None financial line item relevant to category."""
    keys = _CATEGORY_TO_FINANCIALS.get(category, ["capex", "total_opex", "revenue"])
    for key in keys:
        val = _safe_float(raw_financials.get(key))
        if val is not None and val > 0:
            return key, val
    return None, None


def _annualise(amount: float, horizon_year: Optional[int]) -> float:
    """
    Spread a total commitment over its remaining horizon to get an annual figure.

    If no year is given, assume a 5-year horizon (common ESG pledge window).
    Minimum divisor of 1 prevents negative or zero division.
    """
    if horizon_year is None:
        years = 5
    else:
        years = max(1, horizon_year - _BASE_YEAR)
    return amount / years


def _compute_score(checks: List[CommitmentCheck]) -> float:
    """Mean of per-commitment flag scores, rounded to 4 dp."""
    if not checks:
        return 0.5
    total = sum(_FLAG_SCORES.get(c.flag, 0.5) for c in checks)
    return round(total / len(checks), 4)


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Convenience: evidence string for credibility scorer
# ---------------------------------------------------------------------------


def evidence_string(factor_name: str, result: WordsMoneyResult) -> str:
    """
    Produce a single human-readable evidence string summarising the result,
    suitable for inclusion in FactorScore.evidence.
    """
    if not result.commitment_checks:
        return f"Words vs Money ({factor_name}): no monetary commitments found — neutral"

    flags = [c.flag for c in result.commitment_checks]
    counts = {f: flags.count(f) for f in _FLAG_SCORES}
    parts = [f"{v} {k}" for k, v in counts.items() if v > 0]
    summary = ", ".join(parts)
    return f"Words vs Money ({factor_name}): {len(flags)} commitment(s) checked — {summary}"
