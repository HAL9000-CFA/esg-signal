"""
Words vs Money — issue #15.

For each material ESG factor, extracts stated financial commitments from the
sustainability report text (e.g. "invest £500 M in renewables by 2030") and
cross-checks them against actual capex / opex line items in raw_financials.

Two-stage pipeline
------------------
1. Commitment extraction  — one Claude call per chunk (all factors batched),
                            returns structured JSON keyed by factor name.
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
    # Preferred: all factors in one chunked pass
    results = check_all(profile, factors, run_id=None)
    # results is Dict[factor_id → WordsMoneyResult]

    # Single-factor convenience wrapper (delegates to check_all internally)
    result = check(profile, factor, run_id=None)
"""

import json
import logging
import re
from textwrap import dedent
from typing import Dict, List, Optional, Tuple

from pipeline.llm_client import call_claude
from pipeline.models import CommitmentCheck, CompanyProfile, MaterialFactor, WordsMoneyResult

LOGGER = logging.getLogger(__name__)

_MODEL = "claude-opus-4-5"
_AGENT = "words_vs_money"

# ---------------------------------------------------------------------------
# Chunking constants — same strategy as disclosure_checker for consistency
# ---------------------------------------------------------------------------

_CHUNK_SIZE = 20_000  # characters per chunk
_CHUNK_OVERLAP = 2_000  # overlap to avoid cutting commitments mid-sentence

# ---------------------------------------------------------------------------
# Claude extraction prompt — multi-factor batch variant
# ---------------------------------------------------------------------------

_EXTRACTION_SYSTEM = """You are a financial analyst extracting ESG investment commitments from annual and sustainability reports.

You will be given a list of ESG factors and a section of a report. For each factor, extract every explicit monetary commitment present in this section.

Return a single JSON object where each key is an ESG factor name (exactly as given) and the value is an array of commitment objects. If a factor has no relevant commitments in this section, its value must be an empty array [].

Each commitment object must have exactly these fields:
  "text"     - verbatim quote (≤200 chars) of the commitment
  "amount"   - the stated monetary value expressed as a FULL integer or float in base units (no abbreviations — see expansion rules below). null if no monetary figure is stated.
  "currency" - ISO 4217 code: USD, GBP, EUR, etc. If the currency is implicit from the report context (e.g. a UK company's figures are GBP unless stated otherwise), infer it. null only if genuinely indeterminate.
  "year"     - target year as integer, e.g. 2030 (null if not stated)
  "category" - one of: capex, opex, reduction_target, other

Magnitude expansion — ALWAYS expand to full base-unit numbers:
  thousand / k / K           → × 1,000
  million / m / M / mn / mm  → × 1,000,000
  billion / b / B / bn / bln → × 1,000,000,000
  trillion / tr / T / tn     → × 1,000,000,000,000

Examples:
  "£500m"           → amount=500000000,     currency="GBP"
  "$1.2 billion"    → amount=1200000000,    currency="USD"
  "USD 135 million" → amount=135000000,     currency="USD"
  "€2.5bn"          → amount=2500000000,    currency="EUR"
  "13 billion"      → amount=13000000000,   currency=<inferred or null>
  "$800M"           → amount=800000000,     currency="USD"

Rules:
- Only include commitments with an explicit monetary figure (capital spend, budget, investment, cost, penalty, or fund amount) — do NOT include percentage targets, qualitative pledges, or policy statements that lack a monetary value
- Do not paraphrase — quote the source text directly
- A commitment may be relevant to multiple factors — include it under each relevant factor
- Return valid JSON only, no prose, no markdown fences
- Every factor in the input list must appear as a key in the output"""

# ---------------------------------------------------------------------------
# Score map and financial key routing
# ---------------------------------------------------------------------------


def _infer_financials_currency(profile: CompanyProfile) -> Optional[str]:
    """
    Infer the currency of raw_financials from the filing type.
    10-K and 20-F are SEC filings reported in USD.
    AA (Companies House annual accounts) are reported in GBP.
    Returns None if indeterminate.
    """
    filing = profile.latest_annual_filing
    if filing is None:
        return None
    t = (filing.filing_type or "").upper()
    if t in ("10-K", "20-F"):
        return "USD"
    if t == "AA":
        return "GBP"
    return None


def _base_year_from_profile(profile: CompanyProfile) -> int:
    """
    Extract the report year from CompanyProfile.latest_annual_filing.filed_date.
    Falls back to _BASE_YEAR if the date is missing or unparseable.
    """
    filing = profile.latest_annual_filing
    if filing and filing.filed_date:
        try:
            return int(str(filing.filed_date)[:4])
        except (ValueError, IndexError):
            pass
    return _BASE_YEAR


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

# Current base year for annualising multi-year commitments.
_BASE_YEAR = 2026

# Annualised amount thresholds relative to the chosen financial line item.
_GAP_THRESHOLD = 3.0
_CONSISTENT_THRESHOLD = 1.5

# Rough mid-market FX rates for commitment-vs-financials comparison.
# These are approximate and updated manually — not for financial use.
# Wider thresholds (_FX_GAP_THRESHOLD, _FX_CONSISTENT_THRESHOLD) applied
# when FX conversion is used to account for rate uncertainty.
_FX_RATES: Dict[tuple, float] = {
    ("GBP", "USD"): 1.27,
    ("EUR", "USD"): 1.09,
    ("GBP", "EUR"): 1.16,
    ("USD", "GBP"): 0.79,
    ("USD", "EUR"): 0.92,
    ("EUR", "GBP"): 0.86,
}
_FX_GAP_THRESHOLD = 4.0  # wider than normal (3.0) to absorb FX uncertainty
_FX_CONSISTENT_THRESHOLD = 2.0  # wider than normal (1.5)


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def check_all(
    profile: CompanyProfile,
    factors: List[MaterialFactor],
    run_id: Optional[str] = None,
) -> Dict[str, WordsMoneyResult]:
    """
    Run Words vs Money for all factors in one chunked pass.

    Chunks the full annual_report_text and makes one Claude call per chunk
    with all factors batched together. Commitments are merged across chunks
    (deduplicated by text). Each factor gets a separate WordsMoneyResult.

    Args:
        profile:  CompanyProfile — must include annual_report_text and raw_financials.
        factors:  All material factors to check.
        run_id:   Airflow run ID for audit log grouping (optional).

    Returns:
        Dict mapping factor_id → WordsMoneyResult for each factor.
    """
    report_text = (profile.annual_report_text or "").strip()
    raw_financials = profile.raw_financials or {}

    if not report_text:
        LOGGER.warning("check_all: annual_report_text is empty for %s", profile.ticker)
        return {
            f.factor_id: WordsMoneyResult(
                ticker=profile.ticker,
                factor_id=f.factor_id,
                commitment_checks=[],
                score=None,  # type: ignore[arg-type]
                errors=["annual_report_text is empty — cannot extract commitments"],
            )
            for f in factors
        }

    financials_currency = _infer_financials_currency(profile)
    base_year = _base_year_from_profile(profile)

    factor_names = [f.name for f in factors]
    factor_by_name = {f.name: f for f in factors}

    # Accumulated raw commitments per factor name across all chunks.
    # Deduplicated by (factor_name, text) to avoid double-counting the same
    # pledge that appears in the overlap between consecutive chunks.
    seen: Dict[str, set] = {name: set() for name in factor_names}
    accumulated: Dict[str, List[dict]] = {name: [] for name in factor_names}
    chunk_errors: List[str] = []

    chunks = _chunk_text(report_text)
    LOGGER.info(
        "check_all: %s — %d chars → %d chunks, %d factors",
        profile.ticker,
        len(report_text),
        len(chunks),
        len(factors),
    )

    for chunk_idx, chunk in enumerate(chunks):
        chunk_result, errors = _extract_commitments_batch(
            chunk=chunk,
            factors=factors,
            factor_names=factor_names,
            company=profile.name,
            chunk_idx=chunk_idx,
            total_chunks=len(chunks),
            run_id=run_id,
        )
        chunk_errors.extend(errors)

        if chunk_result is None:
            continue

        for name, commitments in chunk_result.items():
            if name not in accumulated:
                continue
            for c in commitments:
                key = str(c.get("text", ""))[:100]
                if key and key not in seen[name]:
                    seen[name].add(key)
                    accumulated[name].append(c)

    # Build a WordsMoneyResult per factor from the merged commitments.
    results: Dict[str, WordsMoneyResult] = {}
    for name, raw_commitments in accumulated.items():
        factor = factor_by_name[name]

        if not raw_commitments:
            results[factor.factor_id] = WordsMoneyResult(
                ticker=profile.ticker,
                factor_id=factor.factor_id,
                commitment_checks=[],
                score=None,  # type: ignore[arg-type]  # excluded, not penalised
                errors=chunk_errors,
            )
            continue

        checks = _compare(raw_commitments, raw_financials, financials_currency, base_year)
        score = _compute_score(checks)
        results[factor.factor_id] = WordsMoneyResult(
            ticker=profile.ticker,
            factor_id=factor.factor_id,
            commitment_checks=checks,
            score=score,
            errors=chunk_errors,
        )

    LOGGER.info(
        "check_all: %s done — %d factors, commitments found: %s",
        profile.ticker,
        len(factors),
        {name: len(accumulated[name]) for name in factor_names},
    )
    return results


def check(
    profile: CompanyProfile,
    factor: MaterialFactor,
    run_id: Optional[str] = None,
) -> WordsMoneyResult:
    """
    Single-factor convenience wrapper. Delegates to check_all().

    Prefer check_all() when scoring multiple factors — it amortises the
    chunking cost across all factors in one pass.
    """
    results = check_all(profile=profile, factors=[factor], run_id=run_id)
    return results[factor.factor_id]


# ---------------------------------------------------------------------------
# Step 1 — Claude extraction (all factors, one chunk at a time)
# ---------------------------------------------------------------------------


def _extract_commitments_batch(
    chunk: str,
    factors: List[MaterialFactor],
    factor_names: List[str],
    company: str,
    chunk_idx: int,
    total_chunks: int,
    run_id: Optional[str],
) -> Tuple[Optional[Dict[str, List[dict]]], List[str]]:
    """
    Ask Claude to extract commitments for all factors from one text chunk.

    Returns (dict_keyed_by_factor_name, errors).
    dict is None if the call fails entirely.
    """

    def _factor_line(f: MaterialFactor) -> str:
        desc = (f.description or "").strip()
        if desc:
            return f"- {f.name}: {desc}"
        return f"- {f.name}"

    factors_list = "\n".join(_factor_line(f) for f in factors)
    prompt = dedent(
        f"""
        Company: {company}
        Report section {chunk_idx + 1} of {total_chunks}:

        ESG factors to extract commitments for:
        {factors_list}

        Report text:
        ---
        {chunk}
        ---

        For each ESG factor, extract all monetary commitments present in this section.
        Return a JSON object keyed by factor name as described in your instructions.
        Every factor listed above must appear as a key in your response.
    """
    ).strip()

    # max_tokens: ~200 tokens per factor covers several commitments per factor per chunk
    max_tokens = max(512, 200 * len(factor_names))

    try:
        raw = call_claude(
            agent=_AGENT,
            model=_MODEL,
            version=_MODEL,
            purpose=f"WvM extraction chunk {chunk_idx + 1}/{total_chunks} ({len(factor_names)} factors)",
            system=_EXTRACTION_SYSTEM,
            prompt=prompt,
            max_tokens=max_tokens,
            temperature=0.0,
            run_id=run_id,
        )
    except Exception as exc:
        LOGGER.warning(
            "check_all: extraction failed chunk %d/%d for %s: %s",
            chunk_idx + 1,
            total_chunks,
            company,
            exc,
        )
        return None, [f"Extraction failed chunk {chunk_idx + 1}: {exc}"]

    try:
        cleaned = raw.strip()
        # Strip markdown code fences that Claude sometimes adds despite instructions
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```[a-zA-Z]*\n?", "", cleaned)
            cleaned = re.sub(r"\n?```$", "", cleaned).strip()
        parsed = json.loads(cleaned)
        if not isinstance(parsed, dict):
            raise ValueError(f"Expected a JSON object, got {type(parsed).__name__}")
        return parsed, []
    except (json.JSONDecodeError, ValueError) as exc:
        LOGGER.warning(
            "check_all: JSON parse error chunk %d/%d for %s: %s — raw: %r",
            chunk_idx + 1,
            total_chunks,
            company,
            exc,
            raw[:200],
        )
        return None, [f"JSON parse error chunk {chunk_idx + 1}: {exc}"]


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------


def _chunk_text(text: str) -> List[str]:
    """Split text into overlapping chunks to avoid cutting commitments mid-sentence."""
    if len(text) <= _CHUNK_SIZE:
        return [text]
    step = _CHUNK_SIZE - _CHUNK_OVERLAP
    return [
        text[i : i + _CHUNK_SIZE] for i in range(0, len(text), step) if text[i : i + _CHUNK_SIZE]
    ]


# ---------------------------------------------------------------------------
# Step 2 — Pure Python comparison (no LLM)
# ---------------------------------------------------------------------------


def _compare(
    raw_commitments: List[dict],
    raw_financials: Dict,
    financials_currency: Optional[str] = None,
    base_year: int = _BASE_YEAR,
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
        fx_rate = None

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

        # Unknown currency — cannot safely compare against financials.
        if currency == "UNKNOWN":
            checks.append(
                CommitmentCheck(
                    commitment_text=text,
                    claimed_amount=amount,
                    currency=currency,
                    horizon_year=year,
                    category=category,
                    financials_label=None,
                    financials_value=None,
                    flag="unverifiable",
                    notes=f"Stated amount {amount:,.0f} — currency unknown, cannot verify against financials",
                )
            )
            continue

        # Currency mismatch — attempt rough FX conversion if rate is known.
        fx_rate = None
        if financials_currency and currency != financials_currency:
            fx_rate = _FX_RATES.get((currency, financials_currency))
            if fx_rate is None:
                checks.append(
                    CommitmentCheck(
                        commitment_text=text,
                        claimed_amount=amount,
                        currency=currency,
                        horizon_year=year,
                        category=category,
                        financials_label=None,
                        financials_value=None,
                        flag="unverifiable",
                        notes=(
                            f"Stated {currency} {amount:,.0f} — currency mismatch: "
                            f"financials are in {financials_currency}, no FX rate available"
                        ),
                    )
                )
                continue
            # Convert commitment to financials currency for comparison
            amount = amount * fx_rate

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
        annualised = _annualise(amount, year, base_year)
        ratio = annualised / fin_val

        gap_thresh = _FX_GAP_THRESHOLD if fx_rate else _GAP_THRESHOLD
        consistent_thresh = _FX_CONSISTENT_THRESHOLD if fx_rate else _CONSISTENT_THRESHOLD
        fx_note = f" (~{financials_currency} via FX ×{fx_rate:.2f})" if fx_rate else ""

        if ratio > gap_thresh:
            flag = "gap"
            notes = (
                f"Stated {currency} {amount:,.0f}{fx_note}"
                + (f" by {year}" if year else "")
                + f" → annualised {financials_currency} {annualised:,.0f}/yr"
                + f" vs {fin_key} {fin_val:,.0f} (ratio {ratio:.1f}× — material gap)"
            )
        elif ratio > consistent_thresh:
            flag = "plausible"
            notes = (
                f"Stated {currency} {amount:,.0f}{fx_note}"
                + (f" by {year}" if year else "")
                + f" → annualised {financials_currency} {annualised:,.0f}/yr"
                + f" vs {fin_key} {fin_val:,.0f} (ratio {ratio:.1f}× — ambitious but plausible)"
            )
        else:
            flag = "consistent"
            notes = (
                f"Stated {currency} {amount:,.0f}{fx_note}"
                + (f" by {year}" if year else "")
                + f" → annualised {financials_currency} {annualised:,.0f}/yr"
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


def _annualise(amount: float, horizon_year: Optional[int], base_year: int = _BASE_YEAR) -> float:
    """
    Spread a total commitment over its remaining horizon to get an annual figure.

    If no year is given, assume a 5-year horizon (common ESG pledge window).
    Minimum divisor of 1 prevents negative or zero division.
    base_year should be the report's filing year (defaults to _BASE_YEAR).
    """
    if horizon_year is None:
        years = 5
    else:
        years = max(1, horizon_year - base_year)
    return amount / years


def _compute_score(checks: List[CommitmentCheck]) -> Optional[float]:
    """
    Mean of per-commitment flag scores, rounded to 4 dp.

    Returns None when the majority of checks are unverifiable — the stream
    provides no real signal in that case and should be excluded from scoring
    rather than contributing a neutral 0.5.
    """
    if not checks:
        return None
    unverifiable_count = sum(1 for c in checks if c.flag == "unverifiable")
    if unverifiable_count > len(checks) / 2:
        return None
    total = sum(_FLAG_SCORES.get(c.flag, 0.5) for c in checks)
    return round(total / len(checks), 4)


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        result = float(val)
        import math

        if math.isnan(result) or math.isinf(result):
            return None
        return result
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
        return f"Words vs Money ({factor_name}): no monetary commitments found in extracted text — excluded (uninformative, not penalised)"

    flags = [c.flag for c in result.commitment_checks]
    counts = {f: flags.count(f) for f in _FLAG_SCORES}
    parts = [f"{v} {k}" for k, v in counts.items() if v > 0]
    summary = ", ".join(parts)
    return f"Words vs Money ({factor_name}): {len(flags)} commitment(s) checked — {summary}"
