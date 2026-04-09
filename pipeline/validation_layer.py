"""
Validation Layer — issue #15.

Acts as a gatekeeper between LLM outputs and final briefing content.  Enforces
the core ethical guarantee of the ESG Signal system: all numerical values in the
output come from Python calculations over structured data, never from LLM
generation.

Directly addresses the hallucination risk identified in the Stage 1 ethical
considerations: "all numerical outputs are calculated in Python from structured
data, the LLM produces only qualitative narrative".

What is validated
-----------------
1. Score range           — every score / stream_score / overall_score ∈ [0.0, 1.0]
2. Flag consistency      — flag must agree with score thresholds (green ≥ 0.65,
                           amber ≥ 0.40, red < 0.40)
3. Valid flag values     — only "green" / "amber" / "red" accepted
4. Source URL format     — every cited URL is a parseable HTTP/HTTPS URL
5. Trusted domain check  — URLs outside known-domain list raise a warning
6. Narrative number scan — score-like decimals in Claude narrative (n ∈ (0, 1)
                           with ≥ 1 decimal place) that cannot be traced back
                           to any computed score are flagged as potential
                           hallucinations
7. Scenario range order  — DCF scenario_low < scenario_mid < scenario_high,
                           all positive

Failure model
-------------
Validation never raises an exception.  It always returns a ValidationResult
containing:
  - a list of ValidationWarning objects (severity: error | warning | info)
  - an adjusted_report — a corrected copy with scores clamped, flags fixed,
    invalid URLs removed, and validation warnings appended to errors[]
  - passed — True only when no severity="error" warnings were raised

When errors are present the overall_flag is downgraded one tier (green → amber,
amber → red) and a plain-English disclosure is appended to errors[].  This
means the briefing is always produced, but with an explicit confidence reduction
and an audit trail.

Public API
----------
    result = ValidationLayer().validate(report, dcf_result=dcf_result)
    if not result.passed:
        for w in result.warnings:
            print(w.severity, w.code, w.message)
    # Use result.adjusted_report for display — never the raw report
"""

import copy
import logging
import re
from dataclasses import dataclass
from typing import Any, List, Optional
from urllib.parse import urlparse

from pipeline.models import CredibilityReport, DcfMapperResult, EsgDcfMapping, FactorScore

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Warning codes
# ---------------------------------------------------------------------------

SCORE_OUT_OF_RANGE = "SCORE_OUT_OF_RANGE"
FLAG_INCONSISTENT = "FLAG_INCONSISTENT_WITH_SCORE"
INVALID_FLAG_VALUE = "INVALID_FLAG_VALUE"
INVALID_SOURCE_URL = "INVALID_SOURCE_URL"
UNTRUSTED_DOMAIN = "UNTRUSTED_SOURCE_DOMAIN"
NARRATIVE_UNTRACED_NUMBER = "NARRATIVE_UNTRACED_NUMBER"
SCENARIO_RANGE_INVALID = "SCENARIO_RANGE_INVALID"

# ---------------------------------------------------------------------------
# Known trusted domains for source URL validation.
# URLs outside this list generate a WARNING, not an error — company own-site
# URLs (e.g. bp.com/sustainability-report.pdf) are legitimate sources.
# ---------------------------------------------------------------------------

_TRUSTED_DOMAINS = frozenset(
    [
        # US regulatory
        "sec.gov",
        "www.sec.gov",
        "efts.sec.gov",
        "epa.gov",
        "www.epa.gov",
        "echo.epa.gov",
        "enviro.epa.gov",
        "ghgdata.epa.gov",
        "nrc.ov",  # NRC event reports
        "nrc.gov",
        "www.nrc.gov",
        # UK regulatory
        "companieshouse.gov.uk",
        "api.companieshouse.gov.uk",
        "find-and-update.company-information.service.gov.uk",
        "environment.data.gov.uk",
        "environment-agency.gov.uk",
        # EU regulatory
        "ec.europa.eu",
        "ets.emissions.europa.eu",
        # Data sources used by the pipeline
        "cdp.net",
        "data.cdp.net",
        "gdeltproject.org",
        "api.gdeltproject.org",
        # Financial data
        "data.sec.gov",
    ]
)

# ---------------------------------------------------------------------------
# Flag thresholds — must match agents/credibility_scorer.py _flag()
# ---------------------------------------------------------------------------

_GREEN_THRESHOLD = 0.65
_AMBER_THRESHOLD = 0.40

# Regex: decimal numbers in the range (0.0, 1.0) exclusive, with at least
# one decimal place.  These look like computed scores and are the highest-risk
# hallucination surface in narrative text.
_SCORE_LIKE_RE = re.compile(r"\b0\.\d{1,4}\b")


# ---------------------------------------------------------------------------
# Output dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ValidationWarning:
    code: str  # machine-readable constant, e.g. SCORE_OUT_OF_RANGE
    severity: str  # "error" | "warning" | "info"
    field: str  # dotted path to the offending field, e.g. "factor_scores[0].score"
    message: str  # human-readable explanation
    value: Any  # the actual offending value


@dataclass
class ValidationResult:
    passed: bool  # True only when no severity="error" warnings exist
    warnings: List[ValidationWarning]
    adjusted_report: CredibilityReport  # corrected copy — always use this for display
    adjusted_dcf: Optional[DcfMapperResult]  # corrected DCF result (None if not provided)

    @property
    def error_count(self) -> int:
        return sum(1 for w in self.warnings if w.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for w in self.warnings if w.severity == "warning")


# ---------------------------------------------------------------------------
# ValidationLayer
# ---------------------------------------------------------------------------


class ValidationLayer:
    """
    Validates a CredibilityReport and optional DcfMapperResult.

    Usage::

        result = ValidationLayer().validate(report, dcf_result=dcf_result)
        briefing = result.adjusted_report   # always use adjusted, never raw
    """

    def validate(
        self,
        report: CredibilityReport,
        dcf_result: Optional[DcfMapperResult] = None,
    ) -> ValidationResult:
        """
        Validate all numerical and structural invariants of the pipeline output.

        Args:
            report:     CredibilityReport from CredibilityScorer.
            dcf_result: Optional DcfMapperResult from DcfMapper.

        Returns:
            ValidationResult with warnings, adjusted_report, and passed flag.
        """
        warnings: List[ValidationWarning] = []

        # Work on deep copies so originals are never mutated.
        adj_report: CredibilityReport = copy.deepcopy(report)
        adj_dcf: Optional[DcfMapperResult] = copy.deepcopy(dcf_result) if dcf_result else None

        # --- Check 1–3: scores, flags ---
        self._validate_overall_score(adj_report, warnings)
        for i, fs in enumerate(adj_report.factor_scores):
            self._validate_factor_score(fs, i, warnings)

        # --- Check 4–5: source URLs ---
        for i, fs in enumerate(adj_report.factor_scores):
            cleaned_sources = self._validate_sources(
                fs.sources, f"factor_scores[{i}].sources", warnings
            )
            fs.sources = cleaned_sources

        # --- Check 6: narrative number scan ---
        for i, fs in enumerate(adj_report.factor_scores):
            self._validate_narrative(fs, i, warnings)

        # --- Check 7: DCF scenario ranges ---
        if adj_dcf:
            for i, mapping in enumerate(adj_dcf.mappings):
                self._validate_scenario_range(mapping, i, warnings)

        # --- Confidence reduction when errors found ---
        has_errors = any(w.severity == "error" for w in warnings)
        if has_errors:
            adj_report.overall_flag = _downgrade_flag(adj_report.overall_flag)
            adj_report.errors.append(
                f"Validation layer: {sum(1 for w in warnings if w.severity == 'error')} error(s) "
                f"and {sum(1 for w in warnings if w.severity == 'warning')} warning(s) detected. "
                "Overall confidence flag downgraded one tier. See ValidationResult.warnings for detail."
            )

        passed = not has_errors

        return ValidationResult(
            passed=passed,
            warnings=warnings,
            adjusted_report=adj_report,
            adjusted_dcf=adj_dcf,
        )

    # ------------------------------------------------------------------
    # Check 1: overall score
    # ------------------------------------------------------------------

    def _validate_overall_score(
        self, report: CredibilityReport, warnings: List[ValidationWarning]
    ) -> None:
        score = report.overall_score
        if not _in_unit_interval(score):
            warnings.append(
                ValidationWarning(
                    code=SCORE_OUT_OF_RANGE,
                    severity="error",
                    field="overall_score",
                    message=f"overall_score {score!r} is outside [0.0, 1.0] — clamped",
                    value=score,
                )
            )
            report.overall_score = _clamp(score)

        # Check valid flag value FIRST — invalid values must be corrected before
        # the flag-score consistency check can be meaningful.
        if report.overall_flag not in ("green", "amber", "red"):
            warnings.append(
                ValidationWarning(
                    code=INVALID_FLAG_VALUE,
                    severity="error",
                    field="overall_flag",
                    message=f"overall_flag {report.overall_flag!r} is not green/amber/red",
                    value=report.overall_flag,
                )
            )
            report.overall_flag = _expected_flag(report.overall_score)

        flag_issue = _flag_mismatch(report.overall_score, report.overall_flag)
        if flag_issue:
            warnings.append(
                ValidationWarning(
                    code=FLAG_INCONSISTENT,
                    severity="error",
                    field="overall_flag",
                    message=flag_issue,
                    value=report.overall_flag,
                )
            )
            report.overall_flag = _expected_flag(report.overall_score)

    # ------------------------------------------------------------------
    # Check 2–3: per-factor scores and flags
    # ------------------------------------------------------------------

    def _validate_factor_score(
        self, fs: FactorScore, index: int, warnings: List[ValidationWarning]
    ) -> None:
        prefix = f"factor_scores[{index}]"

        # Overall factor score
        if not _in_unit_interval(fs.score):
            warnings.append(
                ValidationWarning(
                    code=SCORE_OUT_OF_RANGE,
                    severity="error",
                    field=f"{prefix}.score",
                    message=(
                        f"factor '{fs.factor_id}' score {fs.score!r} "
                        "is outside [0.0, 1.0] — clamped"
                    ),
                    value=fs.score,
                )
            )
            fs.score = _clamp(fs.score)

        # Flag value — check before consistency so invalid values are corrected first
        if fs.flag not in ("green", "amber", "red"):
            warnings.append(
                ValidationWarning(
                    code=INVALID_FLAG_VALUE,
                    severity="error",
                    field=f"{prefix}.flag",
                    message=f"factor '{fs.factor_id}' flag {fs.flag!r} is not green/amber/red",
                    value=fs.flag,
                )
            )
            fs.flag = _expected_flag(fs.score)

        # Flag–score consistency
        flag_issue = _flag_mismatch(fs.score, fs.flag)
        if flag_issue:
            warnings.append(
                ValidationWarning(
                    code=FLAG_INCONSISTENT,
                    severity="error",
                    field=f"{prefix}.flag",
                    message=f"factor '{fs.factor_id}': {flag_issue}",
                    value=fs.flag,
                )
            )
            fs.flag = _expected_flag(fs.score)

        # Stream scores
        for stream_name, stream_score in list(fs.stream_scores.items()):
            if not _in_unit_interval(stream_score):
                warnings.append(
                    ValidationWarning(
                        code=SCORE_OUT_OF_RANGE,
                        severity="error",
                        field=f"{prefix}.stream_scores[{stream_name!r}]",
                        message=(
                            f"factor '{fs.factor_id}' stream '{stream_name}' "
                            f"score {stream_score!r} is outside [0.0, 1.0] — clamped"
                        ),
                        value=stream_score,
                    )
                )
                fs.stream_scores[stream_name] = _clamp(stream_score)

    # ------------------------------------------------------------------
    # Check 4–5: source URL validation
    # ------------------------------------------------------------------

    def _validate_sources(
        self, sources: List[str], field_prefix: str, warnings: List[ValidationWarning]
    ) -> List[str]:
        """Validate URLs, remove malformed ones, warn on untrusted domains."""
        cleaned: List[str] = []
        for url in sources:
            if not isinstance(url, str) or not url.strip():
                warnings.append(
                    ValidationWarning(
                        code=INVALID_SOURCE_URL,
                        severity="error",
                        field=field_prefix,
                        message=f"Source URL {url!r} is empty or not a string — removed",
                        value=url,
                    )
                )
                continue

            parsed = urlparse(url)
            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                warnings.append(
                    ValidationWarning(
                        code=INVALID_SOURCE_URL,
                        severity="error",
                        field=field_prefix,
                        message=f"Source URL {url!r} is not a valid HTTP/HTTPS URL — removed",
                        value=url,
                    )
                )
                continue

            # URL is structurally valid — keep it regardless of domain
            cleaned.append(url)

            # Warn (not error) if domain is outside the known-trusted list
            domain = parsed.netloc.lower().lstrip("www.")
            if not any(domain == d or domain.endswith("." + d) for d in _TRUSTED_DOMAINS):
                warnings.append(
                    ValidationWarning(
                        code=UNTRUSTED_DOMAIN,
                        severity="warning",
                        field=field_prefix,
                        message=(
                            f"Source URL domain '{parsed.netloc}' is not in the "
                            "trusted-domain list — manual verification recommended"
                        ),
                        value=url,
                    )
                )

        return cleaned

    # ------------------------------------------------------------------
    # Check 6: narrative number scan
    # ------------------------------------------------------------------

    def _validate_narrative(
        self, fs: FactorScore, index: int, warnings: List[ValidationWarning]
    ) -> None:
        """
        Scan Claude-generated narrative for score-like decimals not traceable
        to computed data.

        A number is considered "traced" if it appears (as a string) in any of:
          - fs.score
          - fs.overall_score (N/A here)
          - fs.stream_scores values
          - any string in fs.evidence

        Numbers from 0.01 to 0.99 (score-like decimals) that are NOT traced
        are flagged as potential hallucinations.

        Integers and percentages > 1 are not checked — too many false positives
        from legitimate narrative content (e.g. "the company employs 50,000 people").
        """
        narrative = fs.narrative or ""
        if not narrative:
            return

        # Build set of known-good score strings from the deterministic layer
        known: set = set()
        known.add(f"{fs.score:.4f}")
        known.add(f"{fs.score:.2f}")
        known.add(str(round(fs.score, 2)))
        for v in fs.stream_scores.values():
            known.add(f"{v:.4f}")
            known.add(f"{v:.2f}")
            known.add(str(round(v, 2)))
        # Include any decimal numbers appearing in evidence strings (from Python)
        for ev in fs.evidence:
            for m in _SCORE_LIKE_RE.findall(ev):
                known.add(m)

        # Find all score-like numbers in the narrative
        candidates = _SCORE_LIKE_RE.findall(narrative)
        untraced = [n for n in candidates if n not in known]

        if untraced:
            warnings.append(
                ValidationWarning(
                    code=NARRATIVE_UNTRACED_NUMBER,
                    severity="warning",
                    field=f"factor_scores[{index}].narrative",
                    message=(
                        f"factor '{fs.factor_id}' narrative contains score-like "
                        f"decimal(s) not traceable to computed data: {untraced}. "
                        "These may be LLM-generated — verify against source data."
                    ),
                    value=untraced,
                )
            )

    # ------------------------------------------------------------------
    # Check 7: DCF scenario range ordering
    # ------------------------------------------------------------------

    def _validate_scenario_range(
        self, mapping: EsgDcfMapping, index: int, warnings: List[ValidationWarning]
    ) -> None:
        prefix = f"dcf_mappings[{index}]({mapping.factor_id})"

        low = mapping.scenario_low
        mid = mapping.scenario_mid
        high = mapping.scenario_high

        # All must be present and positive
        for name, val in [("scenario_low", low), ("scenario_mid", mid), ("scenario_high", high)]:
            if val is None or val <= 0:
                warnings.append(
                    ValidationWarning(
                        code=SCENARIO_RANGE_INVALID,
                        severity="error",
                        field=f"{prefix}.{name}",
                        message=(
                            f"factor '{mapping.factor_id}' {name} is {val!r} "
                            "— must be a positive number"
                        ),
                        value=val,
                    )
                )

        # Ordering: low < mid < high
        if low is not None and mid is not None and low >= mid:
            warnings.append(
                ValidationWarning(
                    code=SCENARIO_RANGE_INVALID,
                    severity="error",
                    field=f"{prefix}.scenario_low/mid",
                    message=(
                        f"factor '{mapping.factor_id}' scenario_low ({low}) "
                        f">= scenario_mid ({mid})"
                    ),
                    value=(low, mid),
                )
            )

        if mid is not None and high is not None and mid >= high:
            warnings.append(
                ValidationWarning(
                    code=SCENARIO_RANGE_INVALID,
                    severity="error",
                    field=f"{prefix}.scenario_mid/high",
                    message=(
                        f"factor '{mapping.factor_id}' scenario_mid ({mid}) "
                        f">= scenario_high ({high})"
                    ),
                    value=(mid, high),
                )
            )


# ---------------------------------------------------------------------------
# Pure-function helpers
# ---------------------------------------------------------------------------


def _in_unit_interval(value) -> bool:
    """Return True if value is a float/int in [0.0, 1.0]."""
    try:
        return 0.0 <= float(value) <= 1.0
    except (TypeError, ValueError):
        return False


def _clamp(value: float) -> float:
    """Clamp to [0.0, 1.0], rounded to 4 dp."""
    return round(max(0.0, min(1.0, float(value))), 4)


def _expected_flag(score: float) -> str:
    """Compute the correct flag for a given score."""
    if score >= _GREEN_THRESHOLD:
        return "green"
    if score >= _AMBER_THRESHOLD:
        return "amber"
    return "red"


def _flag_mismatch(score: float, flag: str) -> Optional[str]:
    """
    Return a human-readable mismatch message if flag is inconsistent with score,
    or None if they agree.
    """
    expected = _expected_flag(score)
    if flag != expected:
        return (
            f"flag is '{flag}' but score {score:.4f} implies '{expected}' "
            f"(green≥{_GREEN_THRESHOLD}, amber≥{_AMBER_THRESHOLD}, red<{_AMBER_THRESHOLD})"
        )
    return None


def _downgrade_flag(flag: str) -> str:
    """Downgrade a flag by one tier: green→amber, amber→red, red→red."""
    return {"green": "amber", "amber": "red", "red": "red"}.get(flag, flag)
