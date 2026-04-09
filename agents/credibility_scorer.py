"""
Credibility Scorer — issue #11.

Aggregates five independent evidence streams for each SASB-material ESG factor
and produces a per-factor Commitment Credibility Score (0.0–1.0) with a
traffic-light confidence flag (green / amber / red).

Evidence streams and weights:
    disclosure    0.30  — disclosure quality via Claude (QUANTIFIED/VAGUE/UNDISCLOSED)
    regulatory    0.25  — mandatory disclosures: EPA GHGRP/ECHO/NRC, EA, EU ETS
    talent        0.20  — hiring signal from TalentSignal (issue #9)
    words_money   0.15  — capex/opex cross-check (stub — issue #12 not yet built)
    supply_chain  0.10  — supplier exposure (stub — no issue assigned yet)

Rules:
    - All numerical aggregation is deterministic Python — no LLM arithmetic
    - Claude is called once per factor for qualitative narrative synthesis only
    - Every FactorScore includes its primary source URLs for auditability
    - Stubbed streams return 0.5 (neutral) and note their pending status

Flag thresholds (coverage-adjusted):
    green  score >= 0.80 AND coverage >= 0.75 (3+ of 4 scoreable streams)
    amber  score >= 0.40 (or green score with insufficient coverage)
    red     < 0.40
"""

import logging
from pathlib import Path
from textwrap import dedent
from typing import Dict, List, Optional

from agents.disclosure_checker import grade_all_factors
from pipeline.llm_client import call_claude
from pipeline.models import (
    CommitmentCheck,
    CompanyProfile,
    CredibilityReport,
    FactorScore,
    MaterialFactor,
    RelevanceFilterResult,
    TalentSignalResult,
)
from pipeline.words_vs_money import check_all as words_money_check_all
from pipeline.words_vs_money import evidence_string as words_money_evidence

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stream weights (must sum to 1.0)
# ---------------------------------------------------------------------------
_WEIGHTS: Dict[str, float] = {
    "disclosure": 0.30,
    "regulatory": 0.25,
    "talent": 0.20,
    "words_money": 0.15,
    "supply_chain": 0.10,
}

# ---------------------------------------------------------------------------
# Disclosure grade → numeric score
# ---------------------------------------------------------------------------
_GRADE_SCORES = {
    "QUANTIFIED": 0.85,
    "VAGUE": 0.35,
    "UNDISCLOSED": 0.0,
}

# ---------------------------------------------------------------------------
# Regulatory: which regulatory sources are relevant per factor_id
# ---------------------------------------------------------------------------
_FACTOR_REGULATORY_SOURCES: Dict[str, List[str]] = {
    "ghg_emissions": ["ghgrp", "eu_ets"],
    "air_quality": ["echo", "ea_pollution"],
    "water_management": ["echo", "ea_pollution"],
    "ecological_impacts": ["echo", "ea_pollution"],
    "waste_management": ["echo", "ea_pollution"],
    "critical_incident_risk": ["nrc"],
    "climate_physical_risk": ["eu_ets"],
}

# ---------------------------------------------------------------------------
# Talent: map SASB dimension → TalentSignalResult.factor_scores key
# ---------------------------------------------------------------------------
_DIMENSION_TO_TALENT_KEY: Dict[str, str] = {
    "Environment": "environment",
    "Social Capital": "social",
    "Human Capital": "social",
    "Business Model & Innovation": "governance",
    "Leadership & Governance": "governance",
}

# ---------------------------------------------------------------------------
# Claude config
# ---------------------------------------------------------------------------
_MODEL = "claude-opus-4-5"
_AGENT = "credibility_scorer"
_NARRATIVE_SYSTEM = """You are an ESG credibility analyst writing for a CFA investment audience.

Given a company name, an ESG factor, stream scores, and their confidence levels, write a concise 2–3 sentence credibility narrative.

Rules:
- Calibrate language to confidence: use "indicates" only for high-confidence signals (n≥20 or full data); use "suggests" for medium confidence; use "insufficient evidence to conclude" for low confidence or excluded streams
- Distinguish between "weak signal" (low score, high confidence) and "no signal" (excluded/low n) — these are different claims
- A stream marked N/A or excluded means the evidence is absent, NOT that the company performed poorly
- words_money N/A means no monetary commitment was extracted from the text window — it does NOT mean there is a gap between ambition and financial backing; do not infer or imply a gap from a missing words_money score
- Only flag gaps or contradictions when the evidence actually supports that conclusion
- Do not use bullet points or headers
- Return plain prose only, no JSON, no markdown"""


class CredibilityScorer:
    """
    Aggregates evidence streams into per-factor credibility scores.

    Usage:
        scorer = CredibilityScorer()
        report = scorer.score(
            profile=company_profile,
            relevance_result=relevance_filter_result,
            talent_result=talent_signal_result,      # optional
            regulatory_paths=data_gatherer_result.regulatory_paths,  # optional
            previous_profile=prior_year_profile,     # optional, enables YoY drift
            run_id="airflow_run_123",                # optional
        )
    """

    def score(
        self,
        profile: CompanyProfile,
        relevance_result: RelevanceFilterResult,
        talent_result: Optional[TalentSignalResult] = None,
        regulatory_paths: Optional[Dict[str, str]] = None,
        previous_profile: Optional[CompanyProfile] = None,
        run_id: Optional[str] = None,
    ) -> CredibilityReport:
        """
        Score all material factors and return a CredibilityReport.

        Args:
            profile:           CompanyProfile from DataGatherer (current year)
            relevance_result:  RelevanceFilterResult from RelevanceFilter
            talent_result:     TalentSignalResult from TalentSignal (optional)
            regulatory_paths:  Dict mapping source name to processed CSV path
            previous_profile:  Prior-year CompanyProfile for drift detection (optional)
            run_id:            Airflow run ID for audit log grouping (optional)
        """
        errors: List[str] = []
        regulatory_paths = regulatory_paths or {}
        report_text = profile.annual_report_text or ""

        # --- Stream 1: Disclosure quality (single batched Claude call) ---
        factors = relevance_result.material_factors
        if report_text and factors:
            grades = grade_all_factors(
                report_text=report_text,
                factors=factors,
                company=profile.name,
                run_id=run_id,
            )
            grade_map = {g["factor"]: g for g in grades}
        else:
            grade_map = {}
            if not report_text:
                errors.append("annual_report_text is empty — disclosure stream scoring zero")

        # --- Stream 4: Words vs Money (single chunked pass across all factors) ---
        if report_text and factors:
            try:
                wm_map = words_money_check_all(
                    profile=profile,
                    factors=factors,
                    run_id=run_id,
                )
            except Exception as exc:
                LOGGER.warning("words_money_check_all failed: %s — WvM stream excluded", exc)
                wm_map = {}
        else:
            wm_map = {}

        # --- Stream 5: Supply chain — derive from the supply chain factor's disclosure grade ---
        # If the industry has a supply chain management factor, its disclosure quality
        # (QUANTIFIED / VAGUE / UNDISCLOSED) is used as a cross-cutting stream score
        # applied to all factors.  No additional LLM calls — the grade is already in grade_map.
        _SC_GRADE_SCORES = {"QUANTIFIED": 0.85, "VAGUE": 0.50, "UNDISCLOSED": 0.15}
        _SC_FACTOR_NAMES = {"Supply Chain Management", "supply_chain_management"}
        sc_grade = next(
            (grade_map[name]["grade"] for name in _SC_FACTOR_NAMES if name in grade_map),
            None,
        )
        supply_chain_stream_score = _SC_GRADE_SCORES.get(sc_grade) if sc_grade else None

        factor_scores: List[FactorScore] = []
        for factor in factors:
            fs = self._score_factor(
                factor=factor,
                profile=profile,
                grade_map=grade_map,
                wm_map=wm_map,
                talent_result=talent_result,
                regulatory_paths=regulatory_paths,
                supply_chain_score=supply_chain_stream_score,
                run_id=run_id,
            )
            factor_scores.append(fs)

        overall = (
            sum(fs.score for fs in factor_scores) / len(factor_scores) if factor_scores else 0.0
        )
        mean_coverage = (
            sum(fs.coverage for fs in factor_scores) / len(factor_scores) if factor_scores else 0.0
        )

        return CredibilityReport(
            ticker=profile.ticker,
            company_name=profile.name,
            sasb_industry=relevance_result.sasb_industry,
            factor_scores=factor_scores,
            overall_score=round(overall, 4),
            overall_flag=_flag(overall, mean_coverage),
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Per-factor scoring
    # ------------------------------------------------------------------

    def _score_factor(
        self,
        factor: MaterialFactor,
        profile: CompanyProfile,
        grade_map: Dict,
        wm_map: Dict,
        talent_result: Optional[TalentSignalResult],
        regulatory_paths: Dict[str, str],
        supply_chain_score: Optional[float] = None,
        run_id: Optional[str] = None,
    ) -> FactorScore:
        evidence: List[str] = []
        stream_scores: Dict[str, float] = {}

        # Stream 1: Disclosure
        disc_score, disc_evidence = self._score_disclosure(factor, grade_map)
        stream_scores["disclosure"] = disc_score
        evidence.append(disc_evidence)

        # Stream 2: Regulatory
        reg_score, reg_evidence = self._score_regulatory(factor, regulatory_paths)
        stream_scores["regulatory"] = reg_score
        evidence.append(reg_evidence)

        # Stream 3: Talent — returns (score, evidence, talent_confidence)
        tal_score, tal_evidence, tal_confidence = self._score_talent(factor, talent_result)
        stream_scores["talent"] = tal_score
        evidence.append(tal_evidence)

        # Stream 4: Words vs Money — capex/opex vs stated ESG commitments
        wm_score, wm_evidence, wm_checks = self._score_words_money(factor=factor, wm_map=wm_map)
        stream_scores["words_money"] = wm_score
        evidence.append(wm_evidence)

        # Stream 5: Supply chain — disclosure quality of the supply chain factor (cross-cutting).
        # Derived from the supply chain management factor's disclosure grade (already in grade_map).
        # No additional LLM calls. None when the industry has no supply chain management factor.
        stream_scores["supply_chain"] = supply_chain_score  # type: ignore[assignment]
        _sc_grade_label = {0.85: "QUANTIFIED", 0.50: "VAGUE", 0.15: "UNDISCLOSED"}
        if supply_chain_score is not None:
            evidence.append(
                f"Supply chain: disclosure-quality signal "
                f"({_sc_grade_label.get(supply_chain_score, '?')}) — "
                "derived from supply chain management factor grade"
            )
        else:
            evidence.append(
                "Supply chain: material but unmeasured — stream not yet implemented for this industry"
            )

        # Weighted aggregate — skip streams with no data (None) and re-normalise weights
        active = {k: v for k, v in stream_scores.items() if v is not None}
        active_weight_total = sum(_WEIGHTS[k] for k in active)
        weighted = (
            sum(_WEIGHTS[k] * v for k, v in active.items()) / active_weight_total
            if active_weight_total > 0
            else 0.0
        )
        raw_score = round(min(1.0, max(0.0, weighted)), 4)

        # Coverage: structural — fraction of scoreable streams that returned a score.
        # Supply chain counts when the industry has a supply chain factor; excluded otherwise.
        scoreable = {
            k for k in stream_scores if k != "supply_chain" or supply_chain_score is not None
        }
        coverage = round(
            len([k for k in scoreable if stream_scores[k] is not None]) / len(scoreable), 2
        )

        # Damp score toward 0.5 proportional to lack of coverage.
        # Thin evidence should pull the score to neutral, not toward bad.
        # At coverage=1.0: no damp. At coverage=0.25: blends 75% toward 0.5.
        score = round(0.5 * (1.0 - coverage) + raw_score * coverage, 4)

        # Confidence: quality-weighted — accounts for evidence strength within active streams.
        # A stream scoring 0.5 because no commitments were found (WvM) or sample is small
        # (talent) should contribute less weight than a stream with rich, verifiable data.
        #
        # Per-stream quality weights (0.0–1.0):
        #   disclosure  — scales with score: UNDISCLOSED (0.0) → 0.10, QUANTIFIED (0.85) → 1.0
        #   regulatory  — always 1.0 (objective external data source)
        #   talent      — equals the n-based confidence computed in _score_talent
        #   words_money — fraction of commitment checks that are verifiable (not "unverifiable");
        #                 0.5 if stream ran but produced only unverifiable checks
        stream_quality: Dict[str, float] = {}
        if stream_scores.get("disclosure") is not None:
            stream_quality["disclosure"] = min(1.0, 0.10 + stream_scores["disclosure"])
        if stream_scores.get("regulatory") is not None:
            stream_quality["regulatory"] = 1.0
        if stream_scores.get("talent") is not None:
            stream_quality["talent"] = tal_confidence
        if stream_scores.get("words_money") is not None:
            if wm_checks:
                verifiable = sum(1 for c in wm_checks if c.flag != "unverifiable")
                stream_quality["words_money"] = max(0.3, verifiable / len(wm_checks))
            else:
                stream_quality["words_money"] = 0.5  # ran but neutral

        quality_weight_total = sum(_WEIGHTS[k] for k in stream_quality)
        raw_confidence = (
            sum(_WEIGHTS[k] * stream_quality[k] for k in stream_quality) / quality_weight_total
            if quality_weight_total > 0
            else 0.0
        )
        # Damp confidence by coverage: high-quality signals from few streams should not
        # imply high overall confidence. Full confidence (1.0) requires all streams active.
        # At coverage < 1.0, cap the damp at 0.90 so even 3-stream cases stay below certainty.
        coverage_damp = min(0.90 if coverage < 1.0 else 1.0, coverage / 0.75)
        confidence = round(raw_confidence * coverage_damp, 2)

        flag = _flag(score, coverage)

        # Claude narrative (qualitative synthesis only — no arithmetic)
        narrative = self._generate_narrative(
            company=profile.name,
            factor=factor,
            stream_scores=stream_scores,
            evidence=evidence,
            score=score,
            flag=flag,
            coverage=coverage,
            run_id=run_id,
        )

        return FactorScore(
            factor_id=factor.factor_id,
            factor_name=factor.name,
            score=score,
            flag=flag,
            coverage=coverage,
            confidence=confidence,
            stream_scores=stream_scores,
            evidence=evidence,
            sources=list(profile.source_urls),
            narrative=narrative,
            words_money_checks=wm_checks,
        )

    # ------------------------------------------------------------------
    # Individual stream scorers
    # ------------------------------------------------------------------

    def _score_disclosure(self, factor: MaterialFactor, grade_map: Dict) -> tuple[float, str]:
        grade_data = grade_map.get(factor.name)
        if not grade_data:
            return 0.0, f"Disclosure ({factor.name}): no report text available"
        grade = grade_data.get("grade", "UNDISCLOSED")
        score = _GRADE_SCORES.get(grade, 0.0)
        quote = grade_data.get("evidence") or ""
        note = f"Disclosure ({factor.name}): {grade}"
        if quote:
            note += f' — "{quote[:120]}"'
        return score, note

    def _score_regulatory(
        self, factor: MaterialFactor, regulatory_paths: Dict[str, str]
    ) -> tuple[float, str]:
        relevant_sources = _FACTOR_REGULATORY_SOURCES.get(factor.factor_id, [])
        if not relevant_sources:
            return (
                None,  # type: ignore[return-value]
                f"Regulatory ({factor.name}): no mandatory disclosure source mapped — excluded",
            )

        try:
            import pandas as pd
        except ImportError:
            return 0.5, f"Regulatory ({factor.name}): pandas not available — neutral"

        found_sources = []
        violation_count = 0
        record_count = 0

        for source in relevant_sources:
            path = regulatory_paths.get(source)
            if not path or not Path(path).exists():
                continue
            try:
                df = pd.read_csv(path)
                record_count += len(df)
                found_sources.append(source)
                # Look for violation/penalty columns
                for col in df.columns:
                    col_lower = col.lower()
                    if any(kw in col_lower for kw in ("violation", "penalty", "enforcement")):
                        non_zero = df[col].fillna(0).astype(str).str.strip().ne("0").sum()
                        violation_count += int(non_zero)
            except Exception as exc:
                LOGGER.warning(f"Could not read regulatory CSV {path}: {exc}")

        if not found_sources:
            # No data from any mapped source — could mean no facilities, wrong mapping, or
            # incomplete dataset.  Exclude rather than penalise; absence is ambiguous.
            return (
                None,  # type: ignore[return-value]
                f"Regulatory ({factor.name}): mapped source(s) {relevant_sources} returned no data — excluded (ambiguous absence)",
            )

        if violation_count == 0:
            score = 0.75
            note = f"Regulatory ({factor.name}): {record_count} records from {found_sources}, no violations detected"
        else:
            penalty = min(0.55, violation_count * 0.1)
            score = max(0.1, 0.75 - penalty)
            note = f"Regulatory ({factor.name}): {violation_count} violation/penalty records from {found_sources}"

        return round(score, 4), note

    def _score_talent(
        self, factor: MaterialFactor, talent_result: Optional[TalentSignalResult]
    ) -> tuple[float, str, float]:
        """Returns (score, evidence_note, talent_confidence).
        talent_confidence is the data-quality weight (0.0–1.0) used for the
        overall confidence metric — distinct from the blended score itself.
        """
        if talent_result is None:
            return None, f"Talent ({factor.name}): no TalentSignalResult provided — excluded", 0.0  # type: ignore[return-value]

        talent_key = _DIMENSION_TO_TALENT_KEY.get(factor.dimension)
        if not talent_key:
            return (
                None,  # type: ignore[return-value]
                f"Talent ({factor.name}): dimension {factor.dimension!r} not mapped — excluded",
                0.0,
            )

        raw = talent_result.factor_scores.get(talent_key, 0.0)
        n = talent_result.total_postings

        # Too few postings to be meaningful — exclude rather than drag score down
        _MIN_POSTINGS = 10
        if n < _MIN_POSTINGS:
            return (
                None,  # type: ignore[return-value]
                f"Talent ({factor.name}): n={n} postings — below minimum ({_MIN_POSTINGS}) for reliable signal, excluded",
                0.0,
            )

        # Confidence: n=5 → 0.25×, n=10 → 0.5×, n=20+ → full weight
        confidence = min(1.0, n / 20.0)

        # Floor for zero-signal cases with meaningful total postings.
        # raw=0.0 with n>=_MIN_POSTINGS means ESG hiring is detected but keyword mapping
        # doesn't cover this specific factor's dimension — "unclassified" not "absent".
        # Apply a neutral floor (0.25) to avoid asserting zero capability with high confidence.
        _ZERO_SIGNAL_FLOOR = 0.25
        if raw == 0.0 and n >= _MIN_POSTINGS:
            raw = _ZERO_SIGNAL_FLOOR

        # Senior ratio adjustment: 0% → ×0.7, 50% → ×1.0, 100% → ×1.15
        senior_adj = 0.70 + (talent_result.senior_ratio * 0.45)

        # Blend adjusted signal toward neutral (0.5) proportional to lack of confidence.
        # Low confidence means "insufficient evidence", not "negative evidence".
        # confidence=0.25 → 75% weight on neutral; confidence=1.0 → full signal
        raw_adjusted = min(1.0, float(raw) * senior_adj)
        blended = round(0.5 * (1.0 - confidence) + raw_adjusted * confidence, 4)

        note = (
            f"Talent ({factor.name}): raw={raw:.2f} → blended={blended:.2f} "
            f"(n={n}, confidence={confidence:.0%}, {talent_result.senior_ratio:.0%} senior, "
            f"{talent_result.ghost_count} ghost flags)"
        )
        return blended, note, confidence

    def _score_words_money(
        self,
        factor: MaterialFactor,
        wm_map: Dict,
    ) -> tuple[float, str, Optional[List[CommitmentCheck]]]:
        """
        Return (score, evidence_string, checks) from the pre-computed WvM map.

        check_all() is called once before the factor loop and the results
        stored in wm_map (factor_id → WordsMoneyResult). This method just
        looks up the pre-computed result so no additional API calls are made.
        """
        result = wm_map.get(factor.factor_id)
        if result is None:
            return (
                None,  # type: ignore[return-value]
                f"Words vs Money ({factor.name}): not computed — excluded",
                None,
            )
        ev = words_money_evidence(factor.name, result)
        return result.score, ev, result.commitment_checks or None  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Claude narrative synthesis
    # ------------------------------------------------------------------

    def _generate_narrative(
        self,
        company: str,
        factor: MaterialFactor,
        stream_scores: Dict[str, float],
        evidence: List[str],
        score: float,
        flag: str,
        coverage: float,
        run_id: Optional[str],
    ) -> str:
        active_count = sum(1 for v in stream_scores.values() if v is not None)
        total_count = len(stream_scores)
        stream_summary = "\n".join(
            f"  {k}: {v:.2f}" if v is not None else f"  {k}: N/A (excluded)"
            for k, v in stream_scores.items()
        )
        evidence_summary = "\n".join(f"  - {e}" for e in evidence)
        prompt = dedent(
            f"""
            Company: {company}
            ESG factor: {factor.name} ({factor.dimension})
            Overall credibility score: {score:.2f} ({flag.upper()})
            Evidence coverage: {active_count}/{total_count} streams active ({coverage:.0%})

            Stream scores:
            {stream_summary}

            Evidence notes (what each score actually means):
            {evidence_summary}

            Write a 2–3 sentence credibility narrative for this factor.
            Coverage is {coverage:.0%} — calibrate confidence accordingly.
        """
        )

        try:
            return call_claude(
                agent=_AGENT,
                model=_MODEL,
                version=_MODEL,
                purpose=f"Narrative synthesis: {factor.name}",
                system=_NARRATIVE_SYSTEM,
                prompt=prompt,
                max_tokens=400,
                temperature=0.2,
                run_id=run_id,
            )
        except Exception as exc:
            LOGGER.warning(f"Claude narrative failed for {factor.name}: {exc}")
            return f"Narrative unavailable: {exc}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _flag(score: float, coverage: float = 1.0) -> str:
    """
    Coverage-adjusted traffic light.

    Green requires both a strong score AND sufficient evidence breadth.
    A high score from only 1–2 streams is capped at amber — the rating
    is provisionally positive, not confirmed.

    Coverage thresholds (out of 4–5 scoreable streams; supply_chain included when the industry has a supply chain management factor):
      >= 0.75  (3+/4) → full confidence
      >= 0.50  (2/4)  → limited evidence, cap at amber
      <  0.50  (0–1)  → very thin evidence, cap at amber
    """
    _COVERAGE_FOR_GREEN = 0.75  # need 3+ of 4 scoreable streams for green
    if score >= 0.80 and coverage >= _COVERAGE_FOR_GREEN:
        return "green"
    if score >= 0.40:
        return "amber"
    return "red"
