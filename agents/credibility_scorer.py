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

Flag thresholds:
    green  >= 0.65
    amber  >= 0.40
    red     < 0.40
"""

import logging
from pathlib import Path
from textwrap import dedent
from typing import Dict, List, Optional

from agents.disclosure_checker import grade_all_factors
from pipeline.llm_client import call_claude
from pipeline.models import (
    CompanyProfile,
    CredibilityReport,
    FactorScore,
    MaterialFactor,
    RelevanceFilterResult,
    TalentSignalResult,
)

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

Given a company name, an ESG factor, and a summary of evidence scores across five
independent streams, write a concise 2–3 sentence credibility narrative.

Rules:
- Focus on what the evidence implies about the company's actual commitment vs stated position
- Note any significant gaps or contradictions between streams
- Do not reproduce numbers verbatim — interpret what they mean
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

        factor_scores: List[FactorScore] = []
        for factor in factors:
            fs = self._score_factor(
                factor=factor,
                profile=profile,
                grade_map=grade_map,
                talent_result=talent_result,
                regulatory_paths=regulatory_paths,
                run_id=run_id,
            )
            factor_scores.append(fs)

        overall = (
            sum(fs.score for fs in factor_scores) / len(factor_scores) if factor_scores else 0.0
        )

        return CredibilityReport(
            ticker=profile.ticker,
            company_name=profile.name,
            sasb_industry=relevance_result.sasb_industry,
            factor_scores=factor_scores,
            overall_score=round(overall, 4),
            overall_flag=_flag(overall),
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
        talent_result: Optional[TalentSignalResult],
        regulatory_paths: Dict[str, str],
        run_id: Optional[str],
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

        # Stream 3: Talent
        tal_score, tal_evidence = self._score_talent(factor, talent_result)
        stream_scores["talent"] = tal_score
        evidence.append(tal_evidence)

        # Stream 4: Words vs Money (stub — issue #12)
        stream_scores["words_money"] = 0.5
        evidence.append("Words vs Money: pending issue #12 — neutral score applied")

        # Stream 5: Supply chain (stub — no issue assigned yet)
        stream_scores["supply_chain"] = 0.5
        evidence.append("Supply chain: not yet implemented — neutral score applied")

        # Weighted aggregate
        weighted = sum(_WEIGHTS[k] * v for k, v in stream_scores.items())
        score = round(min(1.0, max(0.0, weighted)), 4)
        flag = _flag(score)

        # Claude narrative (qualitative synthesis only — no arithmetic)
        narrative = self._generate_narrative(
            company=profile.name,
            factor=factor,
            stream_scores=stream_scores,
            score=score,
            flag=flag,
            run_id=run_id,
        )

        return FactorScore(
            factor_id=factor.factor_id,
            factor_name=factor.name,
            score=score,
            flag=flag,
            stream_scores=stream_scores,
            evidence=evidence,
            sources=list(profile.source_urls),
            narrative=narrative,
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
                0.5,
                f"Regulatory ({factor.name}): no mandatory disclosure source mapped — neutral",
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
            return 0.5, f"Regulatory ({factor.name}): no data files found — neutral"

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
    ) -> tuple[float, str]:
        if talent_result is None:
            return 0.5, f"Talent ({factor.name}): no TalentSignalResult provided — neutral"

        talent_key = _DIMENSION_TO_TALENT_KEY.get(factor.dimension)
        if not talent_key:
            return (
                0.5,
                f"Talent ({factor.name}): dimension {factor.dimension!r} not mapped — neutral",
            )

        raw = talent_result.factor_scores.get(talent_key, 0.5)
        note = (
            f"Talent ({factor.name}): {talent_key} score {raw:.2f} "
            f"({talent_result.total_postings} postings, "
            f"{talent_result.senior_ratio:.0%} senior, "
            f"{talent_result.ghost_count} ghost flags)"
        )
        return round(float(raw), 4), note

    # ------------------------------------------------------------------
    # Claude narrative synthesis
    # ------------------------------------------------------------------

    def _generate_narrative(
        self,
        company: str,
        factor: MaterialFactor,
        stream_scores: Dict[str, float],
        score: float,
        flag: str,
        run_id: Optional[str],
    ) -> str:
        stream_summary = "\n".join(f"  {k}: {v:.2f}" for k, v in stream_scores.items())
        prompt = dedent(
            f"""
            Company: {company}
            ESG factor: {factor.name} ({factor.dimension})
            Overall credibility score: {score:.2f} ({flag.upper()})

            Stream scores:
            {stream_summary}

            Write a 2–3 sentence credibility narrative for this factor.
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


def _flag(score: float) -> str:
    if score >= 0.65:
        return "green"
    if score >= 0.40:
        return "amber"
    return "red"
