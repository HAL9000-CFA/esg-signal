"""
Mocked unit tests for CredibilityScorer.
No live API calls, no filesystem reads.
"""

from unittest.mock import patch

from agents.credibility_scorer import _WEIGHTS, CredibilityScorer, _flag
from pipeline.models import (
    CommitmentCheck,
    CompanyProfile,
    CredibilityReport,
    MaterialFactor,
    RelevanceFilterResult,
    TalentSignalResult,
    WordsMoneyResult,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_profile(annual_report_text="ESG report text.", source_urls=None, raw_financials=None):
    return CompanyProfile(
        ticker="TEST",
        name="Test Corp",
        index="SP500",
        sic_code="7372",
        sic_description="Prepackaged Software",
        country="US",
        latest_annual_filing=None,
        annual_report_text=annual_report_text,
        raw_financials=raw_financials if raw_financials is not None else {},
        source_urls=source_urls or ["https://example.com/report"],
        errors=[],
    )


def _make_factor(factor_id="ghg_emissions", dimension="Environment"):
    return MaterialFactor(
        factor_id=factor_id,
        name="GHG Emissions",
        dimension=dimension,
        financial_impacts=["cost_impact", "asset_impact"],
    )


def _make_relevance(factors=None):
    return RelevanceFilterResult(
        ticker="TEST",
        sic_code="7372",
        sasb_industry="Software & IT Services",
        material_factors=factors or [_make_factor()],
        errors=[],
    )


def _make_talent(env_score=0.6):
    return TalentSignalResult(
        company_name="Test Corp",
        total_postings=10,
        senior_ratio=0.4,
        ghost_count=1,
        factor_scores={"environment": env_score, "social": 0.5, "governance": 0.5},
        errors=[],
    )


def _make_wm_result(score=0.5, checks=None):
    return WordsMoneyResult(
        ticker="TEST",
        factor_id="ghg_emissions",
        commitment_checks=checks or [],
        score=score,
        errors=[],
    )


# ---------------------------------------------------------------------------
# _flag helper
# ---------------------------------------------------------------------------


class TestFlag:
    def test_green_at_threshold(self):
        assert _flag(0.65) == "green"

    def test_green_above_threshold(self):
        assert _flag(1.0) == "green"

    def test_amber_at_lower_threshold(self):
        assert _flag(0.40) == "amber"

    def test_amber_just_below_green(self):
        assert _flag(0.64) == "amber"

    def test_red_below_threshold(self):
        assert _flag(0.39) == "red"

    def test_red_at_zero(self):
        assert _flag(0.0) == "red"


# ---------------------------------------------------------------------------
# Weights
# ---------------------------------------------------------------------------


class TestWeights:
    def test_weights_sum_to_one(self):
        assert abs(sum(_WEIGHTS.values()) - 1.0) < 1e-9


# ---------------------------------------------------------------------------
# CredibilityScorer.score — structure
# ---------------------------------------------------------------------------


class TestCredibilityScorerStructure:
    def _run(self, factors=None, talent=None, report_text="ESG report."):
        profile = _make_profile(annual_report_text=report_text)
        relevance = _make_relevance(factors=factors)

        grade_results = [
            {"factor": f.name, "grade": "QUANTIFIED", "evidence": "Reduced by 12%."}
            for f in relevance.material_factors
        ]
        wm_result = _make_wm_result()

        with (
            patch("agents.credibility_scorer.grade_all_factors", return_value=grade_results),
            patch("agents.credibility_scorer.call_claude", return_value="Narrative text."),
            patch("agents.credibility_scorer.words_money_check", return_value=wm_result),
        ):
            return CredibilityScorer().score(
                profile=profile,
                relevance_result=relevance,
                talent_result=talent,
            )

    def test_returns_credibility_report(self):
        result = self._run()
        assert isinstance(result, CredibilityReport)

    def test_ticker_propagated(self):
        assert self._run().ticker == "TEST"

    def test_sasb_industry_propagated(self):
        assert self._run().sasb_industry == "Software & IT Services"

    def test_one_factor_score_per_material_factor(self):
        factors = [_make_factor("ghg_emissions"), _make_factor("data_security", "Social Capital")]
        result = self._run(factors=factors)
        assert len(result.factor_scores) == 2

    def test_overall_score_is_mean_of_factor_scores(self):
        result = self._run()
        expected = sum(fs.score for fs in result.factor_scores) / len(result.factor_scores)
        assert abs(result.overall_score - expected) < 1e-6

    def test_overall_flag_consistent_with_score(self):
        result = self._run()
        assert result.overall_flag == _flag(result.overall_score)

    def test_factor_scores_bounded(self):
        result = self._run()
        for fs in result.factor_scores:
            assert 0.0 <= fs.score <= 1.0

    def test_narrative_populated(self):
        result = self._run()
        for fs in result.factor_scores:
            assert fs.narrative == "Narrative text."

    def test_source_urls_propagated(self):
        result = self._run()
        for fs in result.factor_scores:
            assert "https://example.com/report" in fs.sources

    def test_five_streams_in_stream_scores(self):
        result = self._run()
        for fs in result.factor_scores:
            assert set(fs.stream_scores.keys()) == {
                "disclosure",
                "regulatory",
                "talent",
                "words_money",
                "supply_chain",
            }


# ---------------------------------------------------------------------------
# Disclosure stream
# ---------------------------------------------------------------------------


class TestDisclosureStream:
    def _score_factor(self, grade):
        profile = _make_profile()
        relevance = _make_relevance()
        grade_results = [{"factor": "GHG Emissions", "grade": grade, "evidence": "Quote."}]

        with (
            patch("agents.credibility_scorer.grade_all_factors", return_value=grade_results),
            patch("agents.credibility_scorer.call_claude", return_value="Narrative."),
            patch("agents.credibility_scorer.words_money_check", return_value=_make_wm_result()),
        ):
            report = CredibilityScorer().score(profile=profile, relevance_result=relevance)

        return report.factor_scores[0].stream_scores["disclosure"]

    def test_quantified_scores_high(self):
        assert self._score_factor("QUANTIFIED") == 0.85

    def test_vague_scores_low(self):
        assert self._score_factor("VAGUE") == 0.35

    def test_undisclosed_scores_zero(self):
        assert self._score_factor("UNDISCLOSED") == 0.0

    def test_empty_report_text_produces_error(self):
        profile = _make_profile(annual_report_text="")
        relevance = _make_relevance()

        with (
            patch("agents.credibility_scorer.grade_all_factors", return_value=[]),
            patch("agents.credibility_scorer.call_claude", return_value="Narrative."),
            patch("agents.credibility_scorer.words_money_check", return_value=_make_wm_result()),
        ):
            report = CredibilityScorer().score(profile=profile, relevance_result=relevance)

        assert any("annual_report_text" in e for e in report.errors)


# ---------------------------------------------------------------------------
# Talent stream
# ---------------------------------------------------------------------------


class TestTalentStream:
    def _get_talent_score(self, factor_id, dimension, talent_key_score):
        profile = _make_profile()
        factor = _make_factor(factor_id=factor_id, dimension=dimension)
        relevance = _make_relevance(factors=[factor])
        talent = _make_talent(env_score=talent_key_score)
        grade_results = [{"factor": factor.name, "grade": "VAGUE", "evidence": None}]

        with (
            patch("agents.credibility_scorer.grade_all_factors", return_value=grade_results),
            patch("agents.credibility_scorer.call_claude", return_value="Narrative."),
            patch("agents.credibility_scorer.words_money_check", return_value=_make_wm_result()),
        ):
            report = CredibilityScorer().score(
                profile=profile, relevance_result=relevance, talent_result=talent
            )

        return report.factor_scores[0].stream_scores["talent"]

    def test_environment_dimension_uses_environment_key(self):
        score = self._get_talent_score("ghg_emissions", "Environment", 0.8)
        assert score == 0.8

    def test_no_talent_result_returns_neutral(self):
        profile = _make_profile()
        relevance = _make_relevance()
        grade_results = [{"factor": "GHG Emissions", "grade": "VAGUE", "evidence": None}]

        with (
            patch("agents.credibility_scorer.grade_all_factors", return_value=grade_results),
            patch("agents.credibility_scorer.call_claude", return_value="Narrative."),
            patch("agents.credibility_scorer.words_money_check", return_value=_make_wm_result()),
        ):
            report = CredibilityScorer().score(
                profile=profile, relevance_result=relevance, talent_result=None
            )

        assert report.factor_scores[0].stream_scores["talent"] == 0.5


# ---------------------------------------------------------------------------
# Regulatory stream
# ---------------------------------------------------------------------------


class TestRegulatoryStream:
    def _run_with_paths(self, factor_id, regulatory_paths):
        profile = _make_profile()
        factor = _make_factor(factor_id=factor_id)
        relevance = _make_relevance(factors=[factor])
        grade_results = [{"factor": factor.name, "grade": "VAGUE", "evidence": None}]

        with (
            patch("agents.credibility_scorer.grade_all_factors", return_value=grade_results),
            patch("agents.credibility_scorer.call_claude", return_value="Narrative."),
            patch("agents.credibility_scorer.words_money_check", return_value=_make_wm_result()),
        ):
            report = CredibilityScorer().score(
                profile=profile,
                relevance_result=relevance,
                regulatory_paths=regulatory_paths,
            )

        return report.factor_scores[0].stream_scores["regulatory"]

    def test_no_source_mapped_returns_neutral(self):
        # "data_security" has no regulatory source mapping
        profile = _make_profile()
        factor = _make_factor(factor_id="data_security", dimension="Social Capital")
        relevance = _make_relevance(factors=[factor])
        grade_results = [{"factor": factor.name, "grade": "VAGUE", "evidence": None}]

        with (
            patch("agents.credibility_scorer.grade_all_factors", return_value=grade_results),
            patch("agents.credibility_scorer.call_claude", return_value="Narrative."),
            patch("agents.credibility_scorer.words_money_check", return_value=_make_wm_result()),
        ):
            report = CredibilityScorer().score(profile=profile, relevance_result=relevance)

        assert report.factor_scores[0].stream_scores["regulatory"] == 0.5

    def test_missing_path_returns_neutral(self):
        score = self._run_with_paths("ghg_emissions", {})
        assert score == 0.5

    def test_clean_csv_scores_above_neutral(self, tmp_path):
        import pandas as pd

        csv_path = tmp_path / "ghgrp_TEST.csv"
        pd.DataFrame({"FACILITY_NAME": ["Plant A"], "GHG_QUANTITY": [5000]}).to_csv(
            csv_path, index=False
        )
        score = self._run_with_paths("ghg_emissions", {"ghgrp": str(csv_path)})
        assert score > 0.5

    def test_violations_reduce_score(self, tmp_path):
        import pandas as pd

        csv_path = tmp_path / "echo_TEST.csv"
        pd.DataFrame(
            {
                "FACILITY_NAME": ["Plant A", "Plant B"],
                "VIOLATION_TYPE": ["CAA", "CWA"],
                "PENALTY_AMOUNT": [50000, 20000],
            }
        ).to_csv(csv_path, index=False)
        score = self._run_with_paths("ghg_emissions", {"echo": str(csv_path)})
        assert score <= 0.75


# ---------------------------------------------------------------------------
# Words vs Money stream (via credibility scorer)
# ---------------------------------------------------------------------------


class TestWordsMoneyStream:
    def _run_with_wm(self, wm_result):
        profile = _make_profile()
        relevance = _make_relevance()
        grade_results = [{"factor": "GHG Emissions", "grade": "VAGUE", "evidence": None}]

        with (
            patch("agents.credibility_scorer.grade_all_factors", return_value=grade_results),
            patch("agents.credibility_scorer.call_claude", return_value="Narrative."),
            patch("agents.credibility_scorer.words_money_check", return_value=wm_result),
        ):
            report = CredibilityScorer().score(profile=profile, relevance_result=relevance)

        return report.factor_scores[0]

    def test_score_propagated_from_wm_result(self):
        wm = _make_wm_result(score=0.8)
        fs = self._run_with_wm(wm)
        assert fs.stream_scores["words_money"] == 0.8

    def test_checks_stored_on_factor_score(self):
        check = CommitmentCheck(
            commitment_text="Invest £500M by 2030",
            claimed_amount=500_000_000,
            currency="GBP",
            horizon_year=2030,
            category="capex",
            financials_label="capex",
            financials_value=200_000_000,
            flag="plausible",
            notes="ratio 0.6x",
        )
        wm = _make_wm_result(score=0.6, checks=[check])
        fs = self._run_with_wm(wm)
        assert fs.words_money_checks is not None
        assert len(fs.words_money_checks) == 1
        assert fs.words_money_checks[0].flag == "plausible"

    def test_no_checks_gives_none_on_factor_score(self):
        wm = _make_wm_result(score=0.5, checks=[])
        fs = self._run_with_wm(wm)
        assert fs.words_money_checks is None

    def test_wm_exception_returns_neutral(self):
        profile = _make_profile()
        relevance = _make_relevance()
        grade_results = [{"factor": "GHG Emissions", "grade": "VAGUE", "evidence": None}]

        with (
            patch("agents.credibility_scorer.grade_all_factors", return_value=grade_results),
            patch("agents.credibility_scorer.call_claude", return_value="Narrative."),
            patch(
                "agents.credibility_scorer.words_money_check", side_effect=Exception("wm failed")
            ),
        ):
            report = CredibilityScorer().score(profile=profile, relevance_result=relevance)

        assert report.factor_scores[0].stream_scores["words_money"] == 0.5


# ---------------------------------------------------------------------------
# Stub streams
# ---------------------------------------------------------------------------


class TestStubStreams:
    def test_supply_chain_is_neutral(self):
        profile = _make_profile()
        relevance = _make_relevance()
        grade_results = [{"factor": "GHG Emissions", "grade": "VAGUE", "evidence": None}]

        with (
            patch("agents.credibility_scorer.grade_all_factors", return_value=grade_results),
            patch("agents.credibility_scorer.call_claude", return_value="Narrative."),
            patch("agents.credibility_scorer.words_money_check", return_value=_make_wm_result()),
        ):
            report = CredibilityScorer().score(profile=profile, relevance_result=relevance)

        assert report.factor_scores[0].stream_scores["supply_chain"] == 0.5


# ---------------------------------------------------------------------------
# Claude narrative failure handling
# ---------------------------------------------------------------------------


class TestNarrativeFailure:
    def test_score_survives_claude_error(self):
        profile = _make_profile()
        relevance = _make_relevance()
        grade_results = [{"factor": "GHG Emissions", "grade": "QUANTIFIED", "evidence": "12%."}]

        with (
            patch("agents.credibility_scorer.grade_all_factors", return_value=grade_results),
            patch("agents.credibility_scorer.call_claude", side_effect=Exception("API down")),
            patch("agents.credibility_scorer.words_money_check", return_value=_make_wm_result()),
        ):
            report = CredibilityScorer().score(profile=profile, relevance_result=relevance)

        assert report.factor_scores[0].score > 0
        assert "unavailable" in report.factor_scores[0].narrative
