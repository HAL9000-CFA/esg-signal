"""
Unit tests for pipeline/words_vs_money.py.
No live API calls — Claude is mocked throughout.
"""

import json
from unittest.mock import patch

from pipeline.models import CommitmentCheck, CompanyProfile, MaterialFactor, WordsMoneyResult
from pipeline.words_vs_money import (
    _BASE_YEAR,
    _annualise,
    _compute_score,
    _find_financial,
    check,
    evidence_string,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_profile(
    report_text="We will invest £500M in renewable energy by 2030.",
    raw_financials=None,
):
    return CompanyProfile(
        ticker="TEST",
        name="Test Corp",
        index="SP500",
        sic_code="1311",
        sic_description="Crude Petroleum",
        country="US",
        latest_annual_filing=None,
        annual_report_text=report_text,
        raw_financials=(
            raw_financials
            if raw_financials is not None
            else {
                "revenue": 10_000_000_000,
                "operating_income": 1_500_000_000,
                "total_assets": 20_000_000_000,
                "capex": 800_000_000,
                "total_opex": 6_000_000_000,
            }
        ),
        source_urls=["https://example.com/report"],
        errors=[],
    )


def _make_factor(factor_id="ghg_emissions", dimension="Environment"):
    return MaterialFactor(
        factor_id=factor_id,
        name="GHG Emissions",
        dimension=dimension,
        financial_impacts=["cost_impact", "asset_impact"],
    )


def _mock_claude(commitments: list):
    """Return a patch target that makes call_claude return a JSON array."""
    return patch("pipeline.words_vs_money.call_claude", return_value=json.dumps(commitments))


# ---------------------------------------------------------------------------
# _annualise
# ---------------------------------------------------------------------------


class TestAnnualise:
    def test_known_year_divides_by_remaining_years(self):
        # 2030 → 4 years from 2026
        result = _annualise(400_000, 2030)
        assert result == 100_000.0

    def test_past_year_uses_minimum_divisor_of_one(self):
        # year <= base year → divisor = 1
        result = _annualise(500_000, 2020)
        assert result == 500_000.0

    def test_no_year_uses_five_year_default(self):
        result = _annualise(500_000, None)
        assert result == 100_000.0

    def test_current_year_uses_minimum_divisor_of_one(self):
        result = _annualise(300_000, _BASE_YEAR)
        assert result == 300_000.0


# ---------------------------------------------------------------------------
# _find_financial
# ---------------------------------------------------------------------------


class TestFindFinancial:
    def test_capex_category_returns_capex_first(self):
        fins = {"capex": 500_000, "revenue": 1_000_000}
        key, val = _find_financial("capex", fins)
        assert key == "capex"
        assert val == 500_000

    def test_falls_back_to_next_key_when_first_is_none(self):
        fins = {"capex": None, "total_assets": 2_000_000, "revenue": 1_000_000}
        key, val = _find_financial("capex", fins)
        assert key == "total_assets"

    def test_falls_back_to_revenue_when_capex_missing(self):
        fins = {"revenue": 9_000_000}
        key, val = _find_financial("capex", fins)
        assert key == "revenue"

    def test_returns_none_none_when_all_missing(self):
        key, val = _find_financial("capex", {})
        assert key is None
        assert val is None

    def test_opex_category_returns_total_opex_first(self):
        fins = {"total_opex": 300_000, "revenue": 1_000_000}
        key, val = _find_financial("opex", fins)
        assert key == "total_opex"


# ---------------------------------------------------------------------------
# _compute_score
# ---------------------------------------------------------------------------


class TestComputeScore:
    def test_empty_checks_returns_neutral(self):
        assert _compute_score([]) == 0.5

    def test_all_consistent_scores_high(self):
        checks = [
            CommitmentCheck("t", 1e6, "USD", 2030, "capex", "capex", 2e6, "consistent", "ok"),
            CommitmentCheck("t", 1e6, "USD", 2030, "capex", "capex", 2e6, "consistent", "ok"),
        ]
        assert _compute_score(checks) == 0.80

    def test_all_gap_scores_low(self):
        checks = [
            CommitmentCheck("t", 1e9, "USD", 2030, "capex", "capex", 1e6, "gap", "gap"),
        ]
        assert _compute_score(checks) == 0.10

    def test_mixed_flags_averages_correctly(self):
        checks = [
            CommitmentCheck("t", 1e6, "USD", 2030, "capex", "capex", 2e6, "consistent", "ok"),
            CommitmentCheck("t", 1e9, "USD", 2030, "capex", "capex", 1e6, "gap", "gap"),
        ]
        expected = round((0.80 + 0.10) / 2, 4)
        assert _compute_score(checks) == expected

    def test_unverifiable_is_neutral(self):
        checks = [
            CommitmentCheck("t", None, "USD", None, "other", None, None, "unverifiable", "?"),
        ]
        assert _compute_score(checks) == 0.50


# ---------------------------------------------------------------------------
# check() — full pipeline with Claude mocked
# ---------------------------------------------------------------------------


class TestCheck:
    def test_returns_word_money_result(self):
        profile = _make_profile()
        factor = _make_factor()
        commitments = [
            {
                "text": "£500M by 2030",
                "amount": 500_000_000,
                "currency": "GBP",
                "year": 2030,
                "category": "capex",
            }
        ]

        with _mock_claude(commitments):
            result = check(profile, factor)

        assert isinstance(result, WordsMoneyResult)

    def test_empty_report_returns_neutral_with_error(self):
        profile = _make_profile(report_text="")
        factor = _make_factor()

        with _mock_claude([]):
            result = check(profile, factor)

        assert result.score == 0.5
        assert result.errors

    def test_no_commitments_found_returns_neutral(self):
        profile = _make_profile()
        factor = _make_factor()

        with _mock_claude([]):
            result = check(profile, factor)

        assert result.score == 0.5
        assert result.commitment_checks == []

    def test_claude_failure_returns_neutral_with_error(self):
        profile = _make_profile()
        factor = _make_factor()

        with patch("pipeline.words_vs_money.call_claude", side_effect=Exception("API error")):
            result = check(profile, factor)

        assert result.score == 0.5
        assert any("extraction failed" in e.lower() for e in result.errors)

    def test_invalid_json_returns_neutral_with_error(self):
        profile = _make_profile()
        factor = _make_factor()

        with patch("pipeline.words_vs_money.call_claude", return_value="not json at all"):
            result = check(profile, factor)

        assert result.score == 0.5
        assert result.errors

    def test_consistent_commitment_scores_high(self):
        # annualised = 100M/yr, capex = 800M → ratio 0.125 → consistent
        profile = _make_profile(raw_financials={"capex": 800_000_000, "revenue": 10_000_000_000})
        factor = _make_factor()
        commitments = [
            {
                "text": "invest £500M in green energy by 2031",
                "amount": 500_000_000,
                "currency": "GBP",
                "year": 2031,
                "category": "capex",
            }
        ]

        with _mock_claude(commitments):
            result = check(profile, factor)

        assert len(result.commitment_checks) == 1
        assert result.commitment_checks[0].flag == "consistent"
        assert result.score == 0.80

    def test_gap_commitment_scores_low(self):
        # annualised = 500M/yr (no year given → /5), capex = 10M → ratio 50 → gap
        profile = _make_profile(raw_financials={"capex": 10_000_000, "revenue": 500_000_000})
        factor = _make_factor()
        commitments = [
            {
                "text": "invest $2.5B in net-zero transition",
                "amount": 2_500_000_000,
                "currency": "USD",
                "year": None,
                "category": "capex",
            }
        ]

        with _mock_claude(commitments):
            result = check(profile, factor)

        assert result.commitment_checks[0].flag == "gap"
        assert result.score == 0.10

    def test_no_financial_data_flags_unverifiable(self):
        profile = _make_profile(raw_financials={})
        factor = _make_factor()
        commitments = [
            {
                "text": "invest £500M by 2030",
                "amount": 500_000_000,
                "currency": "GBP",
                "year": 2030,
                "category": "capex",
            }
        ]

        with _mock_claude(commitments):
            result = check(profile, factor)

        assert result.commitment_checks[0].flag == "unverifiable"
        assert result.score == 0.50

    def test_no_amount_stated_flags_unverifiable(self):
        profile = _make_profile()
        factor = _make_factor()
        commitments = [
            {
                "text": "We are committed to reducing our carbon footprint",
                "amount": None,
                "currency": None,
                "year": 2030,
                "category": "reduction_target",
            }
        ]

        with _mock_claude(commitments):
            result = check(profile, factor)

        assert result.commitment_checks[0].flag == "unverifiable"

    def test_run_id_forwarded_to_claude(self):
        profile = _make_profile()
        factor = _make_factor()

        with patch("pipeline.words_vs_money.call_claude", return_value="[]") as mock_claude:
            check(profile, factor, run_id="run_abc")

        call_kwargs = mock_claude.call_args[1]
        assert call_kwargs.get("run_id") == "run_abc"

    def test_multiple_commitments_averaged(self):
        profile = _make_profile(raw_financials={"capex": 800_000_000})
        factor = _make_factor()
        # First: consistent (annualised 100M vs capex 800M → ratio 0.125)
        # Second: gap (annualised 500M/5yr=100M ... wait let me recalc)
        # Consistent: 500M / (2031-2026=5) = 100M; 100M/800M = 0.125 → consistent
        # Gap: 50B / 5yr = 10B; 10B/800M = 12.5 → gap
        commitments = [
            {
                "text": "£500M by 2031",
                "amount": 500_000_000,
                "currency": "GBP",
                "year": 2031,
                "category": "capex",
            },
            {
                "text": "$50B net-zero fund",
                "amount": 50_000_000_000,
                "currency": "USD",
                "year": None,
                "category": "capex",
            },
        ]

        with _mock_claude(commitments):
            result = check(profile, factor)

        flags = {c.flag for c in result.commitment_checks}
        assert "consistent" in flags
        assert "gap" in flags
        expected_score = round((0.80 + 0.10) / 2, 4)
        assert result.score == expected_score


# ---------------------------------------------------------------------------
# evidence_string
# ---------------------------------------------------------------------------


class TestEvidenceString:
    def test_no_checks_returns_neutral_message(self):
        result = WordsMoneyResult(
            ticker="T", factor_id="ghg", commitment_checks=[], score=0.5, errors=[]
        )
        ev = evidence_string("GHG Emissions", result)
        assert "no monetary commitments" in ev
        assert "neutral" in ev

    def test_with_checks_summarises_flag_counts(self):
        checks = [
            CommitmentCheck("t", 1e6, "GBP", 2030, "capex", "capex", 2e6, "consistent", "ok"),
            CommitmentCheck("t", 1e9, "USD", None, "capex", "capex", 1e5, "gap", "gap"),
        ]
        result = WordsMoneyResult(
            ticker="T", factor_id="ghg", commitment_checks=checks, score=0.45, errors=[]
        )
        ev = evidence_string("GHG Emissions", result)
        assert "2 commitment" in ev
        assert "consistent" in ev
        assert "gap" in ev
