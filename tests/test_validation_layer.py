"""
Unit tests for pipeline/validation_layer.py.
No external calls — all inputs are constructed in-process.
"""

from pipeline.models import (
    CredibilityReport,
    DcfLineItem,
    DcfMapperResult,
    EsgDcfMapping,
    FactorScore,
)
from pipeline.validation_layer import (
    FLAG_INCONSISTENT,
    INVALID_FLAG_VALUE,
    INVALID_SOURCE_URL,
    NARRATIVE_UNTRACED_NUMBER,
    SCENARIO_RANGE_INVALID,
    SCORE_OUT_OF_RANGE,
    UNTRUSTED_DOMAIN,
    ValidationLayer,
    ValidationResult,
    ValidationWarning,
    _clamp,
    _downgrade_flag,
    _expected_flag,
    _flag_mismatch,
    _in_unit_interval,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_factor_score(
    factor_id="ghg_emissions",
    score=0.85,
    flag="green",
    stream_scores=None,
    sources=None,
    narrative="The company demonstrates strong commitment to emission reductions.",
    evidence=None,
) -> FactorScore:
    return FactorScore(
        factor_id=factor_id,
        factor_name="GHG Emissions",
        score=score,
        flag=flag,
        coverage=1.0,
        confidence=1.0,
        stream_scores=stream_scores
        or {
            "disclosure": 0.85,
            "regulatory": 0.75,
            "talent": 0.60,
            "words_money": 0.70,
            "supply_chain": 0.50,
        },
        evidence=evidence or ["Disclosure: QUANTIFIED — reduced by 12%."],
        sources=sources
        or ["https://sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000320193"],
        narrative=narrative,
    )


def _make_report(factor_scores=None, overall_score=0.85, overall_flag="green") -> CredibilityReport:
    return CredibilityReport(
        ticker="TEST",
        company_name="Test Corp",
        sasb_industry="Oil & Gas",
        factor_scores=factor_scores or [_make_factor_score()],
        overall_score=overall_score,
        overall_flag=overall_flag,
        errors=[],
    )


def _make_dcf_mapping(
    factor_id="ghg_emissions",
    low=500_000.0,
    mid=5_000_000.0,
    high=50_000_000.0,
) -> EsgDcfMapping:
    return EsgDcfMapping(
        factor_id=factor_id,
        factor_name="GHG Emissions",
        financial_impacts=["cost_impact"],
        mapped_line_items=[DcfLineItem("DCF", 1, "Capital Expenditure")],
        scenario_low=low,
        scenario_mid=mid,
        scenario_high=high,
        scenario_currency="USD",
        scenario_source="EPA ECHO data",
    )


def _make_dcf_result(mappings=None) -> DcfMapperResult:
    return DcfMapperResult(
        ticker="TEST",
        excel_path="model.xlsx",
        sheet_names=["DCF"],
        line_item_count=10,
        mappings=mappings or [_make_dcf_mapping()],
        unmapped_factors=[],
        errors=[],
    )


# ---------------------------------------------------------------------------
# Pure-function helpers
# ---------------------------------------------------------------------------


class TestInUnitInterval:
    def test_zero_is_valid(self):
        assert _in_unit_interval(0.0) is True

    def test_one_is_valid(self):
        assert _in_unit_interval(1.0) is True

    def test_mid_range_valid(self):
        assert _in_unit_interval(0.55) is True

    def test_negative_invalid(self):
        assert _in_unit_interval(-0.01) is False

    def test_above_one_invalid(self):
        assert _in_unit_interval(1.01) is False

    def test_none_invalid(self):
        assert _in_unit_interval(None) is False

    def test_string_invalid(self):
        assert _in_unit_interval("high") is False


class TestClamp:
    def test_in_range_unchanged(self):
        assert _clamp(0.75) == 0.75

    def test_negative_clamped_to_zero(self):
        assert _clamp(-0.5) == 0.0

    def test_above_one_clamped_to_one(self):
        assert _clamp(1.5) == 1.0

    def test_exactly_zero(self):
        assert _clamp(0.0) == 0.0

    def test_exactly_one(self):
        assert _clamp(1.0) == 1.0


class TestExpectedFlag:
    def test_green_at_threshold(self):
        assert _expected_flag(0.80) == "green"

    def test_green_above_threshold(self):
        assert _expected_flag(0.9) == "green"

    def test_amber_at_threshold(self):
        assert _expected_flag(0.40) == "amber"

    def test_amber_just_below_green(self):
        assert _expected_flag(0.79) == "amber"

    def test_red_below_amber(self):
        assert _expected_flag(0.39) == "red"

    def test_red_at_zero(self):
        assert _expected_flag(0.0) == "red"


class TestFlagMismatch:
    def test_correct_flag_returns_none(self):
        assert _flag_mismatch(0.80, "green") is None

    def test_mismatch_returns_message(self):
        msg = _flag_mismatch(0.70, "red")
        assert msg is not None
        assert "red" in msg
        assert "green" in msg

    def test_amber_score_amber_flag_ok(self):
        assert _flag_mismatch(0.50, "amber") is None


class TestDowngradeFlag:
    def test_green_to_amber(self):
        assert _downgrade_flag("green") == "amber"

    def test_amber_to_red(self):
        assert _downgrade_flag("amber") == "red"

    def test_red_stays_red(self):
        assert _downgrade_flag("red") == "red"

    def test_unknown_returned_unchanged(self):
        assert _downgrade_flag("unknown") == "unknown"


# ---------------------------------------------------------------------------
# ValidationResult properties
# ---------------------------------------------------------------------------


class TestValidationResult:
    def _make_result(self, warnings):
        return ValidationResult(
            passed=True,
            warnings=warnings,
            adjusted_report=_make_report(),
            adjusted_dcf=None,
        )

    def test_error_count(self):
        w = [
            ValidationWarning(SCORE_OUT_OF_RANGE, "error", "f", "m", 1.5),
            ValidationWarning(FLAG_INCONSISTENT, "error", "f", "m", "red"),
            ValidationWarning(UNTRUSTED_DOMAIN, "warning", "f", "m", "http://x.com"),
        ]
        assert self._make_result(w).error_count == 2

    def test_warning_count(self):
        w = [
            ValidationWarning(UNTRUSTED_DOMAIN, "warning", "f", "m", "x"),
            ValidationWarning(NARRATIVE_UNTRACED_NUMBER, "warning", "f", "m", []),
        ]
        assert self._make_result(w).warning_count == 2


# ---------------------------------------------------------------------------
# ValidationLayer.validate — clean report passes
# ---------------------------------------------------------------------------


class TestCleanReportPasses:
    def test_valid_report_passes(self):
        result = ValidationLayer().validate(_make_report())
        assert result.passed is True
        assert result.error_count == 0

    def test_adjusted_report_returned(self):
        report = _make_report()
        result = ValidationLayer().validate(report)
        assert isinstance(result.adjusted_report, CredibilityReport)

    def test_original_not_mutated(self):
        report = _make_report()
        original_score = report.overall_score
        ValidationLayer().validate(report)
        assert report.overall_score == original_score

    def test_valid_dcf_passes(self):
        result = ValidationLayer().validate(_make_report(), dcf_result=_make_dcf_result())
        assert result.passed is True
        assert result.adjusted_dcf is not None


# ---------------------------------------------------------------------------
# Check 1: overall score
# ---------------------------------------------------------------------------


class TestOverallScore:
    def test_score_above_one_raises_error(self):
        report = _make_report(overall_score=1.5, overall_flag="green")
        result = ValidationLayer().validate(report)
        codes = [w.code for w in result.warnings]
        assert SCORE_OUT_OF_RANGE in codes

    def test_score_clamped_to_one(self):
        report = _make_report(overall_score=1.5, overall_flag="green")
        result = ValidationLayer().validate(report)
        assert result.adjusted_report.overall_score == 1.0

    def test_negative_score_clamped_to_zero(self):
        report = _make_report(overall_score=-0.2, overall_flag="red")
        result = ValidationLayer().validate(report)
        assert result.adjusted_report.overall_score == 0.0

    def test_out_of_range_score_not_passed(self):
        report = _make_report(overall_score=1.5, overall_flag="green")
        result = ValidationLayer().validate(report)
        assert result.passed is False

    def test_valid_score_no_score_error(self):
        report = _make_report(overall_score=0.65, overall_flag="green")
        result = ValidationLayer().validate(report)
        assert not any(w.code == SCORE_OUT_OF_RANGE for w in result.warnings)


# ---------------------------------------------------------------------------
# Check 2–3: flag consistency and valid values
# ---------------------------------------------------------------------------


class TestFlagValidation:
    def test_mismatched_flag_raises_error(self):
        # score=0.72 → green, but flag="red"
        report = _make_report(overall_score=0.72, overall_flag="red")
        result = ValidationLayer().validate(report)
        assert any(w.code == FLAG_INCONSISTENT for w in result.warnings)

    def test_mismatched_flag_corrected(self):
        # score=0.80 → should be "green", but flag="red" is an error.
        # Correction sets flag to "green", then the error triggers a confidence
        # downgrade (green → amber).  Final adjusted flag is "amber".
        report = _make_report(overall_score=0.80, overall_flag="red")
        result = ValidationLayer().validate(report)
        assert result.adjusted_report.overall_flag == "amber"

    def test_invalid_flag_value_raises_error(self):
        report = _make_report(overall_flag="yellow")
        result = ValidationLayer().validate(report)
        assert any(w.code == INVALID_FLAG_VALUE for w in result.warnings)

    def test_factor_flag_inconsistency_corrected(self):
        fs = _make_factor_score(score=0.80, flag="red")
        report = _make_report(factor_scores=[fs], overall_score=0.80, overall_flag="green")
        result = ValidationLayer().validate(report)
        corrected_flag = result.adjusted_report.factor_scores[0].flag
        assert corrected_flag == "green"

    def test_stream_score_out_of_range_flagged(self):
        fs = _make_factor_score(
            stream_scores={
                "disclosure": 1.5,
                "regulatory": 0.5,
                "talent": 0.5,
                "words_money": 0.5,
                "supply_chain": 0.5,
            }
        )
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        assert any(w.code == SCORE_OUT_OF_RANGE for w in result.warnings)

    def test_stream_score_clamped(self):
        fs = _make_factor_score(
            stream_scores={
                "disclosure": 1.5,
                "regulatory": 0.5,
                "talent": 0.5,
                "words_money": 0.5,
                "supply_chain": 0.5,
            }
        )
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        assert result.adjusted_report.factor_scores[0].stream_scores["disclosure"] == 1.0


# ---------------------------------------------------------------------------
# Check 4–5: source URL validation
# ---------------------------------------------------------------------------


class TestSourceUrls:
    def test_invalid_url_removed(self):
        fs = _make_factor_score(sources=["not-a-url", "https://sec.gov/filing"])
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        sources = result.adjusted_report.factor_scores[0].sources
        assert "not-a-url" not in sources
        assert "https://sec.gov/filing" in sources

    def test_invalid_url_raises_error(self):
        fs = _make_factor_score(sources=["ftp://example.com"])
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        assert any(w.code == INVALID_SOURCE_URL for w in result.warnings)

    def test_trusted_domain_no_warning(self):
        fs = _make_factor_score(sources=["https://sec.gov/filing"])
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        assert not any(w.code == UNTRUSTED_DOMAIN for w in result.warnings)

    def test_untrusted_domain_raises_warning_not_error(self):
        fs = _make_factor_score(sources=["https://bp.com/sustainability-report.pdf"])
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        domain_warnings = [w for w in result.warnings if w.code == UNTRUSTED_DOMAIN]
        assert len(domain_warnings) == 1
        assert domain_warnings[0].severity == "warning"

    def test_untrusted_domain_url_still_kept(self):
        # Untrusted domain is a warning only — URL is still retained
        fs = _make_factor_score(sources=["https://bp.com/sustainability-report.pdf"])
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        assert (
            "https://bp.com/sustainability-report.pdf"
            in result.adjusted_report.factor_scores[0].sources
        )

    def test_empty_url_removed_with_error(self):
        fs = _make_factor_score(sources=["", "https://sec.gov/filing"])
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        assert "" not in result.adjusted_report.factor_scores[0].sources
        assert any(w.code == INVALID_SOURCE_URL for w in result.warnings)

    def test_report_still_passes_with_only_url_warnings(self):
        # Untrusted domain → warning only → report should still pass
        fs = _make_factor_score(sources=["https://company.com/report.pdf"])
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        assert result.passed is True


# ---------------------------------------------------------------------------
# Check 6: narrative number scan
# ---------------------------------------------------------------------------


class TestNarrativeNumberScan:
    def test_traced_score_in_narrative_no_warning(self):
        # score=0.72 is in the computed data — should be fine
        fs = _make_factor_score(
            score=0.72, flag="green", narrative="The score of 0.72 reflects strong performance."
        )
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        assert not any(w.code == NARRATIVE_UNTRACED_NUMBER for w in result.warnings)

    def test_untraced_score_in_narrative_raises_warning(self):
        # 0.93 is not in any computed score
        fs = _make_factor_score(
            score=0.72,
            flag="green",
            stream_scores={
                "disclosure": 0.85,
                "regulatory": 0.75,
                "talent": 0.60,
                "words_money": 0.70,
                "supply_chain": 0.50,
            },
            narrative="Research indicates a credibility score of 0.93 for this company.",
        )
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        untraced = [w for w in result.warnings if w.code == NARRATIVE_UNTRACED_NUMBER]
        assert len(untraced) == 1

    def test_untraced_warning_is_severity_warning_not_error(self):
        fs = _make_factor_score(
            score=0.72,
            flag="green",
            narrative="Analysts rate this 0.93 based on recent disclosures.",
        )
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        untraced = [w for w in result.warnings if w.code == NARRATIVE_UNTRACED_NUMBER]
        assert all(w.severity == "warning" for w in untraced)

    def test_untraced_warning_does_not_cause_failure(self):
        # Narrative warnings don't flip passed → False (only errors do)
        fs = _make_factor_score(
            score=0.85,
            flag="green",
            narrative="The 0.93 figure is notable.",
        )
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        assert result.passed is True

    def test_integers_not_flagged(self):
        # "50000 employees" — integers ignored
        fs = _make_factor_score(narrative="The company employs 50000 people across 12 countries.")
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        assert not any(w.code == NARRATIVE_UNTRACED_NUMBER for w in result.warnings)

    def test_percentages_above_one_not_flagged(self):
        fs = _make_factor_score(narrative="Emissions fell by 23 percent year-on-year.")
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        assert not any(w.code == NARRATIVE_UNTRACED_NUMBER for w in result.warnings)

    def test_number_from_evidence_is_traced(self):
        # 0.55 appears in evidence (from Python) — should not be flagged in narrative
        fs = _make_factor_score(
            score=0.72,
            flag="green",
            evidence=["Regulatory: score 0.55 — no violations"],
            narrative="The regulatory score of 0.55 is satisfactory.",
        )
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        assert not any(w.code == NARRATIVE_UNTRACED_NUMBER for w in result.warnings)

    def test_empty_narrative_no_error(self):
        fs = _make_factor_score(narrative="")
        report = _make_report(factor_scores=[fs])
        result = ValidationLayer().validate(report)
        assert not any(w.code == NARRATIVE_UNTRACED_NUMBER for w in result.warnings)


# ---------------------------------------------------------------------------
# Check 7: DCF scenario ranges
# ---------------------------------------------------------------------------


class TestScenarioRanges:
    def test_valid_ranges_pass(self):
        result = ValidationLayer().validate(_make_report(), dcf_result=_make_dcf_result())
        assert not any(w.code == SCENARIO_RANGE_INVALID for w in result.warnings)

    def test_low_equals_mid_raises_error(self):
        mapping = _make_dcf_mapping(low=500_000, mid=500_000, high=5_000_000)
        result = ValidationLayer().validate(_make_report(), dcf_result=_make_dcf_result([mapping]))
        assert any(w.code == SCENARIO_RANGE_INVALID for w in result.warnings)

    def test_mid_equals_high_raises_error(self):
        mapping = _make_dcf_mapping(low=500_000, mid=5_000_000, high=5_000_000)
        result = ValidationLayer().validate(_make_report(), dcf_result=_make_dcf_result([mapping]))
        assert any(w.code == SCENARIO_RANGE_INVALID for w in result.warnings)

    def test_low_greater_than_mid_raises_error(self):
        mapping = _make_dcf_mapping(low=10_000_000, mid=500_000, high=50_000_000)
        result = ValidationLayer().validate(_make_report(), dcf_result=_make_dcf_result([mapping]))
        assert any(w.code == SCENARIO_RANGE_INVALID for w in result.warnings)

    def test_zero_scenario_value_raises_error(self):
        mapping = _make_dcf_mapping(low=0.0, mid=5_000_000, high=50_000_000)
        result = ValidationLayer().validate(_make_report(), dcf_result=_make_dcf_result([mapping]))
        assert any(w.code == SCENARIO_RANGE_INVALID for w in result.warnings)

    def test_none_scenario_value_raises_error(self):
        mapping = _make_dcf_mapping(low=None, mid=5_000_000, high=50_000_000)
        result = ValidationLayer().validate(_make_report(), dcf_result=_make_dcf_result([mapping]))
        assert any(w.code == SCENARIO_RANGE_INVALID for w in result.warnings)


# ---------------------------------------------------------------------------
# Confidence reduction
# ---------------------------------------------------------------------------


class TestConfidenceReduction:
    def test_errors_cause_flag_downgrade(self):
        # out-of-range score is an error → flag should be downgraded
        report = _make_report(overall_score=1.5, overall_flag="green")
        result = ValidationLayer().validate(report)
        # After clamping, score becomes 1.0 → green → downgraded to amber
        assert result.adjusted_report.overall_flag == "amber"

    def test_downgrade_message_added_to_errors(self):
        report = _make_report(overall_score=1.5, overall_flag="green")
        result = ValidationLayer().validate(report)
        assert any("downgraded" in e for e in result.adjusted_report.errors)

    def test_warnings_only_do_not_downgrade(self):
        # Only URL domain warning (not an error) — no downgrade
        fs = _make_factor_score(sources=["https://company.com/report.pdf"])
        report = _make_report(factor_scores=[fs], overall_flag="green")
        result = ValidationLayer().validate(report)
        assert result.adjusted_report.overall_flag == "green"

    def test_amber_report_with_errors_downgraded_to_red(self):
        fs = _make_factor_score(score=1.5, flag="green")
        report = _make_report(factor_scores=[fs], overall_score=0.50, overall_flag="amber")
        result = ValidationLayer().validate(report)
        assert result.adjusted_report.overall_flag == "red"


# ---------------------------------------------------------------------------
# Multiple factors
# ---------------------------------------------------------------------------


class TestMultipleFactors:
    def test_each_factor_validated_independently(self):
        fs1 = _make_factor_score("ghg_emissions", score=0.72, flag="green")
        fs2 = _make_factor_score("water_management", score=1.5, flag="green")
        report = _make_report(factor_scores=[fs1, fs2])
        result = ValidationLayer().validate(report)

        # fs1 should be fine
        assert result.adjusted_report.factor_scores[0].score == 0.72
        # fs2 score should be clamped
        assert result.adjusted_report.factor_scores[1].score == 1.0

    def test_errors_across_factors_accumulate(self):
        fs1 = _make_factor_score(score=1.5, flag="green")
        fs2 = _make_factor_score(score=1.5, flag="green")
        report = _make_report(factor_scores=[fs1, fs2])
        result = ValidationLayer().validate(report)
        score_errors = [w for w in result.warnings if w.code == SCORE_OUT_OF_RANGE]
        assert len(score_errors) >= 2
