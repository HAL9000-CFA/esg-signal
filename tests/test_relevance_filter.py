"""
Unit tests for RelevanceFilter.

Tests use an injected minimal SASB map JSON so they are not coupled to the
exact content of data/sasb_map.json, but one integration test does load the
real file to verify its structure is valid.
"""

import json

import pytest

from agents.relevance_filter import RelevanceFilter
from pipeline.models import CompanyProfile

# ---------------------------------------------------------------------------
# Minimal test SASB map (injected via tmp_path)
# ---------------------------------------------------------------------------

_TEST_MAP = {
    "version": "test",
    "note": "test map",
    "industries": {
        "software_it_services": {
            "name": "Software & IT Services",
            "sasb_sector": "Technology & Communications",
            "sic_codes": ["7372", "7371"],
            "material_factors": {
                "data_security": {
                    "name": "Data Security",
                    "dimension": "Social Capital",
                    "financial_impacts": ["revenue_impact", "liability_impact"],
                },
                "energy_management": {
                    "name": "Energy Management",
                    "dimension": "Environment",
                    "financial_impacts": ["cost_impact"],
                },
            },
        },
        "oil_gas_ep": {
            "name": "Oil & Gas — Exploration & Production",
            "sasb_sector": "Extractives & Minerals Processing",
            "sic_codes": ["1311"],
            "material_factors": {
                "ghg_emissions": {
                    "name": "GHG Emissions",
                    "dimension": "Environment",
                    "financial_impacts": ["cost_impact", "asset_impact"],
                },
            },
        },
    },
    "fallback_factors": {
        "business_ethics": {
            "name": "Business Ethics",
            "dimension": "Leadership & Governance",
            "financial_impacts": ["liability_impact"],
        },
        "labor_practices": {
            "name": "Labor Practices",
            "dimension": "Human Capital",
            "financial_impacts": ["cost_impact"],
        },
    },
}


@pytest.fixture
def map_path(tmp_path):
    path = tmp_path / "sasb_map.json"
    path.write_text(json.dumps(_TEST_MAP))
    return str(path)


@pytest.fixture
def rf(map_path):
    return RelevanceFilter(sasb_map_path=map_path)


def _make_profile(sic_code=None, ticker="TEST"):
    return CompanyProfile(
        ticker=ticker,
        name="Test Corp",
        index="SP500",
        sic_code=sic_code,
        sic_description=None,
        country="US",
        latest_annual_filing=None,
        annual_report_text=None,
        raw_financials={},
        source_urls=[],
        errors=[],
    )


# ---------------------------------------------------------------------------
# Known SIC — happy path
# ---------------------------------------------------------------------------


class TestKnownSic:
    def test_returns_correct_industry(self, rf):
        result = rf.filter(_make_profile(sic_code="7372"))
        assert result.sasb_industry == "Software & IT Services"

    def test_returns_material_factors(self, rf):
        result = rf.filter(_make_profile(sic_code="7372"))
        assert len(result.material_factors) == 2

    def test_factor_ids_present(self, rf):
        result = rf.filter(_make_profile(sic_code="7372"))
        ids = {f.factor_id for f in result.material_factors}
        assert "data_security" in ids
        assert "energy_management" in ids

    def test_no_errors_on_known_sic(self, rf):
        result = rf.filter(_make_profile(sic_code="7372"))
        assert result.errors == []

    def test_ticker_propagated(self, rf):
        result = rf.filter(_make_profile(sic_code="7372", ticker="MSFT"))
        assert result.ticker == "MSFT"

    def test_sic_code_normalised_to_four_digits(self, rf):
        # SIC "372" should be zero-padded to "0372" — not in map, gets fallback
        result = rf.filter(_make_profile(sic_code="372"))
        assert result.sic_code == "0372"

    def test_different_industry_sic(self, rf):
        result = rf.filter(_make_profile(sic_code="1311"))
        assert result.sasb_industry == "Oil & Gas — Exploration & Production"
        ids = {f.factor_id for f in result.material_factors}
        assert "ghg_emissions" in ids


# ---------------------------------------------------------------------------
# Unknown SIC — fallback
# ---------------------------------------------------------------------------


class TestUnknownSic:
    def test_returns_fallback_factors(self, rf):
        result = rf.filter(_make_profile(sic_code="9999"))
        fallback_ids = {f.factor_id for f in result.material_factors}
        assert "business_ethics" in fallback_ids
        assert "labor_practices" in fallback_ids

    def test_sasb_industry_is_none(self, rf):
        result = rf.filter(_make_profile(sic_code="9999"))
        assert result.sasb_industry is None

    def test_error_message_mentions_sic(self, rf):
        result = rf.filter(_make_profile(sic_code="9999"))
        assert any("9999" in e for e in result.errors)


# ---------------------------------------------------------------------------
# Null SIC — fallback
# ---------------------------------------------------------------------------


class TestNullSic:
    def test_returns_fallback_factors(self, rf):
        result = rf.filter(_make_profile(sic_code=None))
        assert len(result.material_factors) > 0

    def test_sic_code_is_none_in_result(self, rf):
        result = rf.filter(_make_profile(sic_code=None))
        assert result.sic_code is None

    def test_error_mentions_sic_unavailable(self, rf):
        result = rf.filter(_make_profile(sic_code=None))
        assert len(result.errors) > 0
        assert any("SIC" in e for e in result.errors)


# ---------------------------------------------------------------------------
# MaterialFactor structure
# ---------------------------------------------------------------------------


class TestMaterialFactorStructure:
    def test_all_factors_have_required_fields(self, rf):
        result = rf.filter(_make_profile(sic_code="7372"))
        for factor in result.material_factors:
            assert factor.factor_id
            assert factor.name
            assert factor.dimension
            assert isinstance(factor.financial_impacts, list)
            assert len(factor.financial_impacts) > 0

    def test_financial_impacts_are_valid_values(self, rf):
        valid = {"revenue_impact", "cost_impact", "asset_impact", "liability_impact"}
        result = rf.filter(_make_profile(sic_code="7372"))
        for factor in result.material_factors:
            for impact in factor.financial_impacts:
                assert impact in valid


# ---------------------------------------------------------------------------
# Integration — real sasb_map.json
# ---------------------------------------------------------------------------


class TestRealSasbMap:
    def test_real_map_loads_without_error(self):
        rf = RelevanceFilter()  # uses default data/sasb_map.json
        assert len(rf._industries) > 0

    def test_software_sic_maps_correctly(self):
        rf = RelevanceFilter()
        result = rf.filter(_make_profile(sic_code="7372"))
        assert result.sasb_industry == "Software & IT Services"
        assert result.errors == []

    def test_oil_gas_sic_maps_correctly(self):
        rf = RelevanceFilter()
        result = rf.filter(_make_profile(sic_code="1311"))
        assert "Exploration" in result.sasb_industry
        ids = {f.factor_id for f in result.material_factors}
        assert "ghg_emissions" in ids

    def test_all_industries_have_at_least_one_factor(self):
        rf = RelevanceFilter()
        for industry_id, data in rf._industries.items():
            assert (
                len(data["material_factors"]) >= 1
            ), f"Industry '{industry_id}' has no material factors"

    def test_all_financial_impacts_are_valid(self):
        valid = {"revenue_impact", "cost_impact", "asset_impact", "liability_impact"}
        rf = RelevanceFilter()
        for industry_id, data in rf._industries.items():
            for factor_id, factor in data["material_factors"].items():
                for impact in factor["financial_impacts"]:
                    assert (
                        impact in valid
                    ), f"Invalid impact '{impact}' in {industry_id}.{factor_id}"
