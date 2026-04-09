import json
from unittest.mock import MagicMock, patch

import pytest

from pipeline.fetchers.sasb_topics import (
    _gic_to_factor_id,
    _normalise_dimension,
    flatten_topics,
    get_industry_topics,
    to_material_factor_dicts,
)

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

_INDUSTRY_RESPONSE = [
    {
        "industry_code": "EM-EP",
        "industry_name": "Oil & Gas – Exploration & Production",
        "industry_gics": [
            {
                "gic_name": "GHG Emissions",
                "gic_dimension": "Environment",
                "gic_description": "Gross global Scope 1 emissions and management of emissions.",
                "gic_topics": [
                    {"topic_code": "EM-EP-110a.1", "topic_name": "GHG metric tons"},
                    {"topic_code": "EM-EP-110a.2", "topic_name": "Reduction targets"},
                ],
            },
            {
                "gic_name": "Water & Wastewater Management",
                "gic_dimension": "Environment",
                "gic_description": "Total fresh water withdrawn and percentage recycled.",
                "gic_topics": [
                    {"topic_code": "EM-EP-140a.1", "topic_name": "Water withdrawn"},
                ],
            },
            {
                "gic_name": "Critical Incident Risk Management",
                "gic_dimension": "Leadership and Governance",
                "gic_description": "Process safety events and asset integrity management.",
                "gic_topics": [
                    {"topic_code": "EM-EP-540a.1", "topic_name": "Process safety events"},
                ],
            },
        ],
    }
]

# A GIC with no topics — should be skipped by flatten_topics
_INDUSTRY_WITH_EMPTY_GIC = [
    {
        "industry_gics": [
            {
                "gic_name": "Valid Factor",
                "gic_dimension": "Environment",
                "gic_description": "A valid factor with topics.",
                "gic_topics": [{"topic_code": "X-1", "topic_name": "Metric"}],
            },
            {
                "gic_name": "No Topics Factor",
                "gic_dimension": "Social Capital",
                "gic_description": "Missing topics.",
                "gic_topics": [],
            },
        ]
    }
]


# ---------------------------------------------------------------------------
# Pure utility functions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("gic_name", "expected"),
    [
        ("GHG Emissions", "ghg_emissions"),
        ("Water & Wastewater Management", "water_wastewater_management"),
        ("Critical Incident Risk Management", "critical_incident_risk_management"),
        ("Employee Health & Safety", "employee_health_safety"),
        ("Supply Chain Management", "supply_chain_management"),
    ],
)
def test_gic_to_factor_id(gic_name, expected):
    assert _gic_to_factor_id(gic_name) == expected


@pytest.mark.parametrize(
    ("dim_in", "dim_out"),
    [
        ("Leadership and Governance", "Leadership & Governance"),
        ("Business Model and Innovation", "Business Model & Innovation"),
        ("Environment", "Environment"),
        ("Social Capital", "Social Capital"),
        ("Human Capital", "Human Capital"),
    ],
)
def test_normalise_dimension(dim_in, dim_out):
    assert _normalise_dimension(dim_in) == dim_out


# ---------------------------------------------------------------------------
# flatten_topics
# ---------------------------------------------------------------------------


def test_flatten_topics_returns_one_entry_per_gic():
    flat = flatten_topics(_INDUSTRY_RESPONSE[0])
    assert len(flat) == 3


def test_flatten_topics_fields():
    flat = flatten_topics(_INDUSTRY_RESPONSE[0])
    ghg = flat[0]
    assert ghg["factor_id"] == "ghg_emissions"
    assert ghg["factor_name"] == "GHG Emissions"
    assert ghg["dimension"] == "Environment"
    assert ghg["description"] == "Gross global Scope 1 emissions and management of emissions."
    assert ghg["topic_code"] == "EM-EP-110a.1"
    assert ghg["topic_codes"] == ["EM-EP-110a.1", "EM-EP-110a.2"]


def test_flatten_topics_normalises_dimension():
    flat = flatten_topics(_INDUSTRY_RESPONSE[0])
    cirm = flat[2]
    assert cirm["dimension"] == "Leadership & Governance"


def test_flatten_topics_skips_gics_with_no_topics():
    flat = flatten_topics(_INDUSTRY_WITH_EMPTY_GIC[0])
    assert len(flat) == 1
    assert flat[0]["factor_id"] == "valid_factor"


def test_flatten_topics_empty_industry():
    flat = flatten_topics({"industry_gics": []})
    assert flat == []


# ---------------------------------------------------------------------------
# to_material_factor_dicts
# ---------------------------------------------------------------------------


def test_to_material_factor_dicts_infers_financial_impacts():
    factors = to_material_factor_dicts(_INDUSTRY_RESPONSE[0])
    assert len(factors) == 3

    ghg = factors[0]
    assert ghg["factor_id"] == "ghg_emissions"
    assert "cost_impact" in ghg["financial_impacts"]  # Environment dimension

    cirm = factors[2]
    assert "liability_impact" in cirm["financial_impacts"]  # Leadership & Governance


# ---------------------------------------------------------------------------
# get_industry_topics — API call mocking
# ---------------------------------------------------------------------------


@patch("pipeline.fetchers.sasb_topics.requests.get")
def test_get_industry_topics_calls_api_and_returns_data(mock_get, tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.fetchers.sasb_topics.INDUSTRY_DIR", tmp_path / "industries")

    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = _INDUSTRY_RESPONSE
    mock_get.return_value = resp

    result = get_industry_topics("EM-EP")

    assert result["industry_code"] == "EM-EP"
    mock_get.assert_called_once()


@patch("pipeline.fetchers.sasb_topics.requests.get")
def test_get_industry_topics_writes_cache(mock_get, tmp_path, monkeypatch):
    industry_dir = tmp_path / "industries"
    monkeypatch.setattr("pipeline.fetchers.sasb_topics.INDUSTRY_DIR", industry_dir)

    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = _INDUSTRY_RESPONSE
    mock_get.return_value = resp

    get_industry_topics("EM-EP")

    cache_file = industry_dir / "EM-EP.json"
    assert cache_file.exists()
    cached = json.loads(cache_file.read_text())
    assert cached["data"]["industry_code"] == "EM-EP"
    assert "fetched_at" in cached


@patch("pipeline.fetchers.sasb_topics.requests.get")
def test_get_industry_topics_falls_back_to_cache_on_api_failure(mock_get, tmp_path, monkeypatch):
    industry_dir = tmp_path / "industries"
    industry_dir.mkdir(parents=True)
    monkeypatch.setattr("pipeline.fetchers.sasb_topics.INDUSTRY_DIR", industry_dir)

    # Pre-populate cache
    cache_file = industry_dir / "EM-EP.json"
    cache_file.write_text(
        json.dumps({"fetched_at": "2026-01-01T00:00:00Z", "data": _INDUSTRY_RESPONSE[0]})
    )

    mock_get.side_effect = Exception("API unreachable")

    result = get_industry_topics("EM-EP")

    assert result["industry_code"] == "EM-EP"


@patch("pipeline.fetchers.sasb_topics.requests.get")
def test_get_industry_topics_raises_when_api_fails_and_no_cache(mock_get, tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.fetchers.sasb_topics.INDUSTRY_DIR", tmp_path / "industries")
    mock_get.side_effect = Exception("API unreachable")

    with pytest.raises(RuntimeError, match="Failed to fetch industry"):
        get_industry_topics("EM-EP")


@patch("pipeline.fetchers.sasb_topics.requests.get")
def test_get_industry_topics_raises_on_unexpected_response_shape(mock_get, tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.fetchers.sasb_topics.INDUSTRY_DIR", tmp_path / "industries")

    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {"not": "a list"}
    mock_get.return_value = resp

    with pytest.raises(RuntimeError, match="Failed to fetch industry"):
        get_industry_topics("EM-EP")
