from unittest.mock import MagicMock, patch

import pytest

from pipeline.fetchers.companies_house import CompaniesHouseFetcher
from pipeline.models import CompanyProfile, FilingMetadata

SEARCH_RESPONSE = {"items": [{"company_number": "00445790", "title": "TESCO PLC"}]}

PROFILE_RESPONSE = {
    "company_name": "TESCO PLC",
    "company_number": "00445790",
    "company_status": "active",
    "sic_codes": ["47110"],
    "registered_office_address": {
        "address_line_1": "Tesco House",
        "locality": "Welwyn Garden City",
        "country": "England",
    },
}

FILING_HISTORY_RESPONSE = {
    "items": [
        {
            "type": "AA",
            "date": "2023-06-28",
            "description": "full-accounts-made-up-to-2023-02-25",
            "description_values": {"made_up_date": "2023-02-25"},
            "links": {
                "self": "/company/00445790/filing-history/MzM5ODY1MjA1OWFkaXF6a2N4",
            },
        },
        {
            "type": "CS01",
            "date": "2023-10-01",
            "description": "confirmation-statement-with-no-updates",
            "links": {"self": "/company/00445790/filing-history/abc123"},
        },
    ]
}


def make_mock_get():
    def side_effect(url, **kwargs):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if "/search/companies" in url:
            resp.json.return_value = SEARCH_RESPONSE
        elif "/filing-history" in url:
            resp.json.return_value = FILING_HISTORY_RESPONSE
        elif "/company/" in url:
            resp.json.return_value = PROFILE_RESPONSE
        return resp

    return side_effect


@patch("pipeline.fetchers.companies_house.requests.get")
def test_fetch_returns_company_profile(mock_get):
    mock_get.side_effect = make_mock_get()

    fetcher = CompaniesHouseFetcher(api_key="test_key")
    profile = fetcher.fetch("Tesco")

    assert isinstance(profile, CompanyProfile)
    assert profile.name == "TESCO PLC"
    assert profile.country == "GB"
    assert profile.identifier == "00445790"
    assert profile.ticker is None


@patch("pipeline.fetchers.companies_house.requests.get")
def test_fetch_extracts_sic_code(mock_get):
    mock_get.side_effect = make_mock_get()

    fetcher = CompaniesHouseFetcher(api_key="test_key")
    profile = fetcher.fetch("Tesco")

    assert profile.sic_code == "47110"


@patch("pipeline.fetchers.companies_house.requests.get")
def test_fetch_builds_filing_metadata(mock_get):
    mock_get.side_effect = make_mock_get()

    fetcher = CompaniesHouseFetcher(api_key="test_key")
    profile = fetcher.fetch("Tesco")

    assert isinstance(profile.latest_annual_filing, FilingMetadata)
    assert profile.latest_annual_filing.filing_type == "AA"
    assert profile.latest_annual_filing.filed_date == "2023-06-28"
    assert profile.latest_annual_filing.period_of_report == "2023-02-25"
    assert "00445790" in profile.latest_annual_filing.document_url


@patch("pipeline.fetchers.companies_house.requests.get")
def test_fetch_company_not_found_raises(mock_get):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {"items": []}
    mock_get.return_value = resp

    fetcher = CompaniesHouseFetcher(api_key="test_key")
    with pytest.raises(ValueError, match="Company not found"):
        fetcher.fetch("Unknown Company XYZ")


@patch("pipeline.fetchers.companies_house.requests.get")
def test_fetch_records_error_when_no_aa_filing(mock_get):
    no_aa_history = {
        "items": [
            {
                "type": "CS01",
                "date": "2023-10-01",
                "links": {"self": "/company/00445790/filing-history/abc123"},
            }
        ]
    }

    def side_effect(url, **kwargs):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if "/search/companies" in url:
            resp.json.return_value = SEARCH_RESPONSE
        elif "/filing-history" in url:
            resp.json.return_value = no_aa_history
        elif "/company/" in url:
            resp.json.return_value = PROFILE_RESPONSE
        return resp

    mock_get.side_effect = side_effect

    fetcher = CompaniesHouseFetcher(api_key="test_key")
    profile = fetcher.fetch("Tesco")

    assert profile.latest_annual_filing is None
    assert any("No annual accounts" in e for e in profile.errors)
