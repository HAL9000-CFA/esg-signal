from unittest.mock import MagicMock, patch

import pytest

from pipeline.fetchers.edgar import EDGARFetcher
from pipeline.models import CompanyProfile, FilingMetadata

TICKER_MAP = {
    "0": {"cik_str": 789019, "ticker": "MSFT", "title": "MICROSOFT CORP"},
}

SUBMISSIONS = {
    "name": "MICROSOFT CORP",
    "sic": "7372",
    "sicDescription": "Prepackaged Software",
    "addresses": {"business": {"stateOrCountry": "WA"}},
    "filings": {
        "recent": {
            "form": ["10-K", "10-Q"],
            "accessionNumber": ["0001564590-23-039431", "0001564590-23-011111"],
            "filingDate": ["2023-07-27", "2023-04-25"],
            "periodOfReport": ["2023-06-30", "2023-03-31"],
            "primaryDocument": ["msft-20230630.htm", "msft-20230331.htm"],
        }
    },
}

XBRL = {
    "facts": {
        "us-gaap": {
            "Revenues": {
                "units": {
                    "USD": [
                        {"end": "2023-06-30", "val": 211915000000, "form": "10-K"},
                        {"end": "2022-06-30", "val": 198270000000, "form": "10-K"},
                    ]
                }
            },
            "OperatingIncomeLoss": {
                "units": {"USD": [{"end": "2023-06-30", "val": 88523000000, "form": "10-K"}]}
            },
            "Assets": {
                "units": {"USD": [{"end": "2023-06-30", "val": 411976000000, "form": "10-K"}]}
            },
        }
    }
}

DOCUMENT = """
<html><body>
Item 1. Business Microsoft develops software.
Item 1A. Risk Factors
Cybersecurity risks are significant. Revenue depends on cloud adoption.
Item 1B. Unresolved Staff Comments None.
Item 2. Properties We lease office space.
</body></html>
"""


def make_mock_get(ticker_map=TICKER_MAP, submissions=SUBMISSIONS, xbrl=XBRL, document=DOCUMENT):
    def side_effect(url, **kwargs):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if "company_tickers" in url:
            resp.json.return_value = ticker_map
        elif "submissions" in url:
            resp.json.return_value = submissions
        elif "xbrl" in url:
            resp.json.return_value = xbrl
        elif "Archives" in url:
            resp.text = document
        return resp

    return side_effect


@patch("pipeline.fetchers.edgar.requests.get")
def test_fetch_returns_company_profile(mock_get):
    mock_get.side_effect = make_mock_get()

    fetcher = EDGARFetcher(user_email="test@example.com")
    profile = fetcher.fetch("MSFT")

    assert isinstance(profile, CompanyProfile)
    assert profile.ticker == "MSFT"
    assert profile.name == "MICROSOFT CORP"
    assert profile.country == "US"
    assert profile.identifier == "0000789019"


@patch("pipeline.fetchers.edgar.requests.get")
def test_fetch_extracts_sic_code(mock_get):
    mock_get.side_effect = make_mock_get()

    fetcher = EDGARFetcher(user_email="test@example.com")
    profile = fetcher.fetch("MSFT")

    assert profile.sic_code == "7372"
    assert profile.sic_description == "Prepackaged Software"


@patch("pipeline.fetchers.edgar.requests.get")
def test_fetch_builds_filing_metadata(mock_get):
    mock_get.side_effect = make_mock_get()

    fetcher = EDGARFetcher(user_email="test@example.com")
    profile = fetcher.fetch("MSFT")

    assert isinstance(profile.latest_annual_filing, FilingMetadata)
    assert profile.latest_annual_filing.filing_type == "10-K"
    assert profile.latest_annual_filing.filed_date == "2023-07-27"
    assert profile.latest_annual_filing.period_of_report == "2023-06-30"
    assert "Archives" in profile.latest_annual_filing.document_url
    assert profile.latest_annual_filing.document_url in profile.source_urls


@patch("pipeline.fetchers.edgar.requests.get")
def test_fetch_extracts_risk_factors(mock_get):
    mock_get.side_effect = make_mock_get()

    fetcher = EDGARFetcher(user_email="test@example.com")
    profile = fetcher.fetch("MSFT")

    assert profile.annual_report_text is not None
    assert "Cybersecurity" in profile.annual_report_text


@patch("pipeline.fetchers.edgar.requests.get")
def test_fetch_extracts_financials(mock_get):
    mock_get.side_effect = make_mock_get()

    fetcher = EDGARFetcher(user_email="test@example.com")
    profile = fetcher.fetch("MSFT")

    assert profile.raw_financials["revenue"] == 211915000000
    assert profile.raw_financials["operating_income"] == 88523000000
    assert profile.raw_financials["total_assets"] == 411976000000


@patch("pipeline.fetchers.edgar.requests.get")
def test_fetch_unknown_ticker_raises(mock_get):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = TICKER_MAP
    mock_get.return_value = resp

    fetcher = EDGARFetcher(user_email="test@example.com")
    with pytest.raises(ValueError, match="CIK not found"):
        fetcher.fetch("UNKNOWN")


@patch("pipeline.fetchers.edgar.requests.get")
def test_fetch_records_error_when_document_download_fails(mock_get):
    def side_effect(url, **kwargs):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if "company_tickers" in url:
            resp.json.return_value = TICKER_MAP
        elif "submissions" in url:
            resp.json.return_value = SUBMISSIONS
        elif "xbrl" in url:
            resp.json.return_value = XBRL
        elif "Archives" in url:
            resp.raise_for_status.side_effect = Exception("timeout")
        return resp

    mock_get.side_effect = side_effect

    fetcher = EDGARFetcher(user_email="test@example.com")
    profile = fetcher.fetch("MSFT")

    assert any("Failed to download" in e for e in profile.errors)
    assert profile.annual_report_text == ""


@patch("pipeline.fetchers.edgar.requests.get")
def test_fetch_records_error_when_no_10k_found(mock_get):
    submissions_no_10k = {
        **SUBMISSIONS,
        "filings": {
            "recent": {
                "form": ["10-Q", "8-K"],
                "accessionNumber": ["0001111111-23-000001", "0001111111-23-000002"],
                "filingDate": ["2023-04-25", "2023-03-01"],
                "periodOfReport": ["2023-03-31", ""],
                "primaryDocument": ["doc1.htm", "doc2.htm"],
            }
        },
    }

    def side_effect(url, **kwargs):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if "company_tickers" in url:
            resp.json.return_value = TICKER_MAP
        elif "submissions" in url:
            resp.json.return_value = submissions_no_10k
        elif "xbrl" in url:
            resp.json.return_value = XBRL
        return resp

    mock_get.side_effect = side_effect

    fetcher = EDGARFetcher(user_email="test@example.com")
    profile = fetcher.fetch("MSFT")

    assert profile.latest_annual_filing is None
    assert any("No 10-K" in e for e in profile.errors)
