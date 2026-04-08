from unittest.mock import patch

import pandas as pd
import pytest

from agents.data_gathering import DataGatherer
from pipeline.models import CompanyProfile, DataGathererResult, FilingMetadata

EDGAR_PROFILE = CompanyProfile(
    ticker="AAPL",
    name="APPLE INC",
    index=None,
    sic_code="3674",
    sic_description="Semiconductors and Related Devices",
    country="US",
    latest_annual_filing=FilingMetadata(
        filing_type="10-K",
        filed_date="2023-11-03",
        period_of_report="2023-09-30",
        document_url="https://www.sec.gov/Archives/edgar/data/320193/000032019323000106/aapl-20230930.htm",
    ),
    annual_report_text="Apple designs consumer electronics and software.",
    raw_financials={
        "revenue": 383285000000,
        "operating_income": 114301000000,
        "total_assets": 352583000000,
    },
    source_urls=[
        "https://www.sec.gov/Archives/edgar/data/320193/000032019323000106/aapl-20230930.htm"
    ],
    errors=[],
    identifier="0000320193",
)

CH_PROFILE = CompanyProfile(
    ticker=None,
    name="BP PLC",
    index=None,
    sic_code="19100",
    sic_description=None,
    country="GB",
    latest_annual_filing=FilingMetadata(
        filing_type="AA",
        filed_date="2023-03-31",
        period_of_report="2022-12-31",
        document_url="https://find-and-update.company-information.service.gov.uk/company/00102498/filing-history/abc",
    ),
    annual_report_text=None,
    raw_financials={},
    source_urls=[
        "https://find-and-update.company-information.service.gov.uk/company/00102498/filing-history/abc"
    ],
    errors=[],
    identifier="00102498",
)

EMPTY_DF = pd.DataFrame()


def _regulatory_mocks():
    """Returns patch decorators for all three regulatory fetchers."""
    return [
        patch("agents.data_gathering.NRCFetcher"),
        patch("agents.data_gathering.ECHOFetcher"),
        patch("agents.data_gathering.GHGRPFetcher"),
    ]


def _setup_regulatory_mocks(*mocks):
    """Makes all regulatory fetchers return empty DataFrames by default."""
    for m in mocks:
        m.return_value.fetch.return_value = EMPTY_DF


@patch("agents.data_gathering.NRCFetcher")
@patch("agents.data_gathering.ECHOFetcher")
@patch("agents.data_gathering.GHGRPFetcher")
@patch("agents.data_gathering.CompaniesHouseFetcher")
@patch("agents.data_gathering.EDGARFetcher")
def test_fetch_all_edgar_success(MockEDGAR, MockCH, MockGHGRP, MockECHO, MockNRC):
    MockEDGAR.return_value.fetch.return_value = EDGAR_PROFILE
    _setup_regulatory_mocks(MockGHGRP, MockECHO, MockNRC)

    gatherer = DataGatherer(sec_email="test@example.com", ch_api_key="key")
    result = gatherer.fetch_all(ticker="AAPL")

    assert isinstance(result, DataGathererResult)
    assert result.source_statuses["edgar"] == "success"
    assert result.profile is EDGAR_PROFILE


@patch("agents.data_gathering.NRCFetcher")
@patch("agents.data_gathering.ECHOFetcher")
@patch("agents.data_gathering.GHGRPFetcher")
@patch("agents.data_gathering.CompaniesHouseFetcher")
@patch("agents.data_gathering.EDGARFetcher")
def test_fetch_all_sets_index_on_profile(MockEDGAR, MockCH, MockGHGRP, MockECHO, MockNRC):
    MockEDGAR.return_value.fetch.return_value = EDGAR_PROFILE
    _setup_regulatory_mocks(MockGHGRP, MockECHO, MockNRC)

    gatherer = DataGatherer(sec_email="test@example.com", ch_api_key="key")
    result = gatherer.fetch_all(ticker="AAPL", index="SP500")

    assert result.profile.index == "SP500"


@patch("agents.data_gathering.NRCFetcher")
@patch("agents.data_gathering.ECHOFetcher")
@patch("agents.data_gathering.GHGRPFetcher")
@patch("agents.data_gathering.CompaniesHouseFetcher")
@patch("agents.data_gathering.EDGARFetcher")
def test_fetch_all_includes_ch_when_company_name_given(
    MockEDGAR, MockCH, MockGHGRP, MockECHO, MockNRC
):
    MockEDGAR.return_value.fetch.return_value = EDGAR_PROFILE
    MockCH.return_value.fetch.return_value = CH_PROFILE
    _setup_regulatory_mocks(MockGHGRP, MockECHO, MockNRC)

    gatherer = DataGatherer(sec_email="test@example.com", ch_api_key="key")
    result = gatherer.fetch_all(ticker="BP", company_name="BP PLC")

    assert result.source_statuses.get("companies_house") == "success"


@patch("agents.data_gathering.NRCFetcher")
@patch("agents.data_gathering.ECHOFetcher")
@patch("agents.data_gathering.GHGRPFetcher")
@patch("agents.data_gathering.CompaniesHouseFetcher")
@patch("agents.data_gathering.EDGARFetcher")
def test_fetch_all_edgar_failure_falls_back_to_ch(MockEDGAR, MockCH, MockGHGRP, MockECHO, MockNRC):
    MockEDGAR.return_value.fetch.side_effect = ValueError("CIK not found for BP")
    MockCH.return_value.fetch.return_value = CH_PROFILE

    gatherer = DataGatherer(sec_email="test@example.com", ch_api_key="key")
    result = gatherer.fetch_all(ticker="BP", company_name="BP PLC")

    assert result.source_statuses["edgar"].startswith("failed:")
    assert result.profile is CH_PROFILE
    # regulatory fetchers not called when EDGAR fails (UK company fallback)
    MockGHGRP.return_value.fetch.assert_not_called()


@patch("agents.data_gathering.NRCFetcher")
@patch("agents.data_gathering.ECHOFetcher")
@patch("agents.data_gathering.GHGRPFetcher")
@patch("agents.data_gathering.CompaniesHouseFetcher")
@patch("agents.data_gathering.EDGARFetcher")
def test_fetch_all_records_ch_failure(MockEDGAR, MockCH, MockGHGRP, MockECHO, MockNRC):
    MockEDGAR.return_value.fetch.return_value = EDGAR_PROFILE
    MockCH.return_value.fetch.side_effect = ValueError("Company not found: Unknown")
    _setup_regulatory_mocks(MockGHGRP, MockECHO, MockNRC)

    gatherer = DataGatherer(sec_email="test@example.com", ch_api_key="key")
    result = gatherer.fetch_all(ticker="AAPL", company_name="Unknown")

    assert result.source_statuses["companies_house"].startswith("failed:")
    assert result.profile is EDGAR_PROFILE


@patch("agents.data_gathering.NRCFetcher")
@patch("agents.data_gathering.ECHOFetcher")
@patch("agents.data_gathering.GHGRPFetcher")
@patch("agents.data_gathering.CompaniesHouseFetcher")
@patch("agents.data_gathering.EDGARFetcher")
def test_fetch_all_regulatory_populated_on_us_company(
    MockEDGAR, MockCH, MockGHGRP, MockECHO, MockNRC
):
    MockEDGAR.return_value.fetch.return_value = EDGAR_PROFILE
    ghgrp_df = pd.DataFrame({"facility_name": ["HQ"], "ghg_quantity_mtco2e": [32000.0]})
    MockGHGRP.return_value.fetch.return_value = ghgrp_df
    MockECHO.return_value.fetch.return_value = EMPTY_DF
    MockNRC.return_value.fetch.return_value = EMPTY_DF

    gatherer = DataGatherer(sec_email="test@example.com", ch_api_key="key")
    result = gatherer.fetch_all(ticker="AAPL")

    assert "ghgrp" in result.source_statuses
    assert result.source_statuses["ghgrp"].startswith("success")
    assert "ghgrp" in result.regulatory_paths


@patch("agents.data_gathering.NRCFetcher")
@patch("agents.data_gathering.ECHOFetcher")
@patch("agents.data_gathering.GHGRPFetcher")
@patch("agents.data_gathering.CompaniesHouseFetcher")
@patch("agents.data_gathering.EDGARFetcher")
def test_fetch_all_regulatory_failure_does_not_block(
    MockEDGAR, MockCH, MockGHGRP, MockECHO, MockNRC
):
    MockEDGAR.return_value.fetch.return_value = EDGAR_PROFILE
    MockGHGRP.return_value.fetch.side_effect = Exception("EPA API down")
    MockECHO.return_value.fetch.return_value = EMPTY_DF
    MockNRC.return_value.fetch.return_value = EMPTY_DF

    gatherer = DataGatherer(sec_email="test@example.com", ch_api_key="key")
    result = gatherer.fetch_all(ticker="AAPL")

    assert result.profile is EDGAR_PROFILE
    assert result.source_statuses["ghgrp"].startswith("failed:")


@patch("agents.data_gathering.NRCFetcher")
@patch("agents.data_gathering.ECHOFetcher")
@patch("agents.data_gathering.GHGRPFetcher")
@patch("agents.data_gathering.CompaniesHouseFetcher")
@patch("agents.data_gathering.EDGARFetcher")
def test_fetch_company_profile_returns_profile(MockEDGAR, MockCH, MockGHGRP, MockECHO, MockNRC):
    MockEDGAR.return_value.fetch.return_value = EDGAR_PROFILE
    _setup_regulatory_mocks(MockGHGRP, MockECHO, MockNRC)

    gatherer = DataGatherer(sec_email="test@example.com", ch_api_key="key")
    profile = gatherer.fetch_company_profile(ticker="AAPL", index="SP500")

    assert isinstance(profile, CompanyProfile)
    assert profile.ticker == "AAPL"
    assert profile.index == "SP500"


@patch("agents.data_gathering.NRCFetcher")
@patch("agents.data_gathering.ECHOFetcher")
@patch("agents.data_gathering.GHGRPFetcher")
@patch("agents.data_gathering.CompaniesHouseFetcher")
@patch("agents.data_gathering.EDGARFetcher")
def test_fetch_company_profile_raises_when_no_data(MockEDGAR, MockCH, MockGHGRP, MockECHO, MockNRC):
    MockEDGAR.return_value.fetch.side_effect = ValueError("CIK not found")

    gatherer = DataGatherer(sec_email="test@example.com", ch_api_key="key")
    with pytest.raises(ValueError, match="No data found"):
        gatherer.fetch_company_profile(ticker="UNKNOWN")
