import os

from pipeline.fetchers.companies_house import CompaniesHouseFetcher


def test_companies_house_fetch():
    api_key = os.getenv("CH_API_KEY")
    assert api_key, "Set CH_API_KEY env var"

    fetcher = CompaniesHouseFetcher(api_key)

    profile = fetcher.fetch("Tesco")

    assert profile.identifier is not None
    assert profile.country == "UK"
    assert "company_name" in profile.extra
