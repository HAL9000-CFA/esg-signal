import time
from datetime import datetime

from pipeline.fetchers.companies_house import CompaniesHouseFetcher
from pipeline.fetchers.edgar import EDGARFetcher
from pipeline.models import DataGathererResult


class DataGatherer:
    def __init__(self, sec_email: str, ch_api_key: str):
        self.edgar = EDGARFetcher(sec_email)
        self.ch = CompaniesHouseFetcher(ch_api_key)

    def fetch_all(
        self,
        ticker: str,
        company_name: str = None,
    ) -> DataGathererResult:

        results = {
            "ticker": ticker,
            "company_name": company_name or ticker,
            "timestamp": datetime.now().isoformat(),
            "sources": {},
        }

        # EDGAR
        try:
            edgar_profile = self.edgar.fetch(ticker)
            results["sources"]["edgar"] = {
                "status": "success",
                "data": edgar_profile.extra,
            }
        except Exception as e:
            results["sources"]["edgar"] = {
                "status": "failed",
                "error": str(e),
            }
            edgar_profile = None

        time.sleep(0.3)

        # Companies House
        if company_name:
            try:
                ch_profile = self.ch.fetch(company_name)
                results["sources"]["companies_house"] = {
                    "status": "success",
                    "data": ch_profile.extra,
                }
            except Exception as e:
                results["sources"]["companies_house"] = {
                    "status": "failed",
                    "error": str(e),
                }

        # profile = primary structured output (prefer EDGAR)
        profile = edgar_profile or ch_profile

        return DataGathererResult(
            profile=profile,
            sources=results["sources"],
        )
