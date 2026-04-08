import os
import time

from dotenv import load_dotenv

from pipeline.fetchers.companies_house import CompaniesHouseFetcher
from pipeline.fetchers.edgar import EDGARFetcher
from pipeline.models import CompanyProfile, DataGathererResult

load_dotenv()


class DataGatherer:
    def __init__(self, sec_email: str = None, ch_api_key: str = None):
        self.edgar = EDGARFetcher(sec_email or os.getenv("SEC_EMAIL"))
        self.ch = CompaniesHouseFetcher(ch_api_key or os.getenv("COMPANIES_HOUSE_API_KEY"))

    def fetch_all(
        self,
        ticker: str,
        company_name: str = None,
        index: str = None,
    ) -> DataGathererResult:
        """
        Fetches from EDGAR (US) and optionally Companies House (UK).

        Args:
            ticker: stock ticker, used for EDGAR CIK lookup
            company_name: company name for Companies House search (UK companies only)
            index: "SP500" or "FTSE100" — stored on the profile for downstream agents

        Returns DataGathererResult with:
            .profile — CompanyProfile (EDGAR preferred, CH fallback)
            .source_statuses — {"edgar": "success", "companies_house": "failed: ..."}
        """
        source_statuses = {}
        edgar_profile = None
        ch_profile = None

        # EDGAR — US annual report (10-K), financials, SIC code
        try:
            edgar_profile = self.edgar.fetch(ticker)
            if index:
                edgar_profile.index = index
            source_statuses["edgar"] = "success"
        except Exception as e:
            source_statuses["edgar"] = f"failed: {e}"

        time.sleep(0.3)  # SEC rate limit

        # Companies House — UK annual accounts, SIC code
        if company_name:
            try:
                ch_profile = self.ch.fetch(company_name)
                if index:
                    ch_profile.index = index
                source_statuses["companies_house"] = "success"
            except Exception as e:
                source_statuses["companies_house"] = f"failed: {e}"

        # Prefer EDGAR profile; fall back to CH if EDGAR failed
        profile = edgar_profile or ch_profile

        return DataGathererResult(profile=profile, source_statuses=source_statuses)

    def fetch_company_profile(
        self,
        ticker: str,
        company_name: str = None,
        index: str = None,
    ) -> CompanyProfile:
        """
        Main entry point for downstream agents (relevance_filter, credibility_scorer, etc.).
        Returns a CompanyProfile combining the best available data.
        """
        result = self.fetch_all(ticker=ticker, company_name=company_name, index=index)
        if result.profile is None:
            raise ValueError(f"No data found for ticker={ticker!r} company_name={company_name!r}")
        return result.profile
