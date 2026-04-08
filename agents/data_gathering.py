import os
import time

from dotenv import load_dotenv

from pipeline.fetchers.companies_house import CompaniesHouseFetcher
from pipeline.fetchers.ea_pollution import EAPollutionFetcher
from pipeline.fetchers.echo import ECHOFetcher
from pipeline.fetchers.edgar import EDGARFetcher
from pipeline.fetchers.eu_ets import EUETSFetcher
from pipeline.fetchers.ghgrp import GHGRPFetcher
from pipeline.fetchers.nrc import NRCFetcher
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
        For US companies, also fetches EPA GHGRP, ECHO, and NRC regulatory data.

        Args:
            ticker: stock ticker, used for EDGAR CIK lookup
            company_name: company name for Companies House search (UK companies only)
            index: "SP500" or "FTSE100" — stored on the profile for downstream agents

        Returns DataGathererResult with:
            .profile            — CompanyProfile (EDGAR preferred, CH fallback)
            .source_statuses    — {"edgar": "success", "ghgrp": "failed: ...", ...}
            .regulatory_paths   — {"ghgrp": "data/processed/ghgrp_AAPL.csv", ...}
        """
        source_statuses = {}
        regulatory_paths = {}
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

        # EPA / NRC regulatory fetchers — US companies only.
        # List is built here (not at module level) so @patch decorators work in tests.
        if edgar_profile is not None:
            name = edgar_profile.name
            for source, fetcher_cls in [
                ("ghgrp", GHGRPFetcher),
                ("echo", ECHOFetcher),
                ("nrc", NRCFetcher),
            ]:
                try:
                    fetcher = fetcher_cls()
                    df = fetcher.fetch(company_name=name)
                    if df.empty:
                        source_statuses[source] = "success: no records found"
                    else:
                        raw_path = f"data/raw/{source}_{ticker}.csv"
                        processed_path = f"data/processed/{source}_{ticker}.csv"
                        fetcher.save(df, raw_path=raw_path, processed_path=processed_path)
                        source_statuses[source] = f"success: {len(df)} records"
                        regulatory_paths[source] = processed_path
                except Exception as e:
                    source_statuses[source] = f"failed: {e}"

        # EA / EU ETS regulatory fetchers — UK/EU companies only.
        # List is built here (not at module level) so @patch decorators work in tests.
        if ch_profile is not None:
            name = ch_profile.name
            for source, fetcher_cls in [
                ("ea_pollution", EAPollutionFetcher),
                ("eu_ets", EUETSFetcher),
            ]:
                try:
                    fetcher = fetcher_cls()
                    df = fetcher.fetch(company_name=name)
                    if df.empty:
                        source_statuses[source] = "success: no records found"
                    else:
                        raw_path = f"data/raw/{source}_{ticker}.csv"
                        processed_path = f"data/processed/{source}_{ticker}.csv"
                        fetcher.save(df, raw_path=raw_path, processed_path=processed_path)
                        source_statuses[source] = f"success: {len(df)} records"
                        regulatory_paths[source] = processed_path
                except Exception as e:
                    source_statuses[source] = f"failed: {e}"

        # Prefer EDGAR profile; fall back to CH if EDGAR failed
        profile = edgar_profile or ch_profile

        return DataGathererResult(
            profile=profile,
            source_statuses=source_statuses,
            regulatory_paths=regulatory_paths,
        )

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
