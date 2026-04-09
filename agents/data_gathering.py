import logging
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
from pipeline.fetchers.pdf_extractor import PDFExtractor
from pipeline.models import CompanyProfile, DataGathererResult

load_dotenv()

LOGGER = logging.getLogger(__name__)


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
        LOGGER.info("fetch_all [1/6]: starting EDGAR fetch ticker=%s", ticker)
        try:
            edgar_profile = self.edgar.fetch(ticker)
            if index:
                edgar_profile.index = index
            source_statuses["edgar"] = "success"
        except Exception as e:
            source_statuses["edgar"] = f"failed: {e}"
            LOGGER.warning("fetch_all [1/6]: EDGAR failed ticker=%s: %s", ticker, e)
        LOGGER.info("fetch_all [1/6]: EDGAR done status=%s", source_statuses.get("edgar"))

        time.sleep(0.3)  # SEC rate limit

        # Companies House — UK annual accounts, SIC code
        if company_name:
            LOGGER.info("fetch_all [2/6]: starting Companies House fetch company=%r", company_name)
            try:
                ch_profile = self.ch.fetch(company_name)
                if index:
                    ch_profile.index = index
                source_statuses["companies_house"] = "success"
            except Exception as e:
                source_statuses["companies_house"] = f"failed: {e}"
                LOGGER.warning("fetch_all [2/6]: Companies House failed company=%r: %s", company_name, e)
            LOGGER.info("fetch_all [2/6]: Companies House done status=%s", source_statuses.get("companies_house"))
        else:
            LOGGER.info("fetch_all [2/6]: Companies House skipped (no company_name)")

        # PDF extraction — populate annual_report_text from the filing document URL.
        # Tried for whichever profile succeeded (EDGAR 10-K or CH annual accounts).
        # pdfplumber handles text-layer PDFs; Gemini 1.5 Pro handles scanned ones.
        _active_profile = edgar_profile or ch_profile
        if (
            _active_profile is not None
            and _active_profile.annual_report_text is None
            and _active_profile.latest_annual_filing is not None
            and _active_profile.latest_annual_filing.document_url
        ):
            LOGGER.info(
                "fetch_all [3/6]: starting PDF extraction url=%s",
                _active_profile.latest_annual_filing.document_url,
            )
            try:
                pdf_text = PDFExtractor().extract(
                    url=_active_profile.latest_annual_filing.document_url,
                    agent="data_gatherer",
                )
                _active_profile.annual_report_text = pdf_text or None
                source_statuses["pdf_extraction"] = (
                    f"success: {len(pdf_text):,} chars" if pdf_text else "success: empty"
                )
            except Exception as e:
                source_statuses["pdf_extraction"] = f"failed: {e}"
                LOGGER.warning("fetch_all [3/6]: PDF extraction failed: %s", e)
            LOGGER.info("fetch_all [3/6]: PDF extraction done status=%s", source_statuses.get("pdf_extraction"))
        else:
            LOGGER.info("fetch_all [3/6]: PDF extraction skipped (text already present or no filing URL)")

        # EPA / NRC regulatory fetchers — US companies only.
        # List is built here (not at module level) so @patch decorators work in tests.
        if edgar_profile is not None:
            name = edgar_profile.name
            for source, fetcher_cls in [
                ("ghgrp", GHGRPFetcher),
                ("echo", ECHOFetcher),
                ("nrc", NRCFetcher),
            ]:
                LOGGER.info("fetch_all [4/6]: starting %s fetch company=%r", source.upper(), name)
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
                    LOGGER.warning("fetch_all [4/6]: %s failed company=%r: %s", source.upper(), name, e)
                LOGGER.info("fetch_all [4/6]: %s done status=%s", source.upper(), source_statuses.get(source))
        else:
            LOGGER.info("fetch_all [4/6]: EPA/NRC fetchers skipped (no EDGAR profile)")

        # EA / EU ETS regulatory fetchers — UK/EU companies only.
        # List is built here (not at module level) so @patch decorators work in tests.
        if ch_profile is not None:
            name = ch_profile.name
            for source, fetcher_cls in [
                ("ea_pollution", EAPollutionFetcher),
                ("eu_ets", EUETSFetcher),
            ]:
                LOGGER.info("fetch_all [5/6]: starting %s fetch company=%r", source.upper(), name)
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
                    LOGGER.warning("fetch_all [5/6]: %s failed company=%r: %s", source.upper(), name, e)
                LOGGER.info("fetch_all [5/6]: %s done status=%s", source.upper(), source_statuses.get(source))
        else:
            LOGGER.info("fetch_all [5/6]: EA/EU ETS fetchers skipped (no Companies House profile)")

        # Prefer EDGAR profile; fall back to CH if EDGAR failed
        profile = edgar_profile or ch_profile
        LOGGER.info(
            "fetch_all [6/6]: complete ticker=%s profile_source=%s statuses=%s",
            ticker,
            "edgar" if edgar_profile else "companies_house" if ch_profile else "none",
            source_statuses,
        )

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
