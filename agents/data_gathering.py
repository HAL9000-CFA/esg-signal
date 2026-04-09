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
        _ch_key = (ch_api_key or os.getenv("COMPANIES_HOUSE_API_KEY", "")).strip()
        if _ch_key:
            try:
                self.ch = CompaniesHouseFetcher(_ch_key)
            except Exception as exc:
                LOGGER.warning("DataGatherer: Companies House init failed (%s) — CH disabled", exc)
                self.ch = None
        else:
            LOGGER.info("DataGatherer: COMPANIES_HOUSE_API_KEY not set — CH fetcher disabled")
            self.ch = None

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
        # company_name is passed so EDGARFetcher can fall back to name-based CIK search
        # for FTSE100 companies whose LSE ticker differs from their SEC filing ticker
        # (e.g. ULVR → Unilever PLC → 20-F filer with CIK found via EFTS name search)
        LOGGER.info("fetch_all [1/6]: starting EDGAR fetch ticker=%s", ticker)
        try:
            edgar_profile = self.edgar.fetch(ticker, company_name=company_name)
            if index:
                edgar_profile.index = index
            source_statuses["edgar"] = "success"
        except Exception as e:
            source_statuses["edgar"] = f"failed: {e}"
            LOGGER.warning("fetch_all [1/6]: EDGAR failed ticker=%s: %s", ticker, e)
        LOGGER.info("fetch_all [1/6]: EDGAR done status=%s", source_statuses.get("edgar"))

        time.sleep(0.3)  # SEC rate limit

        # Companies House — UK annual accounts, SIC code
        if company_name and self.ch is not None:
            LOGGER.info("fetch_all [2/6]: starting Companies House fetch company=%r", company_name)
            try:
                ch_profile = self.ch.fetch(company_name)
                if index:
                    ch_profile.index = index
                source_statuses["companies_house"] = "success"
            except Exception as e:
                source_statuses["companies_house"] = f"failed: {e}"
                LOGGER.warning(
                    "fetch_all [2/6]: Companies House failed company=%r: %s", company_name, e
                )
            LOGGER.info(
                "fetch_all [2/6]: Companies House done status=%s",
                source_statuses.get("companies_house"),
            )
        elif self.ch is None:
            LOGGER.info("fetch_all [2/6]: Companies House skipped (no API key configured)")
        else:
            LOGGER.info("fetch_all [2/6]: Companies House skipped (no company_name)")

        # PDF extraction — populate annual_report_text from the filing document URL.
        # Tried for whichever profile succeeded (EDGAR 10-K or CH annual accounts).
        # pdfplumber handles text-layer PDFs; Gemini falls back for scanned ones.
        #
        # Companies House documents require authentication via the Document API:
        #   GET {document_metadata_url}/content  →  302 → actual PDF
        # We download the PDF bytes via CompaniesHouseFetcher (which holds the auth)
        # and pass bytes directly to PDFExtractor.extract_from_bytes().
        # EDGAR documents are public URLs — PDFExtractor.extract(url) handles those.
        _active_profile = edgar_profile or ch_profile
        if _active_profile is not None and not _active_profile.annual_report_text:
            filing = _active_profile.latest_annual_filing
            doc_url = filing.document_url if filing else ""

            if not doc_url:
                LOGGER.info("fetch_all [3/6]: PDF extraction skipped (no filing URL)")
            elif (
                ch_profile is not None
                and _active_profile is ch_profile
                and self.ch is not None
                and "document-api.company-information.service.gov.uk" in doc_url
            ):
                # Companies House document — download with CH auth.
                # Large UK companies file in iXBRL (application/xhtml+xml) or PDF;
                # detect content-type and handle accordingly.
                _ticker_safe = ticker.upper().replace(".", "_")
                save_path = f"data/raw/ch_filing_{_ticker_safe}.bin"
                LOGGER.info("fetch_all [3/6]: starting CH document download url=%s", doc_url)
                try:
                    doc_bytes, content_type = self.ch.download_document(
                        doc_url, save_path=save_path
                    )
                    if doc_bytes:
                        if "pdf" in content_type:
                            doc_text = PDFExtractor().extract_from_bytes(
                                doc_bytes,
                                url=doc_url,
                                agent="data_gatherer",
                            )
                        else:
                            # iXBRL / HTML — strip tags to plain text
                            import re

                            try:
                                from bs4 import BeautifulSoup

                                soup = BeautifulSoup(doc_bytes, "html.parser")
                                for tag in soup(["script", "style", "meta", "link", "noscript"]):
                                    tag.decompose()
                                doc_text = soup.get_text(separator=" ", strip=True)
                            except Exception:
                                doc_text = re.sub(
                                    r"<[^>]+>", " ", doc_bytes.decode("utf-8", errors="replace")
                                )
                            doc_text = re.sub(r"\s+", " ", doc_text).strip()
                            LOGGER.info(
                                "fetch_all [3/6]: iXBRL/HTML stripped to %d chars (Content-Type=%s)",
                                len(doc_text),
                                content_type,
                            )
                        ch_profile.annual_report_text = doc_text or None
                        source_statuses["pdf_extraction"] = (
                            f"success: {len(doc_text):,} chars ({content_type})"
                            if doc_text
                            else "success: empty"
                        )
                    else:
                        source_statuses["pdf_extraction"] = "failed: no bytes returned"
                except Exception as e:
                    source_statuses["pdf_extraction"] = f"failed: {e}"
                    LOGGER.warning("fetch_all [3/6]: CH document extraction failed: %s", e)
                LOGGER.info(
                    "fetch_all [3/6]: CH document extraction done status=%s",
                    source_statuses.get("pdf_extraction"),
                )
            else:
                # Public URL (EDGAR) — PDFExtractor downloads it directly
                LOGGER.info("fetch_all [3/6]: starting PDF extraction url=%s", doc_url)
                try:
                    pdf_text = PDFExtractor().extract(url=doc_url, agent="data_gatherer")
                    _active_profile.annual_report_text = pdf_text or None
                    source_statuses["pdf_extraction"] = (
                        f"success: {len(pdf_text):,} chars" if pdf_text else "success: empty"
                    )
                except Exception as e:
                    source_statuses["pdf_extraction"] = f"failed: {e}"
                    LOGGER.warning("fetch_all [3/6]: PDF extraction failed: %s", e)
                LOGGER.info(
                    "fetch_all [3/6]: PDF extraction done status=%s",
                    source_statuses.get("pdf_extraction"),
                )
        else:
            LOGGER.info(
                "fetch_all [3/6]: PDF extraction skipped (text already present or no profile)"
            )

        # EPA / NRC regulatory fetchers — US companies only.
        # List is built here (not at module level) so @patch decorators work in tests.
        if edgar_profile is not None and edgar_profile.country == "US":
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
                    LOGGER.warning(
                        "fetch_all [4/6]: %s failed company=%r: %s", source.upper(), name, e
                    )
                LOGGER.info(
                    "fetch_all [4/6]: %s done status=%s",
                    source.upper(),
                    source_statuses.get(source),
                )
        else:
            LOGGER.info(
                "fetch_all [4/6]: EPA/NRC fetchers skipped (no EDGAR profile or non-US company country=%s)",
                edgar_profile.country if edgar_profile else "none",
            )

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
                    LOGGER.warning(
                        "fetch_all [5/6]: %s failed company=%r: %s", source.upper(), name, e
                    )
                LOGGER.info(
                    "fetch_all [5/6]: %s done status=%s",
                    source.upper(),
                    source_statuses.get(source),
                )
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
