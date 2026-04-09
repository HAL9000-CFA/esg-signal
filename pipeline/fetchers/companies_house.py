import logging
import os
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv

from pipeline.fetchers.base import BaseFetcher
from pipeline.models import CompanyProfile, FilingMetadata

load_dotenv()

LOGGER = logging.getLogger(__name__)

CH_FILING_BASE = "https://find-and-update.company-information.service.gov.uk"
CH_DOCUMENT_API = "https://document-api.company-information.service.gov.uk"


class CompaniesHouseFetcher(BaseFetcher):
    BASE_URL = "https://api.company-information.service.gov.uk"

    def __init__(self, api_key: str = None):
        key = api_key or os.getenv("COMPANIES_HOUSE_API_KEY")
        if not key:
            raise ValueError("Provide api_key or set COMPANIES_HOUSE_API_KEY env var")
        self.auth = (key.strip(), "")

    def search(self, name: str) -> Optional[str]:
        r = requests.get(
            f"{self.BASE_URL}/search/companies",
            params={"q": name},
            auth=self.auth,
            timeout=10,
        )
        r.raise_for_status()
        items = r.json().get("items", [])
        return items[0]["company_number"] if items else None

    def get_profile(self, number: str) -> Dict:
        r = requests.get(
            f"{self.BASE_URL}/company/{number}",
            auth=self.auth,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def get_filing_history(self, number: str, category: str = "accounts") -> Dict:
        r = requests.get(
            f"{self.BASE_URL}/company/{number}/filing-history",
            params={"category": category, "items_per_page": 10},
            auth=self.auth,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def get_latest_annual_accounts(self, number: str) -> Optional[Dict]:
        """Returns metadata for the most recent full annual accounts filing (type AA)."""
        history = self.get_filing_history(number, category="accounts")
        for item in history.get("items", []):
            if item.get("type") == "AA":
                links = item.get("links", {})
                # document_metadata points to the Document API endpoint (requires auth).
                # Prefer this over links.self which is a viewer page that returns 404 for
                # direct PDF download.
                doc_meta = links.get("document_metadata", "")
                self_path = links.get("self", "")
                document_url = (
                    doc_meta if doc_meta else (f"{CH_FILING_BASE}{self_path}" if self_path else "")
                )
                return {
                    "filing_type": "AA",
                    "filed_date": item.get("date", ""),
                    "period_of_report": item.get("description_values", {}).get("made_up_date"),
                    "document_url": document_url,
                    "document_metadata_url": doc_meta,
                }
        return None

    def download_document(
        self, document_metadata_url: str, save_path: str = None
    ) -> tuple[Optional[bytes], str]:
        """
        Download a Companies House filing document via the Document API.

        Returns (bytes, content_type). content_type is used by the caller to decide
        how to extract text:
          - "application/pdf"       → pass to PDFExtractor.extract_from_bytes()
          - "application/xhtml+xml" → iXBRL; strip tags to plain text
          - "text/html"             → HTML; strip tags to plain text

        The document_metadata_url (from filing-history links.document_metadata) points to
        https://document-api.company-information.service.gov.uk/document/{id}.
        Appending /content and following the redirect yields the actual document.

        If save_path is provided the raw bytes are written there so they can be
        inspected outside the pipeline (useful for debugging).
        """
        content_url = document_metadata_url.rstrip("/") + "/content"
        LOGGER.info("CompaniesHouse: downloading document from %s", content_url)
        try:
            r = requests.get(
                content_url,
                auth=self.auth,
                headers={"Accept": "application/pdf,application/xhtml+xml,text/html,*/*"},
                timeout=60,
                allow_redirects=True,
            )
            r.raise_for_status()
            content_type = (
                r.headers.get("Content-Type", "application/octet-stream").split(";")[0].strip()
            )
            LOGGER.info(
                "CompaniesHouse: downloaded %d bytes Content-Type=%s",
                len(r.content),
                content_type,
            )
            if save_path:
                import os

                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                with open(save_path, "wb") as fh:
                    fh.write(r.content)
                LOGGER.info("CompaniesHouse: saved raw document to %s", save_path)
            return r.content, content_type
        except Exception as e:
            LOGGER.warning("CompaniesHouse: document download failed %s: %s", content_url, e)
            return None, ""

    def fetch(self, company_name: str) -> CompanyProfile:
        LOGGER.info("Companies House: starting fetch for company=%r", company_name)
        errors: List[str] = []
        source_urls: List[str] = []

        number = self.search(company_name)
        if not number:
            raise ValueError(f"Company not found: {company_name}")
        LOGGER.info("Companies House: resolved %r to company_number=%s", company_name, number)

        profile = self.get_profile(number)
        name = profile.get("company_name", company_name)

        sic_codes = profile.get("sic_codes", [])
        sic_code = sic_codes[0] if sic_codes else None

        latest_annual_filing = None
        try:
            LOGGER.info("Companies House: fetching filing history for number=%s", number)
            filing = self.get_latest_annual_accounts(number)
            if filing:
                latest_annual_filing = FilingMetadata(
                    filing_type=filing["filing_type"],
                    filed_date=filing["filed_date"],
                    period_of_report=filing.get("period_of_report"),
                    document_url=filing["document_url"],
                )
                if filing["document_url"]:
                    source_urls.append(filing["document_url"])
                LOGGER.info("Companies House: latest AA filing dated %s", filing["filed_date"])
            else:
                errors.append("No annual accounts (AA) filing found")
                LOGGER.info("Companies House: no AA filing found for number=%s", number)
        except Exception as e:
            errors.append(f"Filing history fetch failed: {e}")
            LOGGER.warning("Companies House: filing history failed for number=%s: %s", number, e)

        LOGGER.info(
            "Companies House: fetch complete company=%r number=%s sic=%s errors=%d",
            name,
            number,
            sic_code,
            len(errors),
        )
        return CompanyProfile(
            ticker=None,
            name=name,
            index=None,  # set by caller (DataGatherer)
            sic_code=sic_code,
            sic_description=None,  # CH API returns code only, not description
            country="GB",
            latest_annual_filing=latest_annual_filing,
            annual_report_text=None,  # full document text requires Document API (issue #8)
            raw_financials={},
            source_urls=source_urls,
            errors=errors,
            identifier=number,
        )
