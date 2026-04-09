import logging
import os
import re
from functools import lru_cache
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv

from pipeline.fetchers.base import BaseFetcher
from pipeline.models import CompanyProfile, FilingMetadata

load_dotenv()

LOGGER = logging.getLogger(__name__)


class EDGARFetcher(BaseFetcher):
    BASE_SUBMISSIONS = "https://data.sec.gov/submissions/"
    BASE_XBRL = "https://data.sec.gov/api/xbrl/companyfacts/"
    BASE_ARCHIVES = "https://www.sec.gov/Archives/edgar/data/"

    def __init__(self, user_email: str = None):
        email = user_email or os.getenv("SEC_EMAIL") or os.getenv("CONTACT_EMAIL")
        if not email:
            raise ValueError(
                "Provide user_email or set SEC_EMAIL env var (required by SEC fair-use policy)"
            )
        self.headers = {"User-Agent": email}

    @lru_cache(maxsize=1)
    def _ticker_map(self) -> Dict:
        url = "https://www.sec.gov/files/company_tickers.json"
        r = requests.get(url, headers=self.headers, timeout=10)
        r.raise_for_status()
        return r.json()

    def get_cik(self, ticker: str) -> Optional[str]:
        ticker = ticker.upper()
        for entry in self._ticker_map().values():
            if entry["ticker"] == ticker:
                return str(entry["cik_str"]).zfill(10)
        return None

    def get_submissions(self, cik: str) -> Dict:
        url = f"{self.BASE_SUBMISSIONS}CIK{cik}.json"
        r = requests.get(url, headers=self.headers, timeout=10)
        r.raise_for_status()
        return r.json()

    def get_latest_10k(self, submissions: Dict) -> Optional[Dict]:
        """Returns the latest 10-K filing, or 20-F as a fallback for foreign private issuers."""
        data = submissions["filings"]["recent"]
        period_list = data.get("periodOfReport", [])

        # First pass: look for 10-K
        for i, form in enumerate(data["form"]):
            if form == "10-K":
                LOGGER.info("EDGAR: found 10-K filing dated %s", data["filingDate"][i])
                return {
                    "accession": data["accessionNumber"][i],
                    "filing_date": data["filingDate"][i],
                    "period_of_report": period_list[i] if i < len(period_list) else None,
                    "primary_doc": data["primaryDocument"][i],
                    "form_type": "10-K",
                }

        # Fallback: look for 20-F (foreign private issuers, e.g. BP)
        for i, form in enumerate(data["form"]):
            if form == "20-F":
                LOGGER.info("EDGAR: no 10-K found — falling back to 20-F dated %s", data["filingDate"][i])
                return {
                    "accession": data["accessionNumber"][i],
                    "filing_date": data["filingDate"][i],
                    "period_of_report": period_list[i] if i < len(period_list) else None,
                    "primary_doc": data["primaryDocument"][i],
                    "form_type": "20-F",
                }

        return None

    def download_10k_document(self, cik: str, filing: Dict) -> Optional[str]:
        try:
            accession = filing["accession"].replace("-", "")
            url = f"{self.BASE_ARCHIVES}{cik}/{accession}/{filing['primary_doc']}"
            r = requests.get(url, headers=self.headers, timeout=20)
            r.raise_for_status()
            return r.text
        except Exception:
            return None

    def strip_html_to_text(self, html: str) -> str:
        """Strips HTML tags and normalises whitespace to produce plain text."""
        text = re.sub(r"<[^>]+>", " ", html)
        return re.sub(r"\s+", " ", text).strip()

    def get_xbrl(self, cik: str) -> Dict:
        url = f"{self.BASE_XBRL}CIK{cik}.json"
        r = requests.get(url, headers=self.headers, timeout=10)
        r.raise_for_status()
        return r.json()

    def extract_financials(self, facts: Dict) -> Dict:
        try:
            us_gaap = facts["facts"]["us-gaap"]

            def latest(tag):
                units = us_gaap.get(tag, {}).get("units", {})
                for unit_vals in units.values():
                    # prefer 10-K annual values over quarterly
                    annual = [v for v in unit_vals if v.get("form") == "10-K"]
                    candidates = annual if annual else unit_vals
                    if candidates:
                        return sorted(candidates, key=lambda x: x.get("end", ""), reverse=True)[0][
                            "val"
                        ]
                return None

            return {
                "revenue": latest("Revenues")
                or latest("RevenueFromContractWithCustomerExcludingAssessedTax"),
                "operating_income": latest("OperatingIncomeLoss"),
                "total_assets": latest("Assets"),
                # capex — payments to acquire property, plant and equipment
                "capex": latest("PaymentsToAcquirePropertyPlantAndEquipment"),
                # total operating expenses for opex-type ESG commitment checks
                "total_opex": latest("OperatingExpenses") or latest("CostsAndExpenses"),
            }
        except Exception:
            return {}

    def fetch(self, ticker: str) -> CompanyProfile:
        LOGGER.info("EDGAR: starting fetch for ticker=%s", ticker)
        errors: List[str] = []
        source_urls: List[str] = []

        cik = self.get_cik(ticker)
        if not cik:
            raise ValueError(f"CIK not found for {ticker}")
        LOGGER.info("EDGAR: resolved ticker=%s to CIK=%s", ticker, cik)

        submissions = self.get_submissions(cik)
        company_name = submissions.get("name", ticker)
        sic_code = submissions.get("sic")
        sic_description = submissions.get("sicDescription")

        filing = self.get_latest_10k(submissions)
        latest_annual_filing = None
        annual_report_text = ""

        if filing:
            form_type = filing.get("form_type", "10-K")
            accession = filing["accession"].replace("-", "")
            report_url = f"{self.BASE_ARCHIVES}{cik}/{accession}/{filing['primary_doc']}"
            source_urls.append(report_url)
            latest_annual_filing = FilingMetadata(
                filing_type=form_type,
                filed_date=filing["filing_date"],
                period_of_report=filing.get("period_of_report"),
                document_url=report_url,
            )
            LOGGER.info("EDGAR: downloading %s document for CIK=%s", form_type, cik)
            document = self.download_10k_document(cik, filing)
            if document:
                annual_report_text = self.strip_html_to_text(document)
                LOGGER.info("EDGAR: %s downloaded and stripped — %d chars", form_type, len(annual_report_text))
            else:
                errors.append(f"Failed to download {form_type} document")
                LOGGER.warning("EDGAR: %s document download returned nothing for CIK=%s", form_type, cik)
        else:
            errors.append("No 10-K or 20-F filing found in recent submissions")
            LOGGER.info("EDGAR: no 10-K or 20-F found for ticker=%s", ticker)

        raw_financials: Dict = {}
        try:
            LOGGER.info("EDGAR: fetching XBRL financials for CIK=%s", cik)
            facts = self.get_xbrl(cik)
            raw_financials = self.extract_financials(facts)
            LOGGER.info("EDGAR: XBRL done — keys=%s", list(raw_financials.keys()))
        except Exception as e:
            errors.append(f"XBRL fetch failed: {e}")
            LOGGER.warning("EDGAR: XBRL fetch failed for CIK=%s: %s", cik, e)

        LOGGER.info(
            "EDGAR: fetch complete ticker=%s name=%r sic=%s report_chars=%d errors=%d",
            ticker, company_name, sic_code, len(annual_report_text), len(errors),
        )
        return CompanyProfile(
            ticker=ticker.upper(),
            name=company_name,
            index=None,  # set by caller (DataGatherer)
            sic_code=str(sic_code) if sic_code else None,
            sic_description=sic_description,
            country="US",
            latest_annual_filing=latest_annual_filing,
            annual_report_text=annual_report_text,
            raw_financials=raw_financials,
            source_urls=source_urls,
            errors=errors,
            identifier=cik,
        )
