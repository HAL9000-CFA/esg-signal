import os
import re
from functools import lru_cache
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv

from pipeline.fetchers.base import BaseFetcher
from pipeline.models import CompanyProfile, FilingMetadata

load_dotenv()


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
        data = submissions["filings"]["recent"]
        period_list = data.get("periodOfReport", [])
        for i, form in enumerate(data["form"]):
            if form == "10-K":
                return {
                    "accession": data["accessionNumber"][i],
                    "filing_date": data["filingDate"][i],
                    "period_of_report": period_list[i] if i < len(period_list) else None,
                    "primary_doc": data["primaryDocument"][i],
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
        errors: List[str] = []
        source_urls: List[str] = []

        cik = self.get_cik(ticker)
        if not cik:
            raise ValueError(f"CIK not found for {ticker}")

        submissions = self.get_submissions(cik)
        company_name = submissions.get("name", ticker)
        sic_code = submissions.get("sic")
        sic_description = submissions.get("sicDescription")

        filing = self.get_latest_10k(submissions)
        latest_annual_filing = None
        annual_report_text = ""

        if filing:
            accession = filing["accession"].replace("-", "")
            report_url = f"{self.BASE_ARCHIVES}{cik}/{accession}/{filing['primary_doc']}"
            source_urls.append(report_url)
            latest_annual_filing = FilingMetadata(
                filing_type="10-K",
                filed_date=filing["filing_date"],
                period_of_report=filing.get("period_of_report"),
                document_url=report_url,
            )
            document = self.download_10k_document(cik, filing)
            if document:
                annual_report_text = self.strip_html_to_text(document)
            else:
                errors.append("Failed to download 10-K document")
        else:
            errors.append("No 10-K filing found in recent submissions")

        raw_financials: Dict = {}
        try:
            facts = self.get_xbrl(cik)
            raw_financials = self.extract_financials(facts)
        except Exception as e:
            errors.append(f"XBRL fetch failed: {e}")

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
