import re
from functools import lru_cache
from typing import Dict, Optional

import requests

from pipeline.fetchers.base import BaseFetcher
from pipeline.models import CompanyProfile


class EDGARFetcher(BaseFetcher):
    BASE_SUBMISSIONS = "https://data.sec.gov/submissions/"
    BASE_XBRL = "https://data.sec.gov/api/xbrl/companyfacts/"
    BASE_ARCHIVES = "https://www.sec.gov/Archives/edgar/data/"

    def __init__(self, user_email: str):
        self.headers = {"User-Agent": user_email}

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

        for i, form in enumerate(data["form"]):
            if form == "10-K":
                return {
                    "accession": data["accessionNumber"][i],
                    "filing_date": data["filingDate"][i],
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

    def extract_risk_factors(self, text: str) -> str:
        if not text:
            return ""

        patterns = [
            r"(?i)item\s*1a\.?\s*risk\s*factors(.*?)(?=item\s*1b|item\s*2)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.DOTALL)
            if match:
                cleaned = re.sub(r"<[^>]+>", " ", match.group(1))
                return re.sub(r"\s+", " ", cleaned)[:10000]

        return ""

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
                for unit in units.values():
                    return sorted(unit, key=lambda x: x.get("end", ""), reverse=True)[0]["val"]
                return None

            return {
                "revenue": latest("Revenues"),
                "operating_income": latest("OperatingIncomeLoss"),
                "total_assets": latest("Assets"),
            }
        except Exception:
            return {}

    def extract_incorporation_state(self, submissions: Dict) -> Optional[str]:
        return submissions.get("addresses", {}).get("business", {}).get("stateOrCountry")

    def fetch(self, ticker: str) -> CompanyProfile:
        cik = self.get_cik(ticker)
        if not cik:
            raise ValueError(f"CIK not found for {ticker}")

        submissions = self.get_submissions(cik)
        company_name = submissions.get("name", ticker)

        filing = self.get_latest_10k(submissions)

        document = None
        risk_factors = ""
        report_url = None

        if filing:
            document = self.download_10k_document(cik, filing)
            risk_factors = self.extract_risk_factors(document)
            accession = filing["accession"].replace("-", "")
            report_url = f"{self.BASE_ARCHIVES}{cik}/{accession}/{filing['primary_doc']}"

        facts = self.get_xbrl(cik)
        financials = self.extract_financials(facts)

        return CompanyProfile(
            name=company_name,
            ticker=ticker,
            country="US",
            listing_country="US",
            incorporation_state=self.extract_incorporation_state(submissions),
            identifier=cik,
            filing_date=filing["filing_date"] if filing else None,
            report_url=report_url,
            revenue=financials.get("revenue"),
            operating_income=financials.get("operating_income"),
            total_assets=financials.get("total_assets"),
            extra={
                "xbrl": facts,
                "submissions": submissions,
                "risk_factors": risk_factors,
                "document": document[:20000] if document else None,
            },
        )
