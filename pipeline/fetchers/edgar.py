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

    def search_cik_by_name(self, company_name: str) -> Optional[str]:
        """
        Search EDGAR full-text search (EFTS) by company name for 20-F filers.
        Used as a fallback for FTSE100 companies whose LSE ticker differs from
        their SEC filing ticker (e.g. ULVR on LSE → UL on SEC as Unilever PLC).

        Returns zero-padded 10-digit CIK string, or None if not found.
        """
        # EFTS search — search by entity name, filter to 20-F (foreign private issuers)
        url = "https://efts.sec.gov/LATEST/search-index"
        params = {
            "q": f'"{company_name}"',
            "forms": "20-F",
            "dateRange": "custom",
            "startdt": "2023-01-01",
            "enddt": "2026-12-31",
        }
        try:
            r = requests.get(url, params=params, headers=self.headers, timeout=10)
            r.raise_for_status()
            data = r.json()
            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                LOGGER.info("EDGAR: EFTS name search for %r returned no hits", company_name)
                return None
            # entity_id in _source is the CIK (without leading zeros)
            source = hits[0].get("_source", {})
            # EFTS response shape: _source.ciks is a list of CIK strings (without leading zeros)
            # Older/alternate shape uses _source.entity_id — check both.
            ciks = source.get("ciks") or []
            if isinstance(ciks, list) and ciks:
                cik = str(ciks[0]).zfill(10)
            elif source.get("entity_id"):
                cik = str(source["entity_id"]).zfill(10)
            else:
                LOGGER.info(
                    "EDGAR: EFTS hit for %r has no ciks/entity_id — source keys: %s",
                    company_name,
                    list(source.keys()),
                )
                return None
            LOGGER.info("EDGAR: EFTS name search resolved %r to CIK=%s", company_name, cik)
            return cik
        except Exception as e:
            LOGGER.warning("EDGAR: EFTS name search failed for %r: %s", company_name, e)
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
                LOGGER.info(
                    "EDGAR: no 10-K found — falling back to 20-F dated %s", data["filingDate"][i]
                )
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
        """
        Strips HTML/iXBRL tags and normalises whitespace to produce plain text.

        Uses BeautifulSoup when available (better extraction from iXBRL 10-K/20-F
        filings that embed XBRL tags inside the narrative HTML).  Falls back to
        a regex strip if BeautifulSoup is unavailable.
        """
        try:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html, "html.parser")
            # Remove non-content tags (scripts, styles, XBRL metadata blocks)
            for tag in soup(
                [
                    "script",
                    "style",
                    "meta",
                    "link",
                    "noscript",
                    "ix:header",
                    "ix:hidden",
                    "xbrli:xbrl",
                ]
            ):
                tag.decompose()
            text = soup.get_text(separator=" ", strip=True)
        except Exception:
            text = re.sub(r"<[^>]+>", " ", html)

        return re.sub(r"\s+", " ", text).strip()

    def get_xbrl(self, cik: str) -> Dict:
        url = f"{self.BASE_XBRL}CIK{cik}.json"
        r = requests.get(url, headers=self.headers, timeout=10)
        r.raise_for_status()
        return r.json()

    def extract_financials(self, facts: Dict) -> Dict:
        """
        Extract key financial line items from EDGAR XBRL company facts.

        Supports both US-GAAP (10-K filers) and IFRS-Full (20-F filers such as
        BP, Shell, Unilever). The two taxonomies use different tag names for the
        same concepts; we try US-GAAP first and fall back to IFRS equivalents.
        """
        all_facts = facts.get("facts", {})
        us_gaap = all_facts.get("us-gaap", {})
        ifrs = all_facts.get("ifrs-full", {})

        # annual form types for each taxonomy
        _GAAP_ANNUAL = "10-K"
        _IFRS_ANNUAL = "20-F"

        def latest_from(namespace: Dict, tag: str, annual_form: str) -> Optional[float]:
            units = namespace.get(tag, {}).get("units", {})
            for unit_vals in units.values():
                annual = [v for v in unit_vals if v.get("form") == annual_form]
                candidates = annual if annual else unit_vals
                if candidates:
                    return sorted(candidates, key=lambda x: x.get("end", ""), reverse=True)[0][
                        "val"
                    ]
            return None

        def latest(gaap_tag: str, ifrs_tag: str = None) -> Optional[float]:
            val = latest_from(us_gaap, gaap_tag, _GAAP_ANNUAL)
            if val is not None:
                return val
            if ifrs_tag and ifrs:
                return latest_from(ifrs, ifrs_tag, _IFRS_ANNUAL)
            return None

        try:
            return {
                "revenue": (
                    latest("Revenues", "Revenue")
                    or latest(
                        "RevenueFromContractWithCustomerExcludingAssessedTax",
                        "RevenueFromContractsWithCustomers",
                    )
                ),
                "operating_income": latest(
                    "OperatingIncomeLoss", "ProfitLossFromOperatingActivities"
                ),
                "total_assets": latest("Assets", "Assets"),
                # capex — payments to acquire PP&E
                "capex": latest(
                    "PaymentsToAcquirePropertyPlantAndEquipment",
                    "PurchaseOfPropertyPlantAndEquipment",
                ),
                # total opex
                "total_opex": (
                    latest("OperatingExpenses", "OperatingExpense")
                    or latest("CostsAndExpenses", "CostOfSales")
                ),
            }
        except Exception:
            return {}

    def fetch(self, ticker: str, company_name: str = None) -> CompanyProfile:
        LOGGER.info("EDGAR: starting fetch for ticker=%s", ticker)
        errors: List[str] = []
        source_urls: List[str] = []

        cik = self.get_cik(ticker)
        if not cik and company_name:
            LOGGER.info(
                "EDGAR: direct ticker lookup failed for %s — trying name search for %r",
                ticker,
                company_name,
            )
            cik = self.search_cik_by_name(company_name)
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
                LOGGER.info(
                    "EDGAR: %s downloaded and stripped — %d chars",
                    form_type,
                    len(annual_report_text),
                )
            else:
                errors.append(f"Failed to download {form_type} document")
                LOGGER.warning(
                    "EDGAR: %s document download returned nothing for CIK=%s", form_type, cik
                )
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
            ticker,
            company_name,
            sic_code,
            len(annual_report_text),
            len(errors),
        )
        # 10-K filers are US domestic companies; 20-F filers are foreign private issuers
        country = "US" if (not filing or filing.get("form_type") == "10-K") else "non-US"

        return CompanyProfile(
            ticker=ticker.upper(),
            name=company_name,
            index=None,  # set by caller (DataGatherer)
            sic_code=str(sic_code) if sic_code else None,
            sic_description=sic_description,
            country=country,
            latest_annual_filing=latest_annual_filing,
            annual_report_text=annual_report_text,
            raw_financials=raw_financials,
            source_urls=source_urls,
            errors=errors,
            identifier=cik,
        )
