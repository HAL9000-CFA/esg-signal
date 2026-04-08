from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class FilingMetadata:
    filing_type: str  # "10-K" or "AA"
    filed_date: str
    period_of_report: Optional[str]
    document_url: str


@dataclass
class CompanyProfile:
    ticker: Optional[str]
    name: str
    index: Optional[str]  # "SP500", "FTSE100", or None
    sic_code: Optional[str]
    sic_description: Optional[str]
    country: str
    latest_annual_filing: Optional[FilingMetadata]
    annual_report_text: Optional[str]
    raw_financials: Dict
    source_urls: List[str]
    errors: List[str]
    # internal identifier — CIK (EDGAR) or company_number (CH)
    identifier: Optional[str] = None


@dataclass
class DataGathererResult:
    profile: Optional["CompanyProfile"]
    source_statuses: Dict[str, str] = field(default_factory=dict)
    # e.g. {"edgar": "success", "ghgrp": "failed: no facilities found"}
    regulatory_paths: Dict[str, str] = field(default_factory=dict)
    # e.g. {"ghgrp": "data/processed/ghgrp_AAPL.csv"} — read by credibility scorer
