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
class JobPosting:
    job_id: str
    title: str
    date_posted: str  # raw string from source (format varies)
    url: str
    source: str  # "indeed" or "serp"
    keywords_matched: List[str]
    seniority: str  # "senior", "mid", "junior"


@dataclass
class TalentSignalResult:
    company_name: str
    total_postings: int
    senior_ratio: float  # proportion of senior-title postings (0.0–1.0)
    ghost_count: int  # postings that disappeared without a hire
    factor_scores: Dict[str, float]  # {"environment": 0.65, "social": 0.3, ...} (0.0–1.0)
    errors: List[str]


@dataclass
class DataGathererResult:
    profile: Optional["CompanyProfile"]
    source_statuses: Dict[str, str] = field(default_factory=dict)
    # e.g. {"edgar": "success", "ghgrp": "failed: no facilities found"}
    regulatory_paths: Dict[str, str] = field(default_factory=dict)
    # e.g. {"ghgrp": "data/processed/ghgrp_AAPL.csv"} — read by credibility scorer
