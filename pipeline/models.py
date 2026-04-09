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
class MaterialFactor:
    factor_id: str  # e.g. "ghg_emissions"
    name: str  # e.g. "GHG Emissions"
    dimension: str  # SASB dimension: "Environment", "Social Capital", etc.
    financial_impacts: List[
        str
    ]  # subset of revenue_impact / cost_impact / asset_impact / liability_impact


@dataclass
class RelevanceFilterResult:
    ticker: Optional[str]
    sic_code: Optional[str]
    sasb_industry: Optional[str]  # None if SIC not mapped
    material_factors: List[MaterialFactor]
    errors: List[str]


@dataclass
class CommitmentCheck:
    commitment_text: str  # verbatim quote from report (≤200 chars)
    claimed_amount: Optional[float]  # monetary figure in source currency (None if not stated)
    currency: str  # ISO 4217 code or "UNKNOWN"
    horizon_year: Optional[int]  # target year stated in the commitment
    category: str  # "capex" | "opex" | "reduction_target" | "other"
    financials_label: Optional[str]  # raw_financials key used for comparison
    financials_value: Optional[float]  # actual financial figure used
    flag: str  # "consistent" | "plausible" | "gap" | "unverifiable"
    notes: str  # human-readable explanation of the verdict


@dataclass
class WordsMoneyResult:
    ticker: Optional[str]
    factor_id: str
    commitment_checks: List["CommitmentCheck"]
    score: float  # 0.0–1.0 mean of per-commitment flag scores
    errors: List[str]


@dataclass
class FactorScore:
    factor_id: str
    factor_name: str
    score: float  # weighted average across streams (0.0–1.0)
    flag: str  # "green" | "amber" | "red"
    stream_scores: Dict[str, float]  # {"disclosure": 0.85, "regulatory": 0.5, ...}
    evidence: List[str]  # human-readable notes per stream
    sources: List[str]  # URLs from CompanyProfile.source_urls
    narrative: str  # Claude-generated synthesis
    words_money_checks: Optional[List["CommitmentCheck"]] = None  # per-commitment detail


@dataclass
class CredibilityReport:
    ticker: Optional[str]
    company_name: str
    sasb_industry: Optional[str]
    factor_scores: List[FactorScore]
    overall_score: float  # mean of per-factor scores
    overall_flag: str  # "green" | "amber" | "red"
    errors: List[str]


@dataclass
class DataGathererResult:
    profile: Optional["CompanyProfile"]
    source_statuses: Dict[str, str] = field(default_factory=dict)
    # e.g. {"edgar": "success", "ghgrp": "failed: no facilities found"}
    regulatory_paths: Dict[str, str] = field(default_factory=dict)
    # e.g. {"ghgrp": "data/processed/ghgrp_AAPL.csv"} — read by credibility scorer


# ---------------------------------------------------------------------------
# DCF Mapper models
# ---------------------------------------------------------------------------


@dataclass
class DcfLineItem:
    """A single row label parsed from the analyst's Excel DCF model."""

    sheet_name: str
    row_index: int
    label: str  # raw text label from the spreadsheet cell


@dataclass
class EsgDcfMapping:
    """Maps one material ESG factor to the specific DCF line items it affects."""

    factor_id: str
    factor_name: str
    financial_impacts: List[str]  # SASB categories: revenue_impact / cost_impact / ...
    mapped_line_items: List[DcfLineItem]  # which rows in the DCF are affected
    scenario_low: Optional[float]  # low scenario financial impact (USD)
    scenario_mid: Optional[float]  # mid scenario financial impact (USD)
    scenario_high: Optional[float]  # high scenario financial impact (USD)
    scenario_currency: str  # "USD" | "GBP" | "EUR"
    scenario_source: str  # citation for the range
    credibility_flag: Optional[str] = None  # green / amber / red from CredibilityReport


@dataclass
class DcfMapperResult:
    """Output of the DCF mapper for one company."""

    ticker: Optional[str]
    excel_path: str
    sheet_names: List[str]  # sheets found in the workbook
    line_item_count: int  # total DCF line items parsed
    mappings: List[EsgDcfMapping]  # one entry per mapped factor
    unmapped_factors: List[str]  # factor names with no matching DCF line
    errors: List[str]
