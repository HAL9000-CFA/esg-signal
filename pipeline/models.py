from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class CompanyProfile:
    name: str
    ticker: Optional[str]
    country: str
    identifier: str  # CIK or company_number
    incorporation_state: Optional[str] = None
    listing_country: Optional[str] = None

    filing_date: Optional[str] = None
    report_url: Optional[str] = None

    risk_factors: Optional[str] = None

    revenue: Optional[float] = None
    operating_income: Optional[float] = None
    total_assets: Optional[float] = None


@dataclass
class DataGathererResult:
    profile: CompanyProfile
    sources: Dict = field(default_factory=dict)
    extra: Dict = field(default_factory=dict)
