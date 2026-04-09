"""
SASB Materiality Relevance Filter — issue #10.

Takes a CompanyProfile and returns the subset of ESG factors that are
financially material for that company's industry, drawn from the SASB
Materiality Map stored locally in data/sasb_map.json.

This list drives everything downstream: the credibility scorer (issue #11)
and DCF mapper (issue #13) only analyse factors that matter for the specific
business rather than applying generic ESG metrics to every company.

SIC codes are mapped to SASB industries at init time. Where no match is found,
a generic set of universally applicable fallback factors is returned with a
warning in errors — the pipeline continues rather than halting.

SASB standards: https://www.sasb.org/standards/
"""

import json
import logging
from pathlib import Path
from typing import Dict, List

from pipeline.models import CompanyProfile, MaterialFactor, RelevanceFilterResult

LOGGER = logging.getLogger(__name__)

_VALID_FINANCIAL_IMPACTS = frozenset(
    ["revenue_impact", "cost_impact", "asset_impact", "liability_impact"]
)


class RelevanceFilter:
    """
    Maps a CompanyProfile to its SASB-material ESG factors via SIC code lookup.

    Args:
        sasb_map_path: Path to the SASB map JSON file.
                       Defaults to data/sasb_map.json relative to the repo root.
    """

    def __init__(self, sasb_map_path: str = "data/sasb_map.json"):
        raw = json.loads(Path(sasb_map_path).read_text())
        self._industries: Dict = raw["industries"]
        self._fallback_factors: Dict = raw["fallback_factors"]
        # Build reverse index: normalised SIC code string -> industry_id
        self._sic_index: Dict[str, str] = {}
        for industry_id, industry_data in self._industries.items():
            for sic in industry_data.get("sic_codes", []):
                self._sic_index[str(sic).zfill(4)] = industry_id

    def filter(self, profile: CompanyProfile) -> RelevanceFilterResult:
        """
        Return the material ESG factors for the company's SASB industry.

        Falls back to a generic factor set when:
          - profile.sic_code is None (no SIC data from fetcher)
          - SIC code has no matching SASB industry in the map

        Args:
            profile: CompanyProfile from DataGatherer.

        Returns:
            RelevanceFilterResult with material_factors and any errors.
        """
        errors: List[str] = []

        if not profile.sic_code:
            errors.append(
                "SIC code not available — falling back to generic ESG factors. "
                "Check that the EDGAR or Companies House fetcher extracted sic_code correctly."
            )
            return RelevanceFilterResult(
                ticker=profile.ticker,
                sic_code=None,
                sasb_industry=None,
                material_factors=self._parse_factors(self._fallback_factors),
                errors=errors,
            )

        sic = str(profile.sic_code).zfill(4)[:4]
        industry_id = self._sic_index.get(sic)

        if not industry_id:
            errors.append(
                f"SIC {sic!r} not mapped to a SASB industry in sasb_map.json — "
                "falling back to generic ESG factors. "
                "Add this SIC to data/sasb_map.json to improve accuracy."
            )
            return RelevanceFilterResult(
                ticker=profile.ticker,
                sic_code=sic,
                sasb_industry=None,
                material_factors=self._parse_factors(self._fallback_factors),
                errors=errors,
            )

        industry = self._industries[industry_id]
        LOGGER.info(
            f"Mapped SIC {sic} ({profile.ticker}) to SASB industry "
            f"'{industry['name']}' ({len(industry['material_factors'])} material factors)"
        )

        return RelevanceFilterResult(
            ticker=profile.ticker,
            sic_code=sic,
            sasb_industry=industry["name"],
            material_factors=self._parse_factors(industry["material_factors"]),
            errors=errors,
        )

    @staticmethod
    def _parse_factors(factors_dict: Dict) -> List[MaterialFactor]:
        factors = []
        for factor_id, data in factors_dict.items():
            impacts = [i for i in data["financial_impacts"] if i in _VALID_FINANCIAL_IMPACTS]
            factors.append(
                MaterialFactor(
                    factor_id=factor_id,
                    name=data["name"],
                    dimension=data["dimension"],
                    financial_impacts=impacts,
                )
            )
        return factors
