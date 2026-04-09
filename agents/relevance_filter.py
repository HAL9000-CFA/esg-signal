"""
SASB Materiality Relevance Filter — issue #10.

Takes a CompanyProfile and returns the subset of ESG factors that are
financially material for that company's industry.

Factor source priority (per industry):
  1. SASB Navigator API  — live GIC list with descriptions; full factor set
  2. Navigator cache     — local data/sasb/industries/<code>.json
  3. data/sasb_map.json  — static fallback when Navigator is unreachable

When the Navigator is used, its GIC list is the authoritative factor set.
The static map is merged in to supply precise financial_impacts for factors
it already knows about; new Navigator factors get dimension-inferred values.

SIC codes are mapped to SASB industries at init time. Where no match is found,
a generic set of universally applicable fallback factors is returned with a
warning in errors — the pipeline continues rather than halting.

SASB standards: https://www.sasb.org/standards/
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

from pipeline.fetchers.sasb_topics import (
    flatten_topics,
    get_industry_topics,
    search_company_results,
)
from pipeline.models import CompanyProfile, MaterialFactor, RelevanceFilterResult

LOGGER = logging.getLogger(__name__)

# Navigator GIC factor_id → static map factor_id (where slugs differ)
_NAVIGATOR_ID_ALIASES: Dict[str, str] = {
    "water_wastewater_management": "water_management",
    "critical_incident_risk_management": "critical_incident_risk",
}

# financial_impacts inferred from dimension for Navigator factors not in static map
_DIMENSION_FINANCIAL_IMPACTS: Dict[str, List[str]] = {
    "Environment": ["cost_impact", "asset_impact"],
    "Social Capital": ["revenue_impact", "liability_impact"],
    "Human Capital": ["cost_impact"],
    "Leadership & Governance": ["liability_impact"],
    "Business Model & Innovation": ["revenue_impact", "asset_impact"],
}

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

        Resolution order:
          1. Navigator companySearch(ticker) — validated against profile.name
          2. SIC code → sasb_map.json → navigator_code → Navigator industryTopics
          3. SIC code → sasb_map.json static factors only
          4. Generic fallback factors

        The Navigator is always the authoritative source for factor lists and
        descriptions. sasb_map.json supplies financial_impacts and is the last
        resort when Navigator and its cache are both unreachable.
        """
        errors: List[str] = []

        # --- Step 1: Navigator ticker lookup (primary) ---
        if profile.ticker:
            navigator_code = self._navigator_code_from_ticker(profile.ticker, profile.name)
            if navigator_code:
                try:
                    industry_data = get_industry_topics(navigator_code)
                    sic = str(profile.sic_code).zfill(4)[:4] if profile.sic_code else None
                    industry_id = self._sic_index.get(sic) if sic else None
                    static_factors = (
                        self._industries[industry_id]["material_factors"] if industry_id else {}
                    )
                    factors = self._merge(
                        flatten_topics(industry_data), static_factors, navigator_code
                    )
                    industry_name = industry_data.get("industry_name", navigator_code)
                    LOGGER.info(
                        "Navigator ticker lookup: %s → %s ('%s'), %d factors",
                        profile.ticker,
                        navigator_code,
                        industry_name,
                        len(factors),
                    )
                    LOGGER.info("Factors: %s", [f.factor_id for f in factors])
                    return RelevanceFilterResult(
                        ticker=profile.ticker,
                        sic_code=str(profile.sic_code).zfill(4)[:4] if profile.sic_code else None,
                        sasb_industry=industry_name,
                        material_factors=factors,
                        errors=errors,
                    )
                except Exception as exc:
                    LOGGER.warning(
                        "relevance_filter: Navigator industry fetch failed for %s/%s (%s) — falling back to SIC",
                        profile.ticker,
                        navigator_code,
                        exc,
                    )

        # --- Step 2: SIC → sasb_map.json fallback ---
        if not profile.sic_code:
            errors.append(
                "SIC code not available and Navigator ticker lookup failed — "
                "falling back to generic ESG factors."
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
        navigator_code = industry.get("navigator_code")
        static_factors = industry["material_factors"]

        factors = self._build_factors(static_factors, navigator_code)

        LOGGER.info(
            "SIC fallback: %s → SIC %s → SASB '%s': %d factors (navigator=%s)",
            profile.ticker,
            sic,
            industry["name"],
            len(factors),
            navigator_code or "none",
        )
        LOGGER.info("Factors: %s", [f.factor_id for f in factors])

        return RelevanceFilterResult(
            ticker=profile.ticker,
            sic_code=sic,
            sasb_industry=industry["name"],
            material_factors=factors,
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Factor construction
    # ------------------------------------------------------------------

    def _navigator_code_from_ticker(self, ticker: str, company_name: str) -> Optional[str]:
        """
        Search Navigator for the ticker and return the industry_code of the result
        whose company_name best matches profile.name.

        The API returns a list — [0] is not always correct (e.g. "BP" → Backstageplay
        Inc at [0], BP PLC at [1]). We iterate all results and pick the first whose
        name shares a meaningful word (>3 chars) with profile.name.
        Returns None if no result matches or on any error.
        """
        try:
            results = search_company_results(ticker)
            profile_name_lower = (company_name or "").lower()
            ticker_lower = ticker.lower()
            _SUFFIXES = {"plc", "inc", "ltd", "llc", "corp", "co", "group", "sa", "ag", "nv"}
            for result in results:
                nav_name = (result.get("company_name") or "").lower()
                nav_tokens = set(nav_name.split())
                # Match if: ticker appears as a token in the Navigator name, OR
                # any substantive word (>3 chars, not a legal suffix) from the
                # Navigator name appears in the profile name
                substantive = [w for w in nav_tokens if len(w) > 3 and w not in _SUFFIXES]
                ticker_in_nav = ticker_lower in nav_tokens
                name_overlap = substantive and any(w in profile_name_lower for w in substantive)
                if ticker_in_nav or name_overlap:
                    LOGGER.info(
                        "relevance_filter: Navigator matched '%s' for ticker %s → industry %s",
                        result.get("company_name"),
                        ticker,
                        result.get("industry_code"),
                    )
                    return result.get("industry_code")
            LOGGER.info(
                "relevance_filter: no Navigator result matched '%s' for ticker %s — using SIC fallback",
                company_name,
                ticker,
            )
            return None
        except Exception as exc:
            LOGGER.info(
                "relevance_filter: Navigator companySearch failed for %s (%s) — using SIC fallback",
                ticker,
                exc,
            )
            return None

    def _build_factors(
        self,
        static_factors: Dict,
        navigator_code: Optional[str],
    ) -> List[MaterialFactor]:
        """
        Build the final factor list.

        If the Navigator is reachable: use its GIC list as the authoritative
        factor set, merging financial_impacts from the static map where available.

        If the Navigator is unreachable: fall back to the static map only.
        """
        if not navigator_code:
            return self._parse_factors(static_factors)

        try:
            industry_data = get_industry_topics(navigator_code)
        except Exception as exc:
            LOGGER.warning(
                "relevance_filter: Navigator + cache both unavailable for %s (%s) — "
                "falling back to static sasb_map.json (add navigator_cache or check connectivity)",
                navigator_code,
                exc,
            )
            return self._parse_factors(static_factors)

        return self._merge(flatten_topics(industry_data), static_factors, navigator_code)

    @staticmethod
    def _merge(
        navigator_gics: List[dict],
        static_factors: Dict,
        navigator_code: str,
    ) -> List[MaterialFactor]:
        """
        Merge Navigator GICs with static map data.

        For each Navigator GIC:
          - Resolve its factor_id (applying aliases for known slug differences)
          - Take financial_impacts from the static map if the factor is known there,
            otherwise infer from dimension
          - Attach the full GIC description

        The Navigator GIC list order is preserved (SASB standard order by GIC code).
        """
        factors: List[MaterialFactor] = []
        new_count = 0

        for gic in navigator_gics:
            nav_id = gic["factor_id"]
            canonical_id = _NAVIGATOR_ID_ALIASES.get(nav_id, nav_id)
            dimension = gic["dimension"]

            if canonical_id in static_factors:
                raw_impacts = static_factors[canonical_id].get("financial_impacts", [])
                financial_impacts = [i for i in raw_impacts if i in _VALID_FINANCIAL_IMPACTS]
                name = static_factors[canonical_id]["name"]
            else:
                financial_impacts = _DIMENSION_FINANCIAL_IMPACTS.get(dimension, ["cost_impact"])
                name = gic["factor_name"]
                new_count += 1

            factors.append(
                MaterialFactor(
                    factor_id=canonical_id,
                    name=name,
                    dimension=dimension,
                    financial_impacts=financial_impacts,
                    description=gic["description"],
                )
            )

        LOGGER.info(
            "Navigator merge (%s): %d factors total, %d new vs static map",
            navigator_code,
            len(factors),
            new_count,
        )
        return factors

    @staticmethod
    def _parse_factors(factors_dict: Dict) -> List[MaterialFactor]:
        """Build MaterialFactor list from the static map dict (fallback path)."""
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
