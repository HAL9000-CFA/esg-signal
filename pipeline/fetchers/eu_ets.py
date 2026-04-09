"""
EU ETS — EUTL Verified Emissions fetcher.

Data source
-----------
European Commission Union Registry Data Portal:
  https://union-registry-data.ec.europa.eu/report/welcome

Download the "Verified Emissions" XLSX manually and place at:
  data/raw/eu_ets/verified_emissions_{year}_en.xlsx

Download URLs use per-document UUIDs and cannot be automated.  The latest annual
file (e.g. 2024) contains all years back to 2008 as columns, so only one file is
ever needed.  See README for update instructions.

File format
-----------
Wide format: one row per installation, year-specific columns:
  VERIFIED_EMISSIONS_{YEAR}  — tCO2e verified for that year (-1 = blank/excluded)
  ALLOCATION_{YEAR}          — freely allocated allowances

Key identity columns:
  IDENTIFIER_IN_REG  — account holder (operator) name
  INSTALLATION_NAME  — installation name
  REGISTRY_CODE      — country code
  MAIN_ACTIVITY_TYPE_CODE — activity code

Name matching
-------------
Uses the same word-boundary token approach as EA Pollution.  Single-token brands
(e.g. BP) may match unrelated entities (e.g. Aker BP ASA).  Documented limitation.
"""

import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd

from pipeline.fetchers.base_regulatory import BaseRegulatoryFetcher

LOGGER = logging.getLogger(__name__)

_CACHE_DIR = Path("data/raw/eu_ets")

# Header row search: scan up to this many rows for 'REGISTRY_CODE'
_HEADER_SEARCH_ROWS = 30

# The latest EUTL XLSX contains ALL years (2008–present) as columns, so we
# always prefer the most recent file rather than downloading per-year slices.
# Download URLs use document UUIDs (not predictable) — place files manually at:
#   data/raw/eu_ets/verified_emissions_{year}_en.xlsx
# Source: https://union-registry-data.ec.europa.eu/report/welcome → Verified Emissions
_KNOWN_YEARS = [2024, 2023, 2022, 2021]

# Legal suffixes stripped when extracting the core search token (mirrors ea_pollution.py)
_LEGAL_SUFFIXES = re.compile(
    r"\b(P\.?L\.?C\.?|LIMITED|LTD|INC|CORP|LLC|GROUP|HOLDINGS?|UK|"
    r"INTERNATIONAL|GLOBAL|CO|COMPANY|PLC)\b",
    re.IGNORECASE,
)


def _search_tokens(company_name: str) -> list[str]:
    cleaned = re.sub(r"[.()\[\]&,]", " ", company_name)
    cleaned = _LEGAL_SUFFIXES.sub(" ", cleaned)
    tokens = [t for t in cleaned.split() if len(t) > 1]
    return tokens[:2] if len(tokens) >= 2 else tokens[:1]


def _build_pattern(company_name: str) -> re.Pattern:
    tokens = _search_tokens(company_name)
    if len(tokens) == 2:
        expr = r"\b" + re.escape(tokens[0]) + r"\s+" + re.escape(tokens[1]) + r"\b"
    elif tokens:
        expr = r"\b" + re.escape(tokens[0]) + r"\b"
    else:
        expr = re.escape(company_name.split()[0])
    return re.compile(expr, re.IGNORECASE)


def _find_xlsx(year: int) -> Optional[Path]:
    """
    Return the best available XLSX path for the given year.

    The latest EUTL file contains all years as columns, so we prefer the most
    recent cached file rather than requiring a per-year download.  Falls back
    through known years in descending order.
    """
    # First try an exact match for the requested year (handles any naming variant)
    for candidate in _CACHE_DIR.glob(f"verified_emissions_{year}_en*.xlsx"):
        LOGGER.info("EU ETS: found exact match for year=%d: %s", year, candidate)
        return candidate

    # Fall back to the most recent available file — it contains all years as columns
    for y in _KNOWN_YEARS:
        for candidate in _CACHE_DIR.glob(f"verified_emissions_{y}_en*.xlsx"):
            LOGGER.info(
                "EU ETS: year=%d not found; using %s (contains all years as columns)",
                year,
                candidate,
            )
            return candidate

    LOGGER.warning(
        "EU ETS: no XLSX found in %s. Download from "
        "https://union-registry-data.ec.europa.eu/report/welcome and place at "
        "data/raw/eu_ets/verified_emissions_{year}_en.xlsx",
        _CACHE_DIR,
    )
    return None


def _find_header_row(xlsx_path: Path) -> int:
    """
    Scan the preamble rows to find the one containing 'REGISTRY_CODE'.
    Different annual files have slightly different preamble lengths.
    Falls back to row 20 if not found.
    """
    probe = pd.read_excel(xlsx_path, header=None, nrows=_HEADER_SEARCH_ROWS)
    for i, row in probe.iterrows():
        if any(str(v).strip().upper() == "REGISTRY_CODE" for v in row):
            return int(i)
    LOGGER.warning("EU ETS: could not detect header row in %s, defaulting to 20", xlsx_path)
    return 20


class EUETSFetcher(BaseRegulatoryFetcher):
    def fetch(self, company_name: str, year: Optional[int] = None) -> pd.DataFrame:
        """
        Fetch EU ETS verified emissions for the given company from the EUTL XLSX.

        Args:
            company_name: Company name (e.g. 'BP P.L.C.'). Core tokens are
                          extracted for matching against IDENTIFIER_IN_REG.
            year:         Reporting year (defaults to 2024). Controls which
                          VERIFIED_EMISSIONS_{year} column is selected.

        Returns:
            DataFrame with columns: account_holder, installation_name, country,
            activity_type, year, verified_emissions_tco2e, free_allowances.
            Empty DataFrame if no matching records found.
        """
        target_year = year or 2024
        pattern = _build_pattern(company_name)

        LOGGER.info(
            "EU ETS: fetching year=%d for company=%r (pattern=%r)",
            target_year,
            company_name,
            pattern.pattern,
        )

        xlsx_path = _find_xlsx(target_year)
        if xlsx_path is None:
            LOGGER.warning("EU ETS: no XLSX available for year=%d", target_year)
            return pd.DataFrame(columns=_output_columns())

        try:
            header_row = _find_header_row(xlsx_path)
            df = pd.read_excel(xlsx_path, header=header_row)
        except Exception as exc:
            LOGGER.warning("EU ETS: could not read XLSX %s: %s", xlsx_path, exc)
            return pd.DataFrame(columns=_output_columns())

        # Match on account holder name
        name_col = "IDENTIFIER_IN_REG"
        if name_col not in df.columns:
            LOGGER.warning(
                "EU ETS: expected column %r not found. Columns: %s", name_col, list(df.columns)
            )
            return pd.DataFrame(columns=_output_columns())

        mask = df[name_col].astype(str).str.contains(pattern, na=False)
        df = df[mask].copy()

        if df.empty:
            LOGGER.info(
                "EU ETS: no records found for company=%r year=%d", company_name, target_year
            )
            return pd.DataFrame(columns=_output_columns())

        # Pick the year-specific columns
        em_col = f"VERIFIED_EMISSIONS_{target_year}"
        alloc_col = f"ALLOCATION_{target_year}"

        result = pd.DataFrame(
            {
                "account_holder": df[name_col],
                "installation_name": df.get("INSTALLATION_NAME", pd.Series(dtype=str)),
                "country": df.get("REGISTRY_CODE", pd.Series(dtype=str)),
                "activity_type": df.get("MAIN_ACTIVITY_TYPE_CODE", pd.Series(dtype=str)),
                "year": target_year,
                "verified_emissions_tco2e": df[em_col] if em_col in df.columns else None,
                "free_allowances": df[alloc_col] if alloc_col in df.columns else None,
            }
        )

        # -1 means "blank / not reported" in the EUTL schema — replace with NaN
        result["verified_emissions_tco2e"] = pd.to_numeric(
            result["verified_emissions_tco2e"], errors="coerce"
        ).where(lambda x: x >= 0)
        result["free_allowances"] = pd.to_numeric(result["free_allowances"], errors="coerce").where(
            lambda x: x >= 0
        )

        result = result.reset_index(drop=True)
        LOGGER.info(
            "EU ETS: done — %d records for company=%r year=%d",
            len(result),
            company_name,
            target_year,
        )
        return result


def _output_columns() -> list[str]:
    return [
        "account_holder",
        "installation_name",
        "country",
        "activity_type",
        "year",
        "verified_emissions_tco2e",
        "free_allowances",
    ]
