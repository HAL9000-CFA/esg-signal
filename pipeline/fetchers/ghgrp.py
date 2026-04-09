import io
import logging
from typing import Optional

import pandas as pd
import requests

from pipeline.fetchers.base_regulatory import BaseRegulatoryFetcher

LOGGER = logging.getLogger(__name__)

# EPA EnviroFacts — facility-level GHG emissions, no API key required
# Companies emitting ≥25,000 metric tons CO2e/year must report under the GHGRP.
# This cross-checks scope 1 emissions claims in voluntary ESG disclosures.
_BASE = "https://data.epa.gov/efservice"

_COLUMNS = {
    "FACILITY_NAME": "facility_name",
    "PARENT_CO_NAME": "parent_company",
    "REPORTING_YEAR": "year",
    "GHG_QUANTITY": "ghg_quantity_mtco2e",
    "FACILITY_ID": "facility_id",
    "STATE": "state",
}


class GHGRPFetcher(BaseRegulatoryFetcher):
    def fetch(self, company_name: str, year: Optional[int] = None) -> pd.DataFrame:
        """
        Downloads facility-level GHG emissions for the given parent company name.
        Searches by PARENT_CO_NAME containing the company name (case-insensitive).
        Returns an empty DataFrame if no facilities are found.
        """
        LOGGER.info("GHGRP: fetching GHG emissions for company=%r", company_name)
        url = (
            f"{_BASE}/GHG_EMITTER_FACILITIES"
            f"/PARENT_CO_NAME/containing/{requests.utils.quote(company_name)}/CSV"
        )
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
        except requests.exceptions.Timeout:
            LOGGER.warning("GHGRP: request timed out (30s) for company=%r", company_name)
            return pd.DataFrame(columns=list(_COLUMNS.values()))
        except Exception as exc:
            LOGGER.warning("GHGRP: request failed for company=%r: %s", company_name, exc)
            return pd.DataFrame(columns=list(_COLUMNS.values()))

        if not r.text.strip():
            LOGGER.info("GHGRP: no records found for company=%r", company_name)
            return pd.DataFrame(columns=list(_COLUMNS.values()))

        df = pd.read_csv(io.StringIO(r.text))

        # keep only columns we care about, rename to snake_case
        present = {k: v for k, v in _COLUMNS.items() if k in df.columns}
        df = df[list(present.keys())].rename(columns=present)

        if year is not None and "year" in df.columns:
            df = df[df["year"] == year]

        if "year" in df.columns:
            df = df.sort_values("year", ascending=False)

        result = df.reset_index(drop=True)
        LOGGER.info("GHGRP: done — %d records for company=%r", len(result), company_name)
        return result
