import io
from typing import Optional

import pandas as pd
import requests

from pipeline.fetchers.base_regulatory import BaseRegulatoryFetcher

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
        url = (
            f"{_BASE}/GHG_EMITTER_FACILITIES"
            f"/PARENT_CO_NAME/containing/{requests.utils.quote(company_name)}/CSV"
        )
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
        except Exception:
            return pd.DataFrame(columns=list(_COLUMNS.values()))

        if not r.text.strip():
            return pd.DataFrame(columns=list(_COLUMNS.values()))

        df = pd.read_csv(io.StringIO(r.text))

        # keep only columns we care about, rename to snake_case
        present = {k: v for k, v in _COLUMNS.items() if k in df.columns}
        df = df[list(present.keys())].rename(columns=present)

        if year is not None and "year" in df.columns:
            df = df[df["year"] == year]

        if "year" in df.columns:
            df = df.sort_values("year", ascending=False)

        return df.reset_index(drop=True)
