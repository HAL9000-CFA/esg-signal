import io
from typing import Optional

import pandas as pd
import requests

from pipeline.fetchers.base_regulatory import BaseRegulatoryFetcher

# UK Environment Agency Pollution Inventory — no API key required.
# Published annually as a structured CSV via the gov.uk open data portal.
# Covers facility-level pollutant releases to air, water, and land under IPPC/IED permits.
# This cross-checks environmental compliance claims for FTSE 100 companies.
#
# Entry point: CKAN package API returns the latest resource download URL,
# avoiding the need to hardcode a year-specific file path.
_CKAN_API = "https://www.data.gov.uk/api/3/action/package_show"
_PACKAGE_ID = "pollution-inventory"

_COLUMNS = {
    "OperatorName": "operator_name",
    "SiteName": "site_name",
    "Town": "town",
    "CountyRegion": "county",
    "Pollutant": "pollutant",
    "Medium": "medium",
    "TotalRelease": "total_release_tonnes",
    "Threshold": "threshold_tonnes",
    "Year": "year",
    "NACECode": "nace_code",
    "NACEDescription": "nace_description",
}


class EAPollutionFetcher(BaseRegulatoryFetcher):
    def _get_csv_url(self) -> Optional[str]:
        """Discovers the latest CSV download URL from the gov.uk CKAN API."""
        r = requests.get(
            _CKAN_API,
            params={"id": _PACKAGE_ID},
            timeout=15,
        )
        r.raise_for_status()
        resources = r.json().get("result", {}).get("resources", [])
        for res in resources:
            if res.get("format", "").upper() == "CSV":
                return res.get("url")
        return None

    def fetch(self, company_name: str, year: Optional[int] = None) -> pd.DataFrame:
        """
        Downloads the EA Pollution Inventory and filters to rows where
        OperatorName contains the company name (case-insensitive).
        Returns an empty DataFrame if no matching records are found.
        """
        try:
            csv_url = self._get_csv_url()
            if not csv_url:
                return pd.DataFrame(columns=list(_COLUMNS.values()))

            r = requests.get(csv_url, timeout=60)
            r.raise_for_status()
        except Exception:
            return pd.DataFrame(columns=list(_COLUMNS.values()))

        if not r.text.strip():
            return pd.DataFrame(columns=list(_COLUMNS.values()))

        try:
            df = pd.read_csv(io.StringIO(r.text))
        except Exception:
            return pd.DataFrame(columns=list(_COLUMNS.values()))

        # filter to company
        name_col = next((c for c in df.columns if "operator" in c.lower()), None)
        if name_col is None:
            return pd.DataFrame(columns=list(_COLUMNS.values()))

        mask = df[name_col].str.contains(company_name, case=False, na=False)
        df = df[mask].copy()

        if df.empty:
            return pd.DataFrame(columns=list(_COLUMNS.values()))

        present = {k: v for k, v in _COLUMNS.items() if k in df.columns}
        df = df[list(present.keys())].rename(columns=present)

        if year is not None and "year" in df.columns:
            df = df[df["year"] == year]

        if "year" in df.columns:
            df = df.sort_values("year", ascending=False)

        return df.reset_index(drop=True)
