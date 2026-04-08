import io
import zipfile
from typing import Optional

import pandas as pd
import requests

from pipeline.fetchers.base_regulatory import BaseRegulatoryFetcher

# EU ETS (Emissions Trading System) verified emissions — no API key required.
# Published annually by the European Environment Agency from the EU Transaction Log (EUTL).
# Covers verified CO2e emissions from all regulated industrial installations across the EU.
# This cross-checks scope 1 emissions claims for companies with European operations.
_EEA_URL = (
    "https://www.eea.europa.eu/data-and-maps/data/"
    "european-union-emissions-trading-scheme-17/"
    "eu-ets-data-download-latest-version/euets.csv.zip/at_download/file"
)

_COLUMNS = {
    "accountHolderName": "account_holder",
    "installationName": "installation_name",
    "country": "country",
    "mainActivityType": "activity_type",
    "year": "year",
    "verifiedEmissions": "verified_emissions_tco2e",
    "allocatedFreeAllowances": "free_allowances",
}


class EUETSFetcher(BaseRegulatoryFetcher):
    def fetch(self, company_name: str, year: Optional[int] = None) -> pd.DataFrame:
        """
        Downloads the EEA EU ETS dataset (zipped CSV), filters to rows where
        accountHolderName contains the company name (case-insensitive).
        Returns an empty DataFrame if no matching records are found.
        """
        try:
            r = requests.get(_EEA_URL, timeout=60)
            r.raise_for_status()
            content = r.content
        except Exception:
            return pd.DataFrame(columns=list(_COLUMNS.values()))

        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
                if csv_name is None:
                    return pd.DataFrame(columns=list(_COLUMNS.values()))
                with zf.open(csv_name) as f:
                    df = pd.read_csv(f, encoding="utf-8", low_memory=False)
        except Exception:
            return pd.DataFrame(columns=list(_COLUMNS.values()))

        # filter to company
        name_col = next(
            (c for c in df.columns if "accountholder" in c.lower() or "operator" in c.lower()),
            None,
        )
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
