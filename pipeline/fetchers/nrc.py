from typing import Optional

import pandas as pd
import requests

from pipeline.fetchers.base_regulatory import BaseRegulatoryFetcher

# NRC (National Response Center) — legally mandated spill and release reports.
# Companies must report incidents to the NRC within 24 hours; reports are public.
# This cross-checks safety and incident claims in voluntary ESG disclosures.
#
# NOTE: The NRC REST API is less stable than EPA APIs. This fetcher fails
# gracefully — returning an empty DataFrame rather than raising — so a transient
# NRC outage does not block the pipeline.
_BASE = "https://nrc.orda.gov/publicapplication/api"


class NRCFetcher(BaseRegulatoryFetcher):
    def fetch(self, company_name: str, year: Optional[int] = None) -> pd.DataFrame:
        """
        Fetches spill and release incident reports filed by or involving the company.
        Returns an empty DataFrame if no incidents are found or the API is unavailable.
        """
        payload = {"reportingOrg": company_name}
        if year is not None:
            payload["year"] = year

        try:
            r = requests.post(
                f"{_BASE}/incidents",
                json=payload,
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
        except Exception:
            return pd.DataFrame()

        incidents = data if isinstance(data, list) else data.get("incidents", [])
        if not incidents:
            return pd.DataFrame()

        df = pd.DataFrame(incidents)

        rename = {
            "reportNumber": "report_number",
            "incidentDate": "incident_date",
            "material": "material",
            "quantity": "quantity",
            "unit": "unit",
            "description": "description",
            "companyName": "company_name",
            "state": "state",
        }
        present = {k: v for k, v in rename.items() if k in df.columns}
        df = df[list(present.keys())].rename(columns=present)

        if "incident_date" in df.columns:
            df = df.sort_values("incident_date", ascending=False)

        return df.reset_index(drop=True)
