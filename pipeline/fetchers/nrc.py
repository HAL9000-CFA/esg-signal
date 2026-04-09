import logging
from typing import Optional

import pandas as pd
import requests

from pipeline.fetchers.base_regulatory import BaseRegulatoryFetcher

LOGGER = logging.getLogger(__name__)

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

        LOGGER.info("NRC: fetching incident reports for company=%r", company_name)
        try:
            r = requests.post(
                f"{_BASE}/incidents",
                json=payload,
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
        except requests.exceptions.Timeout:
            LOGGER.warning("NRC: request timed out (30s) for company=%r", company_name)
            return pd.DataFrame()
        except Exception as exc:
            LOGGER.warning("NRC: request failed for company=%r: %s", company_name, exc)
            return pd.DataFrame()

        incidents = data if isinstance(data, list) else data.get("incidents", [])
        if not incidents:
            LOGGER.info("NRC: no incidents found for company=%r", company_name)
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

        result = df.reset_index(drop=True)
        LOGGER.info("NRC: done — %d incidents for company=%r", len(result), company_name)
        return result
