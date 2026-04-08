from typing import List, Optional

import pandas as pd
import requests

from pipeline.fetchers.base_regulatory import BaseRegulatoryFetcher

# EPA ECHO (Enforcement and Compliance History Online) — no API key required.
# Returns enforcement actions, penalties, and violation history across:
#   CAA (Clean Air Act), CWA (Clean Water Act), RCRA (hazardous waste).
# This cross-checks environmental compliance claims in voluntary ESG disclosures.
_BASE = "https://echodata.epa.gov/echo"


class ECHOFetcher(BaseRegulatoryFetcher):
    def _search_facilities(self, company_name: str) -> List[dict]:
        """Returns list of facility dicts matching the company name."""
        r = requests.get(
            f"{_BASE}/facilities_search.json",
            params={"p_fn": company_name, "output": "JSON"},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        # ECHO wraps results under Results > Facilities
        return data.get("Results", {}).get("Facilities", [])

    def _get_enforcement(self, registry_id: str) -> dict:
        """Returns enforcement summary for a single facility."""
        r = requests.get(
            f"{_BASE}/caa_rest_services.get_facility_info",
            params={"p_id": registry_id, "output": "JSON"},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()

    def fetch(self, company_name: str, year: Optional[int] = None) -> pd.DataFrame:
        """
        Searches ECHO for facilities matching the company name, then retrieves
        enforcement and penalty history for each facility.
        Returns an empty DataFrame if no facilities are found.
        """
        try:
            facilities = self._search_facilities(company_name)
        except Exception:
            return pd.DataFrame()

        if not facilities:
            return pd.DataFrame()

        rows = []
        for facility in facilities:
            registry_id = facility.get("RegistryID") or facility.get("REGISTRY_ID")
            if not registry_id:
                continue
            try:
                info = self._get_enforcement(registry_id)
                fac_info = info.get("Results", {}).get("FACInfo", {})
                rows.append(
                    {
                        "facility_name": facility.get("FacName") or facility.get("FAC_NAME"),
                        "registry_id": registry_id,
                        "state": facility.get("StateCode") or facility.get("STATE_CODE"),
                        "penalty_amount": fac_info.get("TotalPenalties"),
                        "formal_actions": fac_info.get("FormalActions"),
                        "caa_violations": fac_info.get("CAAViolations"),
                        "cwa_violations": fac_info.get("CWAViolations"),
                        "rcra_violations": fac_info.get("RCRAViolations"),
                        "last_inspection_date": fac_info.get("LastInspectionDate"),
                    }
                )
            except Exception:
                # record partial row with just facility identity if enforcement call fails
                rows.append(
                    {
                        "facility_name": facility.get("FacName") or facility.get("FAC_NAME"),
                        "registry_id": registry_id,
                        "state": facility.get("StateCode") or facility.get("STATE_CODE"),
                    }
                )

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows).reset_index(drop=True)
