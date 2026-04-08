from typing import Optional

import requests
from dotenv import load_dotenv

from pipeline.fetchers.base import BaseFetcher
from pipeline.models import CompanyProfile

load_dotenv()


class CompaniesHouseFetcher(BaseFetcher):
    BASE_URL = "https://api.company-information.service.gov.uk"

    def __init__(self, api_key: str):
        self.auth = (api_key, "")

    def search(self, name: str) -> Optional[str]:
        r = requests.get(
            f"{self.BASE_URL}/search/companies",
            params={"q": name},
            auth=self.auth,
            timeout=10,
        )
        r.raise_for_status()

        items = r.json().get("items", [])
        if not items:
            return None

        return items[0]["company_number"]

    def get_profile(self, number: str) -> dict:
        r = requests.get(
            f"{self.BASE_URL}/company/{number}",
            auth=self.auth,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    def fetch(self, company_name: str) -> CompanyProfile:
        number = self.search(company_name)
        if not number:
            raise ValueError("Company not found")

        profile = self.get_profile(number)

        return CompanyProfile(
            name=profile["company_name"],
            ticker=None,
            country="UK",
            listing_country="UK",
            incorporation_state=None,
            identifier=number,
            extra=profile,
        )
