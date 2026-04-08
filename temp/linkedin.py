import os

import requests

PROXYCURL_KEY = os.getenv("key")


def fetch_linkedin_jobs(company_linkedin_url: str) -> list:
    r = requests.get(
        "https://nubela.co/proxycurl/api/linkedin/company/job",
        params={"url": company_linkedin_url, "count": 25},
        headers={"Authorization": f"Bearer {PROXYCURL_KEY}"},
    )
    return r.json().get("job", [])


# signup nubela.co/proxycurl get key
