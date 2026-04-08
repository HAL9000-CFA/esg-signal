import os

import requests

SERPAPI_KEY = os.getenv("key")


# serpapi.com - 100 searches free
def fetch_google_jobs(company: str, keyword: str) -> list:
    r = requests.get(
        "https://serpapi.com/search",
        params={
            "engine": "google_jobs",
            "q": f"{company} {keyword}",
            "api_key": SERPAPI_KEY,
        },
    )
    return r.json().get("jobs_results", [])
