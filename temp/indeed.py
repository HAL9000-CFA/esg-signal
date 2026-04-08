import os

import requests

INDEED_KEY = os.getenv("key here for indeed")
# indeed.com/publisher


def fetch_indeed_jobs(company: str, keywords: list) -> list:
    results = []
    for keyword in keywords:
        r = requests.get(
            "http://api.indeed.com/ads/apisearch",
            params={
                "publisher": INDEED_KEY,
                "q": f"{company} {keyword}",
                "format": "json",
                "v": "2",
                "limit": 25,
            },
        )
        results.extend(r.json().get("results", []))
    return results
