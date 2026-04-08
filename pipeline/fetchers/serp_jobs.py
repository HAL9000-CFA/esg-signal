"""
SerpAPI Google Jobs fallback fetcher.

Used only when Indeed RSS returns no results. Capped at 3 keywords per call
to preserve the 100 free searches/month on SerpAPI's free tier.

Requires env var: SERPAPI_KEY (https://serpapi.com)
"""

import logging
import os
import re
from typing import List

import requests

from pipeline.models import JobPosting

LOGGER = logging.getLogger(__name__)

_SERP_URL = "https://serpapi.com/search"
_MAX_KEYWORDS = 3  # conserve free quota

_SENIOR_RE = re.compile(
    r"\b(senior|sr\.?|director|vp|vice\s+president|head\s+of|chief|principal|lead|manager)\b",
    re.IGNORECASE,
)
_JUNIOR_RE = re.compile(
    r"\b(junior|jr\.?|associate|coordinator|assistant|intern|graduate|entry[\s\-]?level)\b",
    re.IGNORECASE,
)


def fetch_serp_jobs(company: str, keywords: List[str]) -> List[JobPosting]:
    """
    Fetch job postings via SerpAPI Google Jobs for a company + keyword list.

    Skips silently if SERPAPI_KEY is not set. Returns empty list on failure.
    """
    api_key = os.getenv("SERPAPI_KEY")
    if not api_key:
        LOGGER.info("SERPAPI_KEY not set — skipping SerpAPI fallback")
        return []

    postings: List[JobPosting] = []
    for keyword in keywords[:_MAX_KEYWORDS]:
        try:
            resp = requests.get(
                _SERP_URL,
                params={
                    "engine": "google_jobs",
                    "q": f"{company} {keyword}",
                    "api_key": api_key,
                },
                timeout=15,
            )
            resp.raise_for_status()
            for job in resp.json().get("jobs_results", []):
                title = job.get("title", "")
                job_id = (job.get("job_id") or title)[:24]
                postings.append(
                    JobPosting(
                        job_id=job_id,
                        title=title,
                        date_posted=job.get("detected_extensions", {}).get("posted_at", ""),
                        url=job.get("share_link", ""),
                        source="serp",
                        keywords_matched=[keyword],
                        seniority=_classify_seniority(title),
                    )
                )
        except Exception as exc:
            LOGGER.warning(f"SerpAPI failed for {company!r} + {keyword!r}: {exc}")

    return postings


def _classify_seniority(title: str) -> str:
    if _SENIOR_RE.search(title):
        return "senior"
    if _JUNIOR_RE.search(title):
        return "junior"
    return "mid"
