"""
Indeed RSS job fetcher — no API key required.

Indeed's public RSS endpoint returns job listings for a query within a rolling
date window. The legacy Publisher API (api.indeed.com/ads/apisearch) was shut
down in 2023; this uses the publicly accessible RSS feed instead.

URL: https://www.indeed.com/rss?q={query}&fromage={days}
  fromage = days back from today (730 ≈ 24-month rolling window per issue #9)
"""

import hashlib
import logging
import re
import xml.etree.ElementTree as ET
from typing import List

import requests

from pipeline.models import JobPosting

LOGGER = logging.getLogger(__name__)

_RSS_URL = "https://www.indeed.com/rss"
_WINDOW_DAYS = 730  # ~24-month rolling window

_SENIOR_RE = re.compile(
    r"\b(senior|sr\.?|director|vp|vice\s+president|head\s+of|chief|principal|lead|manager)\b",
    re.IGNORECASE,
)
_JUNIOR_RE = re.compile(
    r"\b(junior|jr\.?|associate|coordinator|assistant|intern|graduate|entry[\s\-]?level)\b",
    re.IGNORECASE,
)


def fetch_indeed_jobs(company: str, keywords: List[str]) -> List[JobPosting]:
    """
    Fetch job postings from Indeed RSS for a company + keyword list.

    Issues one RSS request per keyword, deduplicates by job ID, and classifies
    each posting's seniority from its title.

    Returns an empty list on any failure — caller handles missing data.
    """
    postings: List[JobPosting] = []
    for keyword in keywords:
        try:
            resp = requests.get(
                _RSS_URL,
                params={
                    "q": f'"{company}" {keyword}',
                    "fromage": _WINDOW_DAYS,
                    "limit": 25,
                },
                timeout=15,
                headers={"User-Agent": "ESGSignal/1.0 (academic research)"},
            )
            resp.raise_for_status()
            postings.extend(_parse_rss(resp.text, keyword))
        except Exception as exc:
            LOGGER.warning(f"Indeed RSS failed for {company!r} + {keyword!r}: {exc}")

    return _deduplicate(postings)


def _parse_rss(xml_text: str, keyword: str) -> List[JobPosting]:
    items = []
    try:
        root = ET.fromstring(xml_text)
        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            guid = (item.findtext("guid") or link).strip()
            job_id = hashlib.md5(guid.encode()).hexdigest()[:16]
            items.append(
                JobPosting(
                    job_id=job_id,
                    title=title,
                    date_posted=pub_date,
                    url=link,
                    source="indeed",
                    keywords_matched=[keyword],
                    seniority=_classify_seniority(title),
                )
            )
    except ET.ParseError as exc:
        LOGGER.warning(f"RSS XML parse error: {exc}")
    return items


def _deduplicate(postings: List[JobPosting]) -> List[JobPosting]:
    """Merge duplicate job_ids, combining their keywords_matched lists."""
    seen: dict = {}
    for p in postings:
        if p.job_id in seen:
            seen[p.job_id].keywords_matched = list(
                set(seen[p.job_id].keywords_matched + p.keywords_matched)
            )
        else:
            seen[p.job_id] = p
    return list(seen.values())


def _classify_seniority(title: str) -> str:
    if _SENIOR_RE.search(title):
        return "senior"
    if _JUNIOR_RE.search(title):
        return "junior"
    return "mid"
