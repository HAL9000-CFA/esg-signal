"""
Talent Signal — hiring evidence stream for the credibility scorer (issue #11).

Analyses ESG-related job postings to detect whether a company's ESG claims are
backed by real hiring activity. Three signals are combined:

  1. Volume     — how many ESG-related postings exist (relative to total)
  2. Seniority  — senior hires signal real programmes; junior-only suggests tokenism
  3. Ghost rate — postings that disappeared without a hire signal performative listings

Data sources (in priority order):
  1. Indeed RSS (primary — no key, publicly accessible)
  2. SerpAPI Google Jobs (fallback — requires SERPAPI_KEY, 100 free/month)

Known limitation: LinkedIn headcount pages would provide useful workforce-size
context, but all compliant access methods (Proxycurl etc.) are paid services.
Headcount normalisation is left as a future enhancement when a free/compliant
source is available.

Output: TalentSignalResult consumed by agents/credibility_scorer.py (issue #11).
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

from pipeline.fetchers.indeed_jobs import fetch_indeed_jobs
from pipeline.fetchers.serp_jobs import fetch_serp_jobs
from pipeline.models import JobPosting, TalentSignalResult

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ESG keyword map — placeholder until SASB materiality (issue #10) is built.
# Keys become the factor names in TalentSignalResult.factor_scores.
# ---------------------------------------------------------------------------
_ESG_FACTOR_KEYWORDS: Dict[str, List[str]] = {
    "environment": [
        "sustainability",
        "environmental",
        "climate",
        "carbon",
        "net zero",
        "emissions",
        "renewable energy",
        "circular economy",
    ],
    "social": [
        "diversity",
        "inclusion",
        "equity",
        "dei",
        "wellbeing",
        "health and safety",
        "community",
        "human rights",
    ],
    "governance": [
        "compliance",
        "ethics",
        "governance",
        "risk management",
        "esg reporting",
        "audit",
        "transparency",
    ],
}

# A posting unseen for this many days is flagged as a potential ghost listing.
_GHOST_THRESHOLD_DAYS = 30


class TalentSignal:
    """
    Fetches and analyses ESG-related job postings for a company.

    Ghost detection is stateful: job IDs are persisted between runs in
    data/raw/talent_ghost_{ticker}.json. On first run, no ghosts are detected.

    Usage:
        result = TalentSignal(ticker="AAPL").analyse("Apple Inc.")
    """

    def __init__(self, ticker: str, ghost_store_dir: str = "data/raw"):
        self.ticker = ticker
        self._ghost_path = Path(ghost_store_dir) / f"talent_ghost_{ticker}.json"

    def analyse(self, company_name: str) -> TalentSignalResult:
        """
        Fetch postings, run ghost detection, compute per-factor scores.

        Args:
            company_name: Full company name used for job search queries.

        Returns:
            TalentSignalResult with factor_scores ready for credibility scorer.
        """
        errors: List[str] = []
        all_keywords = [kw for kws in _ESG_FACTOR_KEYWORDS.values() for kw in kws]

        # Fetch from Indeed (primary)
        postings = fetch_indeed_jobs(company_name, all_keywords)

        # Fallback to SerpAPI if Indeed returned nothing
        if not postings:
            LOGGER.info(f"Indeed returned no results for {company_name!r} — trying SerpAPI")
            postings = fetch_serp_jobs(company_name, all_keywords)

        if not postings:
            errors.append("No job postings found from any source")

        # Ghost detection (stateful)
        postings, ghost_count = self._detect_ghosts(postings)

        total = len(postings)
        senior_count = sum(1 for p in postings if p.seniority == "senior")
        senior_ratio = senior_count / total if total > 0 else 0.0

        factor_scores = self._score_factors(postings, total, senior_ratio, ghost_count)

        return TalentSignalResult(
            company_name=company_name,
            total_postings=total,
            senior_ratio=round(senior_ratio, 4),
            ghost_count=ghost_count,
            factor_scores=factor_scores,
            errors=errors,
        )

    # ------------------------------------------------------------------
    # Ghost detection
    # ------------------------------------------------------------------

    def _detect_ghosts(self, postings: List[JobPosting]) -> Tuple[List[JobPosting], int]:
        """
        Compare current postings against the persisted ghost store.

        Any job ID that was first seen ≥ 30 days ago and is absent from the
        current results is flagged as a ghost listing.

        Returns the original postings list unchanged, and the ghost count.
        """
        store = self._load_ghost_store()
        now = datetime.now(timezone.utc)
        now_str = now.isoformat()
        current_ids = {p.job_id for p in postings}

        ghost_count = 0

        # Flag IDs in the store that have now disappeared after 30+ days
        for job_id, meta in store.items():
            if job_id not in current_ids:
                first_seen = datetime.fromisoformat(meta["first_seen"])
                if (now - first_seen) >= timedelta(days=_GHOST_THRESHOLD_DAYS):
                    meta["ghost"] = True
                    ghost_count += 1

        # Upsert current postings into the store
        for p in postings:
            if p.job_id not in store:
                store[p.job_id] = {"first_seen": now_str, "last_seen": now_str, "ghost": False}
            else:
                store[p.job_id]["last_seen"] = now_str

        self._save_ghost_store(store)
        return postings, ghost_count

    def _load_ghost_store(self) -> dict:
        if self._ghost_path.exists():
            try:
                return json.loads(self._ghost_path.read_text())
            except Exception as exc:
                LOGGER.warning(f"Could not load ghost store at {self._ghost_path}: {exc}")
        return {}

    def _save_ghost_store(self, store: dict) -> None:
        try:
            self._ghost_path.parent.mkdir(parents=True, exist_ok=True)
            self._ghost_path.write_text(json.dumps(store, indent=2))
        except Exception as exc:
            LOGGER.warning(f"Could not save ghost store to {self._ghost_path}: {exc}")

    # ------------------------------------------------------------------
    # Per-factor scoring
    # ------------------------------------------------------------------

    def _score_factors(
        self,
        postings: List[JobPosting],
        total: int,
        senior_ratio: float,
        ghost_count: int,
    ) -> Dict[str, float]:
        """
        Compute a 0.0–1.0 hiring signal score for each ESG factor.

        Formula:
          base        = factor_postings / max(total, 1)
          senior_mult = 1.0 + (senior_ratio * 0.5)   # up to 1.5× for all-senior
          ghost_pen   = max(0.5, 1.0 - ghost_count / max(total, 1))
          score       = min(1.0, base * senior_mult * ghost_pen)
        """
        senior_mult = 1.0 + (senior_ratio * 0.5)
        ghost_pen = max(0.5, 1.0 - ghost_count / max(total, 1))

        scores: Dict[str, float] = {}
        for factor, keywords in _ESG_FACTOR_KEYWORDS.items():
            factor_postings = sum(
                1 for p in postings if any(kw in p.keywords_matched for kw in keywords)
            )
            base = factor_postings / max(total, 1)
            scores[factor] = round(min(1.0, base * senior_mult * ghost_pen), 4)

        return scores
