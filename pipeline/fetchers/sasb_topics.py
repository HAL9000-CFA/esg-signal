"""
SASB topics fetcher — pipeline/fetchers/sasb_topics.py

Fetches SASB material topics for a given SASB Navigator industry code using the
public IFRS/SASB Navigator API (no API key required).

Fallback chain (intended integration with relevance_filter.py):
    1. Live SASB Navigator API  → cache result to data/sasb/industries/<code>.json
    2. Local cache              → used if API is unreachable
    3. data/sasb_map.json       → static fallback (handled in relevance_filter.py)

IMPORTANT — companySearch reliability:
    get_company_info(ticker) uses the Navigator's company search endpoint which
    matches on ticker symbol. This is unreliable for short tickers ("BP" → "Backstageplay
    Inc", not "BP p.l.c."). Do NOT rely on it to derive the industry code at runtime.
    Instead, derive the Navigator code from the SIC code via relevance_filter.py and
    call get_industry_topics(code) directly.

Public API:
    # Preferred — call with known Navigator code (e.g. "EM-EP" for Oil & Gas E&P)
    topics = get_industry_topics("EM-EP")
    factors = to_material_factor_dicts(topics)

    # Utilities
    flat = flatten_topics(topics)           # one dict per GIC category
    path = build_snapshot()                 # dump all cached industries to one JSON

CLI:
    python pipeline/fetchers/sasb_topics.py --industry EM-EP --flat
    python pipeline/fetchers/sasb_topics.py --ticker ULVR --flat   # unreliable for some tickers
    python pipeline/fetchers/sasb_topics.py --snapshot
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests

LOGGER = logging.getLogger(__name__)

# =========================
# Config
# =========================

BASE_URL = os.getenv("SASB_API_BASE", "https://owaeaasu09.execute-api.us-west-2.amazonaws.com/prod")

DATA_ROOT = Path("data/sasb")
COMPANY_DIR = DATA_ROOT / "companies"
INDUSTRY_DIR = DATA_ROOT / "industries"
SNAPSHOT_DIR = DATA_ROOT / "snapshot"

TIMEOUT = 5

# Dimension names in the Navigator API use "and" — our pipeline uses "&"
_DIMENSION_NORMALISE = {
    "Leadership and Governance": "Leadership & Governance",
    "Business Model and Innovation": "Business Model & Innovation",
}

# Default financial_impacts by dimension (Navigator API doesn't provide these)
_DIMENSION_FINANCIAL_IMPACTS: Dict[str, List[str]] = {
    "Environment": ["cost_impact", "asset_impact"],
    "Social Capital": ["revenue_impact", "liability_impact"],
    "Human Capital": ["cost_impact"],
    "Leadership & Governance": ["liability_impact"],
    "Business Model & Innovation": ["revenue_impact", "asset_impact"],
}


# =========================
# Dir helpers
# =========================


def _ensure_dirs():
    for d in [COMPANY_DIR, INDUSTRY_DIR, SNAPSHOT_DIR]:
        d.mkdir(parents=True, exist_ok=True)


# =========================
# HTTP
# =========================


def _get_json(url: str, params=None):
    resp = requests.get(url, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# =========================
# Cache helpers
# =========================


def _load_cache(path: Path) -> Optional[dict]:
    if path.exists():
        with open(path, "r") as f:
            return json.load(f)
    return None


def _save_cache(path: Path, data):
    _ensure_dirs()
    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)


def _company_cache_path(ticker: str) -> Path:
    return COMPANY_DIR / f"{ticker.upper()}.json"


def _industry_cache_path(code: str) -> Path:
    return INDUSTRY_DIR / f"{code}.json"


# =========================
# Core fetchers
# =========================


def get_company_info(ticker: str) -> dict:
    """
    Fetch the first company result for a ticker via Navigator company search.

    The API returns a list — use search_company_results() to get all candidates
    and match against a known company name.
    """
    results = search_company_results(ticker)
    return results[0] if results else {}


def search_company_results(ticker: str) -> list:
    """
    Return all Navigator company search results for a ticker.

    The API returns a list ordered by relevance. Index [0] is not always the
    correct company — e.g. "BP" returns Backstageplay Inc at [0], BP PLC at [1].
    Callers should match against the known company name to pick the right entry.

    Caches the full result list. Falls back to cache on API failure.
    """
    path = _company_cache_path(ticker)
    cached = _load_cache(path)

    try:
        url = f"{BASE_URL}/navigator-data/companySearch/{ticker}"
        data = _get_json(url, params={"locale": "en-gb"})

        if not data or not isinstance(data, list):
            raise ValueError(f"Unexpected response shape: {type(data).__name__}")

        _save_cache(path, data)
        LOGGER.info(
            "sasb_topics: fetched %d company result(s) for ticker %s from Navigator API",
            len(data),
            ticker,
        )
        return data

    except Exception as e:
        if cached:
            raw = cached["data"]
            # Cache may hold a single dict (old format) or a list (new format)
            results = raw if isinstance(raw, list) else [raw]
            LOGGER.warning(
                "sasb_topics: Navigator API unavailable for ticker %s (%s) — using local cache (%d result(s))",
                ticker,
                e,
                len(results),
            )
            return results
        raise RuntimeError(f"Failed to fetch company info for {ticker}") from e


def get_industry_topics(industry_code: str) -> dict:
    """
    Fetch GIC categories and topics for a SASB Navigator industry code.

    Args:
        industry_code: Navigator code, e.g. "EM-EP" (Oil & Gas E&P),
                       "CG-HP" (Household & Personal Products), "TC-SI" (Software & IT).

    Returns:
        Raw API response dict with "industry_gics" list, or cached equivalent.

    Raises:
        RuntimeError if API is unreachable and no cache exists.
    """
    path = _industry_cache_path(industry_code)
    cached = _load_cache(path)

    try:
        url = f"{BASE_URL}/navigator-data/industryTopics"
        data = _get_json(
            url,
            params={
                "industries": industry_code,
                "locale": "en-gb",
            },
        )

        if not data or not isinstance(data, list):
            raise ValueError(f"Unexpected response shape: {type(data).__name__}")

        result = data[0]
        _save_cache(path, result)
        LOGGER.info(
            "sasb_topics: fetched %s from Navigator API (%d GICs)",
            industry_code,
            len(result.get("industry_gics", [])),
        )
        return result

    except Exception as e:
        if cached:
            LOGGER.warning(
                "sasb_topics: Navigator API unavailable for industry %s (%s) — using local cache",
                industry_code,
                e,
            )
            return cached["data"]
        raise RuntimeError(f"Failed to fetch industry {industry_code}") from e


# =========================
# Public API
# =========================


def get_sasb_data_for_ticker(ticker: str) -> dict:
    """
    Full pipeline: ticker → industry code → topics.

    See WARNING on get_company_info() — ticker search is unreliable for short
    symbols. Prefer calling get_industry_topics(code) directly with a known code.
    """
    company = get_company_info(ticker)
    industry_code = company["industry_code"]
    topics = get_industry_topics(industry_code)

    return {
        "company": {
            "ticker": company["company_ticker"],
            "name": company["company_name"],
            "industry_code": industry_code,
            "industry": company["industry"],
            "sector": company["sector"],
        },
        "topics": topics,
    }


# =========================
# Utilities
# =========================


def _normalise_dimension(dim: str) -> str:
    """Normalise Navigator dimension names to our internal convention (& not 'and')."""
    return _DIMENSION_NORMALISE.get(dim, dim)


def _gic_to_factor_id(gic_name: str) -> str:
    """
    Convert a GIC category name to a snake_case factor_id.

    Examples:
        "GHG Emissions"                    → "ghg_emissions"
        "Water & Wastewater Management"    → "water_wastewater_management"
        "Critical Incident Risk Management"→ "critical_incident_risk_management"
    """
    slug = gic_name.lower()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    return slug.strip("_")


def flatten_topics(industry_data: dict) -> List[dict]:
    """
    Flatten the nested GIC structure into one dict per GIC category.

    Each GIC (General Issue Category) maps to one material factor. When a GIC
    has multiple sub-topics, the first topic code/name is used as the primary
    metric identifier; all topic codes are listed under "topic_codes".

    Returns a list of dicts with keys:
        factor_id   — snake_case slug of the GIC name
        factor_name — GIC category name (e.g. "GHG Emissions")
        topic_code  — primary topic code (first sub-topic)
        topic_codes — all topic codes under this GIC
        dimension   — normalised SASB dimension
        description — GIC description
    """
    flat = []

    for gic in industry_data.get("industry_gics", []):
        topics = gic.get("gic_topics", [])
        if not topics:
            continue

        dimension = _normalise_dimension(gic.get("gic_dimension", ""))
        gic_name = gic.get("gic_name", "")

        flat.append(
            {
                "factor_id": _gic_to_factor_id(gic_name),
                "factor_name": gic_name,
                "topic_code": topics[0]["topic_code"],
                "topic_codes": [t["topic_code"] for t in topics],
                "dimension": dimension,
                "description": gic.get("gic_description", ""),
            }
        )

    return flat


def to_material_factor_dicts(industry_data: dict) -> List[dict]:
    """
    Convert Navigator GIC data into dicts compatible with MaterialFactor.

    Note: financial_impacts are inferred from dimension (the API doesn't provide
    them). For more precise values, merge with data/sasb_map.json in
    relevance_filter.py.

    Returns list of dicts with keys: factor_id, name, dimension, financial_impacts.
    """
    factors = []
    for item in flatten_topics(industry_data):
        dim = item["dimension"]
        factors.append(
            {
                "factor_id": item["factor_id"],
                "name": item["factor_name"],
                "dimension": dim,
                "financial_impacts": _DIMENSION_FINANCIAL_IMPACTS.get(dim, ["cost_impact"]),
            }
        )
    return factors


def get_flat_topics_for_ticker(ticker: str) -> List[dict]:
    """Convenience wrapper: ticker → flat GIC list. See WARNING on get_company_info()."""
    data = get_sasb_data_for_ticker(ticker)
    return flatten_topics(data["topics"])


# =========================
# Snapshot builder
# =========================


def build_snapshot() -> Path:
    """Combine all cached industries into one JSON snapshot."""
    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "industries": {},
    }

    for file in INDUSTRY_DIR.glob("*.json"):
        with open(file) as f:
            data = json.load(f)
            snapshot["industries"][file.stem] = data["data"]

    _ensure_dirs()
    out_path = SNAPSHOT_DIR / f"sasb_full_{datetime.now(timezone.utc).date()}.json"

    with open(out_path, "w") as f:
        json.dump(snapshot, f, indent=2)

    return out_path


# =========================
# Cache warming
# =========================


def warm_industry_cache(industry_codes: List[str]):
    """Pre-fetch and cache a list of industry codes to avoid runtime API calls."""
    for code in industry_codes:
        try:
            get_industry_topics(code)
            print(f"Cached {code}")
        except Exception as e:
            print(f"Failed {code}: {e}")


# =========================
# CLI
# =========================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SASB Navigator topic fetcher")
    parser.add_argument(
        "--industry", help="Fetch topics for a Navigator industry code (e.g. EM-EP)"
    )
    parser.add_argument(
        "--ticker", help="Fetch topics via company search (unreliable for short tickers)"
    )
    parser.add_argument("--flat", action="store_true", help="Return flattened GIC list")
    parser.add_argument(
        "--factors", action="store_true", help="Return MaterialFactor-compatible dicts"
    )
    parser.add_argument(
        "--snapshot", action="store_true", help="Build snapshot of all cached industries"
    )

    args = parser.parse_args()

    if args.industry:
        data = get_industry_topics(args.industry)
        if args.factors:
            result = to_material_factor_dicts(data)
        elif args.flat:
            result = flatten_topics(data)
        else:
            result = data
        print(json.dumps(result, indent=2))

    elif args.ticker:
        if args.flat:
            result = get_flat_topics_for_ticker(args.ticker)
        else:
            result = get_sasb_data_for_ticker(args.ticker)
        print(json.dumps(result, indent=2))

    elif args.snapshot:
        path = build_snapshot()
        print(f"Snapshot saved to {path}")

    else:
        parser.print_help()
