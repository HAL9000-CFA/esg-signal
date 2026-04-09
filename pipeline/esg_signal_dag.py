"""
ESG Signal — Apache Airflow DAG (issue #14)

Wires the full analysis pipeline in sequence with proper dependency management.
Runs inside Docker only (Apache Airflow 2.9.3 in requirements-airflow.txt).

Task graph
----------
    fetch_data
    ├── run_talent              ← parallel: both only need the company profile
    └── run_relevance_filter
          └── run_credibility_scorer
                └── run_dcf_mapper   ← skipped if dcf_path param is empty
                      └── audit_summary

Note: PDF extraction runs inside fetch_data (integrated in DataGatherer.fetch_all).

DAG parameters (trigger with JSON conf)
---------------------------------------
    ticker       : Company ticker, e.g. "AAPL" or "BP"  [required]
    company_name : Full company name, e.g. "Apple Inc."   [required]
    index        : "SP500" or "FTSE100"                   [required]
    dcf_path     : Local path to Excel DCF model          [optional]

Retry policy
------------
Tasks that hit external APIs use retries=3 with exponential backoff.
Pure-local tasks (relevance filter, DCF mapper, audit summary) are not retried.

run_id threading
----------------
Airflow's dag_run.run_id is injected into every LLM call so the audit log
(data/audit_log.jsonl) groups all API calls for one pipeline run under a single
identifier.  The audit_summary task at the end pulls the per-run cost and token
totals and pushes them to XCom for the Streamlit UI to display.
"""

import dataclasses
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default args — retry policy applied per-task below
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "owner": "esg-signal",
    "depends_on_past": False,
}

# External API tasks: 3 retries, 2-min base delay, exponential backoff
_API_RETRY_KWARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}

# Local-only tasks: no retries, shorter timeout
_LOCAL_KWARGS = {
    "retries": 0,
    "execution_timeout": timedelta(minutes=10),
}

# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------
# Airflow XComs accept any JSON-serialisable value.  We use dataclasses.asdict()
# to convert dataclass instances to plain dicts for push, and explicit
# from-dict constructors to rebuild them on pull.  This keeps the contracts
# explicit and avoids hidden coupling to pickle.


def _to_xcom(obj) -> Dict:
    """
    Convert a dataclass (possibly nested) to a plain JSON-serialisable dict.
    NaN/Inf floats are replaced with None so Airflow's JSON backend doesn't drop
    the XCom entry silently.
    """
    import math

    def _sanitise(v):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        if isinstance(v, dict):
            return {k: _sanitise(val) for k, val in v.items()}
        if isinstance(v, list):
            return [_sanitise(i) for i in v]
        return v

    return _sanitise(dataclasses.asdict(obj))


def _profile_from_dict(d: Dict):
    """Reconstruct a CompanyProfile from an xcom_pull dict."""
    from pipeline.models import CompanyProfile, FilingMetadata

    fm = d.get("latest_annual_filing")
    return CompanyProfile(
        ticker=d.get("ticker"),
        name=d["name"],
        index=d.get("index"),
        sic_code=d.get("sic_code"),
        sic_description=d.get("sic_description"),
        country=d.get("country", ""),
        latest_annual_filing=FilingMetadata(**fm) if fm else None,
        annual_report_text=d.get("annual_report_text"),
        raw_financials=d.get("raw_financials") or {},
        source_urls=d.get("source_urls") or [],
        errors=d.get("errors") or [],
        identifier=d.get("identifier"),
    )


def _talent_from_dict(d: Dict):
    """Reconstruct a TalentSignalResult from an xcom_pull dict."""
    from pipeline.models import TalentSignalResult

    return TalentSignalResult(
        company_name=d["company_name"],
        total_postings=d["total_postings"],
        senior_ratio=d["senior_ratio"],
        ghost_count=d["ghost_count"],
        factor_scores=d.get("factor_scores") or {},
        errors=d.get("errors") or [],
    )


def _relevance_from_dict(d: Dict):
    """Reconstruct a RelevanceFilterResult from an xcom_pull dict."""
    from pipeline.models import MaterialFactor, RelevanceFilterResult

    factors = [
        MaterialFactor(
            factor_id=f["factor_id"],
            name=f["name"],
            dimension=f["dimension"],
            financial_impacts=f.get("financial_impacts") or [],
        )
        for f in (d.get("material_factors") or [])
    ]
    return RelevanceFilterResult(
        ticker=d.get("ticker"),
        sic_code=d.get("sic_code"),
        sasb_industry=d.get("sasb_industry"),
        material_factors=factors,
        errors=d.get("errors") or [],
    )


def _credibility_from_dict(d: Dict):
    """Reconstruct a CredibilityReport from an xcom_pull dict."""
    from pipeline.models import CommitmentCheck, CredibilityReport, FactorScore

    def _wm_checks(raw: Optional[List]) -> Optional[List]:
        if not raw:
            return None
        return [CommitmentCheck(**c) for c in raw]

    factor_scores = [
        FactorScore(
            factor_id=fs["factor_id"],
            factor_name=fs["factor_name"],
            score=fs["score"],
            flag=fs["flag"],
            coverage=fs.get("coverage", 1.0),
            confidence=fs.get("confidence", fs.get("coverage", 1.0)),
            stream_scores=fs.get("stream_scores") or {},
            evidence=fs.get("evidence") or [],
            sources=fs.get("sources") or [],
            narrative=fs.get("narrative") or "",
            words_money_checks=_wm_checks(fs.get("words_money_checks")),
        )
        for fs in (d.get("factor_scores") or [])
    ]
    return CredibilityReport(
        ticker=d.get("ticker"),
        company_name=d["company_name"],
        sasb_industry=d.get("sasb_industry"),
        factor_scores=factor_scores,
        overall_score=d["overall_score"],
        overall_flag=d["overall_flag"],
        errors=d.get("errors") or [],
    )


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def _task_fetch_data(**context) -> Dict:
    """
    Task 1 — Data gathering.

    Fetches annual report, SIC code, financial statements and regulatory data.
    PDF extraction runs inside DataGatherer.fetch_all() via LayoutParser/Gemini.
    Pushes DataGathererResult as an XCom dict to downstream tasks.

    Retry policy: 3 attempts, exponential backoff (EDGAR/CH/EPA rate limits).
    """
    from agents.data_gathering import DataGatherer

    params = context["params"]
    ticker: str = params["ticker"]
    company_name: str = params["company_name"]
    index: str = params["index"]

    LOGGER.info("fetch_data: ticker=%s company=%s index=%s", ticker, company_name, index)

    gatherer = DataGatherer()
    result = gatherer.fetch_all(ticker=ticker, company_name=company_name, index=index)

    LOGGER.info("fetch_data: source_statuses=%s", json.dumps(result.source_statuses, default=str))

    # Ensure profile.ticker always reflects the user-entered ticker.
    # EDGAR may return a different or null ticker (e.g. ULVR is an LSE ticker;
    # EDGAR knows Unilever as UL), which would break Navigator lookup downstream.
    if result.profile and not result.profile.ticker:
        result.profile.ticker = ticker

    return {
        "profile": _to_xcom(result.profile) if result.profile else None,
        "source_statuses": result.source_statuses,
        "regulatory_paths": result.regulatory_paths,
    }


def _task_run_talent(**context) -> Dict:
    """
    Task 2 — Talent Signal (parallel with relevance filter).

    Scrapes ESG-related job postings and detects ghost listings.
    Returns TalentSignalResult as an XCom dict.

    Retry policy: 3 attempts, exponential backoff (Indeed/SerpAPI rate limits).
    """
    from pipeline.talent_signal import TalentSignal

    params = context["params"]
    ticker: str = params["ticker"]
    company_name: str = params["company_name"]

    LOGGER.info("run_talent: ticker=%s", ticker)

    result = TalentSignal(ticker=ticker).analyse(company_name)

    LOGGER.info(
        "run_talent: postings=%d senior_ratio=%.2f ghost_count=%d",
        result.total_postings,
        result.senior_ratio,
        result.ghost_count,
    )

    return _to_xcom(result)


def _task_run_relevance_filter(**context) -> Dict:
    """
    Task 3 — SASB Relevance Filter (parallel with talent signal).

    Applies the SASB Materiality Map to identify which ESG factors are
    financially material for the company's industry.

    Retry policy: none (purely local — reads data/sasb_map.json).
    """
    from agents.relevance_filter import RelevanceFilter

    ti = context["ti"]
    fetch_result: Dict = ti.xcom_pull(task_ids="fetch_data", key="return_value")

    if not fetch_result or not fetch_result.get("profile"):
        raise ValueError("fetch_data produced no profile — cannot run relevance filter")

    profile = _profile_from_dict(fetch_result["profile"])

    LOGGER.info("run_relevance_filter: ticker=%s sic_code=%s", profile.ticker, profile.sic_code)

    result = RelevanceFilter().filter(profile)

    LOGGER.info(
        "run_relevance_filter: industry=%s factors=%d",
        result.sasb_industry,
        len(result.material_factors),
    )

    return _to_xcom(result)


def _task_run_credibility_scorer(**context) -> Dict:
    """
    Task 4 — Multi-source Credibility Scorer.

    Aggregates five independent evidence streams (disclosure quality, words vs
    money, talent signal, mandatory disclosures, supply chain) into a per-factor
    Commitment Credibility Score with a green / amber / red confidence flag.

    Calls Claude once per factor for qualitative narrative synthesis.

    Retry policy: 3 attempts, exponential backoff (Claude API).
    """
    from agents.credibility_scorer import CredibilityScorer

    ti = context["ti"]
    run_id: str = context["run_id"]

    fetch_result: Dict = ti.xcom_pull(task_ids="fetch_data", key="return_value")
    talent_dict: Dict = ti.xcom_pull(task_ids="run_talent", key="return_value")
    relevance_dict: Dict = ti.xcom_pull(task_ids="run_relevance_filter", key="return_value")

    if not fetch_result or not fetch_result.get("profile"):
        raise ValueError("fetch_data produced no profile — cannot score credibility")

    profile = _profile_from_dict(fetch_result["profile"])
    regulatory_paths: Dict = fetch_result.get("regulatory_paths") or {}
    relevance_result = _relevance_from_dict(relevance_dict)
    talent_result = _talent_from_dict(talent_dict) if talent_dict else None

    LOGGER.info(
        "run_credibility_scorer: ticker=%s factors=%d run_id=%s",
        profile.ticker,
        len(relevance_result.material_factors),
        run_id,
    )

    result = CredibilityScorer().score(
        profile=profile,
        relevance_result=relevance_result,
        talent_result=talent_result,
        regulatory_paths=regulatory_paths,
        run_id=run_id,
    )

    LOGGER.info(
        "run_credibility_scorer: overall_score=%.4f flag=%s",
        result.overall_score,
        result.overall_flag,
    )

    result_dict = _to_xcom(result)

    # Write result to disk so the UI can load it even if XCom retrieval fails.
    # Path is deterministic from ticker so the UI can find it without the run_id.
    import json as _json
    from pathlib import Path as _Path

    ticker = profile.ticker or "unknown"
    out_path = _Path(f"data/processed/credibility_{ticker}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        out_path.write_text(_json.dumps(result_dict, indent=2))
        LOGGER.info("run_credibility_scorer: result written to %s", out_path)
    except Exception as exc:
        LOGGER.warning("run_credibility_scorer: could not write result to disk: %s", exc)

    return result_dict


def _task_run_dcf_mapper(**context) -> Dict:
    """
    Task 5 — DCF Line Item Mapper (optional).

    Maps each ESG risk to the specific line items in the analyst's Excel DCF
    model and attaches scenario impact ranges from published regulatory
    precedents.

    Skipped gracefully (returns empty result) if no dcf_path was provided.
    The Excel file is processed locally by openpyxl — only label text is sent
    to Claude for mapping; financial figures never leave the local machine.

    Retry policy: none (local Excel parsing + Claude; error is non-blocking).
    """
    from agents.dcf_mapper import DcfMapper

    params = context["params"]
    dcf_path: str = params.get("dcf_path", "")
    run_id: str = context["run_id"]
    ti = context["ti"]

    if not dcf_path:
        LOGGER.info("run_dcf_mapper: no dcf_path provided — skipping")
        return {"skipped": True, "reason": "no dcf_path provided"}

    fetch_result: Dict = ti.xcom_pull(task_ids="fetch_data", key="return_value")
    credibility_dict: Dict = ti.xcom_pull(task_ids="run_credibility_scorer", key="return_value")
    relevance_dict: Dict = ti.xcom_pull(task_ids="run_relevance_filter", key="return_value")

    relevance_result = _relevance_from_dict(relevance_dict)
    credibility_report = _credibility_from_dict(credibility_dict)
    regulatory_paths: Dict = (fetch_result or {}).get("regulatory_paths") or {}

    LOGGER.info("run_dcf_mapper: dcf_path=%s run_id=%s", dcf_path, run_id)

    result = DcfMapper().map(
        excel_path=dcf_path,
        material_factors=relevance_result.material_factors,
        credibility_report=credibility_report,
        regulatory_paths=regulatory_paths,
        run_id=run_id,
    )

    LOGGER.info(
        "run_dcf_mapper: mapped=%d unmapped=%d errors=%d",
        len(result.mappings),
        len(result.unmapped_factors),
        len(result.errors),
    )

    return _to_xcom(result)


def _task_audit_summary(**context) -> Dict:
    """
    Task 6 — Audit log summary.

    Reads data/audit_log.jsonl, filters to this run_id, and pushes a cost and
    token summary to XCom.  Satisfies Rules 4.4 (Reproducibility) and 4.5
    (Disclosure) of the CFA AI Investment Challenge.

    Also logs the summary at INFO level so it appears in Airflow task logs.
    """
    from pipeline.audit_log import summarise

    run_id: str = context["run_id"]

    summary = summarise(run_id=run_id)

    LOGGER.info(
        "audit_summary run_id=%s: calls=%s live=%s cached=%s tokens=%s cost_usd=%s",
        run_id,
        summary.get("total_calls", 0),
        summary.get("live_calls", 0),
        summary.get("cached_calls", 0),
        summary.get("total_tokens", 0),
        summary.get("actual_cost_usd", 0.0),
    )

    return summary


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="esg_signal",
    description="ESG Commitment Credibility Briefing pipeline — HAL 9000 / UEA",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["esg-signal"],
    default_args=_DEFAULT_ARGS,
    params={
        "ticker": Param(
            default="",
            type="string",
            description="FTSE 100 or S&P 500 ticker symbol, e.g. AAPL or BP",
        ),
        "company_name": Param(
            default="",
            type="string",
            description="Full company name used for Companies House search and job postings",
        ),
        "index": Param(
            default="SP500",
            enum=["SP500", "FTSE100"],
            description="Which index the company belongs to",
        ),
        "dcf_path": Param(
            default="",
            type="string",
            description="Local path to analyst's Excel DCF model (leave empty to skip DCF mapping)",
        ),
    },
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=_task_fetch_data,
        **_API_RETRY_KWARGS,
    )

    run_talent = PythonOperator(
        task_id="run_talent",
        python_callable=_task_run_talent,
        **_API_RETRY_KWARGS,
    )

    run_relevance_filter = PythonOperator(
        task_id="run_relevance_filter",
        python_callable=_task_run_relevance_filter,
        **_LOCAL_KWARGS,
    )

    run_credibility_scorer = PythonOperator(
        task_id="run_credibility_scorer",
        python_callable=_task_run_credibility_scorer,
        **_API_RETRY_KWARGS,
    )

    run_dcf_mapper = PythonOperator(
        task_id="run_dcf_mapper",
        python_callable=_task_run_dcf_mapper,
        **_LOCAL_KWARGS,
    )

    audit_summary = PythonOperator(
        task_id="audit_summary",
        python_callable=_task_audit_summary,
        **_LOCAL_KWARGS,
    )

    # ------------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------------
    # fetch_data must complete before talent and relevance can start.
    # Both run in parallel — neither depends on the other.
    # credibility_scorer waits for all three upstream tasks.
    # dcf_mapper and audit_summary run sequentially at the end.

    fetch_data >> [run_talent, run_relevance_filter]
    [run_talent, run_relevance_filter] >> run_credibility_scorer
    run_credibility_scorer >> run_dcf_mapper >> audit_summary
