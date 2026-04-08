"""
Append-only JSONL audit log for all LLM calls.
Satisfies Rules 4.4 (Reproducability) and 4.5 (Disclosure)
"""

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

LOCK = threading.Lock()
LOG_PATH = Path(os.getenv("AUDIT_LOG_PATH", "data/audit_log.jsonl"))
with open("config/pricing.json") as f:
    PRICES = json.load(f)


def compute_cost(model: str, *, input_tokens: int, output_tokens: int) -> float:
    if model not in PRICES:
        LOGGER.warning(
            f"Price not available for model '{model}' (see config/pricing.json). Using '0.0'."
        )
        return 0.0

    input_price = PRICES[model]["pricing_per_1m_usd"]["input"]
    output_price = PRICES[model]["pricing_per_1m_usd"]["output"]

    return (input_price / 1_000_000 * input_tokens) + (output_price / 1_000_000 * output_tokens)


def log_llm_call(
    *,
    agent: str,
    model: str,
    version: str,
    purpose: str,
    input_tokens: str,
    output_tokens: str,
    cost_usd: float,
    cached: bool = False,
    run_id: Optional[str] = None,
    **extra_parameters,
) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id or os.getenv("AIRFLOW_RUN_ID", "local"),
        "agent": agent,
        "model": model,
        "purpose": purpose,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
        "cost_usd": round(cost_usd, 6),
        "cached": cached,
        **extra_parameters,
    }

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOCK:
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")


def summarise(run_id: str | None = None) -> dict:
    if not LOG_PATH.exists():
        return {}

    # All records
    records = [json.loads(line) for line in LOG_PATH.read_text().splitlines() if line.strip()]

    # Apply RID filter if supplied
    if run_id:
        records = [r for r in records if r.get("run_id") == run_id]
    if not records:
        return {}

    return {
        "run_id": run_id or "all",
        "total_calls": len(records),
        "live_calls": sum(1 for r in records if not r["cached"]),
        "cached_calls": sum(1 for r in records if r["cached"]),
        "total_tokens": sum(r["total_tokens"] for r in records),
        "no_cache_cost_usd": round(sum(r["cost_usd"] for r in records), 4),
        "actual_cost_usd": round(sum(r["cost_usd"] for r in records if not r["cached"]), 4),
        "models_used": list({r["model"] for r in records}),
    }


if __name__ == "__main__":
    import sys

    rid = sys.argv[1] if len(sys.argv) > 1 else None
    print(json.dumps(summarise(rid), indent=2))
