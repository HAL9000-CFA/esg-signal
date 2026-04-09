"""
Entry point for all LLM calls.
Handles caching, audit logging, and cost tracking without intervention.

Reproducability:
    Set USE_CACHED=true to read from cache if possible.
    Reponses are cached on each live call. Identical payloads will overwrite.
"""

import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

try:
    import google.generativeai as genai

    _GEMINI_AVAILABLE = True
except Exception:  # protobuf C extension fails on Python 3.14 — deferred to runtime
    genai = None  # type: ignore[assignment]
    _GEMINI_AVAILABLE = False

from anthropic import Anthropic, AuthenticationError, PermissionDeniedError, RateLimitError
from dotenv import load_dotenv

from pipeline.audit_log import compute_cost, log_llm_call

load_dotenv()

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

CACHE_DIR = Path(os.getenv("CACHE_DIR", "data/cache/"))
USE_CACHED = os.getenv("USE_CACHED", "false") == "true"

# Exponential backoff delays (seconds) for Anthropic 429 rate limit retries.
# Up to 5 retries: 5s → 10s → 20s → 40s → 80s.
_RATE_LIMIT_BACKOFF = [5, 10, 20, 40, 80]

# Not true to API, updated as needed and add to /config/pricing.json
VALID_MODELS = {
    "claude": ["claude-opus-4-5"],
    "gemini": ["gemini-2.5-flash"],
}


def call_claude(
    *,
    agent: str,
    model: str = VALID_MODELS["claude"][0],
    version: str,
    purpose: str,
    system: str,
    prompt: str,
    temperature: float = 0.0,
    max_tokens: int = 2048,
    run_id: Optional[str] = None,
) -> str:
    """
    Call claude and return the response text.

    API Docs:
        https://platform.claude.com/docs/en/api/python/messages/create

    Args:
        agent:          Name of the calling agent
        model:          Which model to use
        version:        LLM version for logs
        purpose:        What the call is for
        system:         Sytem prompt
        prompt:         User message
        temperature:    Amount of randomness between 0.0 and 1.0 (inclusive).
        max_tokens:     Maximum tokens in reponse
        run_id:         Airflow run ID for audit log (grouping)
    Returns:
        Reponse text from Claude
    """

    # Validate model
    if model not in VALID_MODELS["claude"]:
        new_model = VALID_MODELS["claude"][0]
        LOGGER.warning(f"Model '{model}' not in valid list. Using '{new_model}' instead.")
        model = new_model

    # Validate temperature
    new_temperature = max(0.0, min(temperature, 1.0))

    if new_temperature != temperature:
        LOGGER.warning(
            f"Temperature '{temperature}' not between 0.0 and 1.0. Using '{new_temperature}' instead."
        )
        temperature = new_temperature

    # Create payload and cache key
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "system": system,
        "messages": [{"role": "user", "content": prompt}],
    }
    cache_key = _make_cache_key(payload)

    # Handle cache
    cache_file = CACHE_DIR / f"{cache_key}.json"
    _cache_valid = False
    if USE_CACHED and cache_file.exists():
        try:
            cached_call = json.loads(cache_file.read_text())
            _cached_content = cached_call.get("content", "")
            if _cached_content and _cached_content.strip():
                LOGGER.info(f"Using cached result for agent '{agent}', purpose '{purpose}'")
                input_tokens = cached_call["input_tokens"]
                output_tokens = cached_call["output_tokens"]
                cost_usd = cached_call["cost_usd"]
                cached = True
                content = _cached_content
                _cache_valid = True
            else:
                # Stale entry with empty content written before the empty-response guard.
                # Delete it and fall through to a live API call.
                LOGGER.warning(
                    "call_claude: stale empty cache entry deleted — will re-call API "
                    "(agent=%s purpose=%s file=%s)",
                    agent,
                    purpose,
                    cache_file.name,
                )
                cache_file.unlink(missing_ok=True)
        except Exception as exc:
            LOGGER.warning("call_claude: cache read failed (%s) — falling back to API", exc)

    if not _cache_valid:  # No valid cache — call the API
        client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        response = None
        for attempt, wait in enumerate(_RATE_LIMIT_BACKOFF, start=1):
            try:
                response = client.messages.create(**payload)
                # Treat an empty response as a transient failure and retry with backoff.
                # This covers the rare case where the API returns 200 with no text content
                # (observed after a 429 burst — the next request succeeds but body is empty).
                _text = response.content[0].text if response.content else ""
                if not _text or not _text.strip():
                    if attempt == len(_RATE_LIMIT_BACKOFF):
                        LOGGER.warning(
                            "call_claude: empty response — all %d retries exhausted (agent=%s)",
                            len(_RATE_LIMIT_BACKOFF),
                            agent,
                        )
                        break
                    LOGGER.warning(
                        "call_claude: empty response (attempt %d/%d) — waiting %ds before retry (agent=%s)",
                        attempt,
                        len(_RATE_LIMIT_BACKOFF),
                        wait,
                        agent,
                    )
                    time.sleep(wait)
                    continue
                break
            except (AuthenticationError, PermissionDeniedError) as exc:
                # Credit exhaustion or invalid key — retrying won't help, fail immediately
                LOGGER.error(
                    "call_claude: billing/auth error — check API key and credit balance: %s", exc
                )
                raise
            except RateLimitError:
                if attempt == len(_RATE_LIMIT_BACKOFF):
                    LOGGER.warning(
                        "call_claude: 429 rate limit — all %d retries exhausted, raising",
                        len(_RATE_LIMIT_BACKOFF),
                    )
                    raise
                LOGGER.warning(
                    "call_claude: 429 rate limit hit (attempt %d/%d) — waiting %ds before retry",
                    attempt,
                    len(_RATE_LIMIT_BACKOFF),
                    wait,
                )
                time.sleep(wait)

        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens
        cost_usd = compute_cost(model=model, input_tokens=input_tokens, output_tokens=output_tokens)
        cached = False
        content = response.content[0].text if response.content else ""

        try:
            if not content or not content.strip():
                LOGGER.warning(
                    "call_claude: empty response from API — not caching (agent=%s purpose=%s)",
                    agent,
                    purpose,
                )
                return content
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(
                json.dumps(
                    {
                        "content": content,
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "cost_usd": cost_usd,
                        "model": model,
                        "agent": agent,
                        "purpose": purpose,
                        "temperature": temperature,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        except OSError as exc:
            LOGGER.warning("call_claude: cache write failed (continuing without cache): %s", exc)

    # Log call and return content
    log_llm_call(
        agent=agent,
        model=model,
        version=version,
        purpose=purpose,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_usd=cost_usd,
        temperature=temperature,
        cached=cached,
        run_id=run_id,
    )

    return content


def call_gemini(
    *,
    agent: str,
    model: str = VALID_MODELS["gemini"][0],
    version: str,
    purpose: str,
    prompt: str,
    temperature: float = 0.0,
    max_tokens: int = 8192,
    run_id: Optional[str] = None,
) -> str:
    """
    Call Gemini and return the response text.

    Gemini 2.0 Flash has a 1M token context window, making it suitable for long PDF text.
    Uses same caching and audit logging as call_claude().

    Args:
        agent:      Name of the calling agent
        model:      Which Gemini model to use
        version:    LLM version for logs
        purpose:    What the call is for
        prompt:     Full prompt (system + user combined — Gemini uses single-turn)
        temperature: Amount of randomness between 0.0 and 1.0 (inclusive)
        max_tokens: Maximum tokens in response
        run_id:     Airflow run ID for audit log (grouping)
    Returns:
        Response text from Gemini
    """

    if not _GEMINI_AVAILABLE:
        raise RuntimeError(
            "google-generativeai is not available in this environment. "
            "Install it or use Python 3.11 (protobuf C extension incompatible with Python 3.14)."
        )

    # Validate model
    if model not in VALID_MODELS["gemini"]:
        new_model = VALID_MODELS["gemini"][0]
        LOGGER.warning(f"Model '{model}' not in valid list. Using '{new_model}' instead.")
        model = new_model

    # Validate temperature
    new_temperature = max(0.0, min(temperature, 1.0))
    if new_temperature != temperature:
        LOGGER.warning(
            f"Temperature '{temperature}' not between 0.0 and 1.0. Using '{new_temperature}' instead."
        )
        temperature = new_temperature

    # Create payload and cache key
    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "prompt": prompt,
    }
    cache_key = _make_cache_key(payload)

    # Handle cache
    cache_file = CACHE_DIR / f"{cache_key}.json"
    if USE_CACHED and cache_file.exists():
        LOGGER.info(f"Using cached result for agent '{agent}', purpose '{purpose}'")
        cached_call = json.loads(cache_file.read_text())
        input_tokens = cached_call["input_tokens"]
        output_tokens = cached_call["output_tokens"]
        cost_usd = cached_call["cost_usd"]
        cached = True
        content = cached_call["content"]
    else:
        genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
        gemini_model = genai.GenerativeModel(
            model_name=model,
            generation_config=genai.GenerationConfig(
                temperature=temperature,
                max_output_tokens=max_tokens,
            ),
        )
        response = gemini_model.generate_content(prompt)
        content = response.text

        # Gemini usage metadata (may be None for some response types)
        usage = getattr(response, "usage_metadata", None)
        input_tokens = getattr(usage, "prompt_token_count", 0) or 0
        output_tokens = getattr(usage, "candidates_token_count", 0) or 0
        cost_usd = compute_cost(model=model, input_tokens=input_tokens, output_tokens=output_tokens)
        cached = False

        try:
            if not content or not content.strip():
                LOGGER.warning(
                    "call_gemini: empty response from API — not caching (agent=%s purpose=%s)",
                    agent,
                    purpose,
                )
                return content
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(
                json.dumps(
                    {
                        "content": content,
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "cost_usd": cost_usd,
                        "model": model,
                        "agent": agent,
                        "purpose": purpose,
                        "temperature": temperature,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        except OSError as exc:
            LOGGER.warning("call_gemini: cache write failed (continuing without cache): %s", exc)

    # Log call and return content
    log_llm_call(
        agent=agent,
        model=model,
        version=version,
        purpose=purpose,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_usd=cost_usd,
        temperature=temperature,
        cached=cached,
        run_id=run_id,
    )

    return content


def _make_cache_key(payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()
