"""
PDF text extraction for annual reports.

Extraction strategy (in order of preference):
  1. pdfplumber  — fast, accurate for text-layer PDFs (most modern filings)
  2. Gemini 2.0 Flash — fallback for scanned/image PDFs via 1M-token context window

LayoutParser is listed in requirements as a commented-out heavy dep.
When reinstated it can slot in between pdfplumber and Gemini as an OCR step.
"""

import hashlib
import io
import json
import logging
import os
from pathlib import Path
from typing import Optional

import pdfplumber
import requests

from pipeline.audit_log import compute_cost, log_llm_call

LOGGER = logging.getLogger(__name__)

_GEMINI_PDF_MODEL = "gemini-2.5-flash"
_CACHE_DIR = Path(os.getenv("CACHE_DIR", "data/cache/"))
_USE_CACHED = os.getenv("USE_CACHED", "false") == "true"

# Minimum character count to accept pdfplumber output as usable.
# Below this threshold the PDF is likely scanned; fall back to Gemini.
_MIN_TEXT_CHARS = 500


class PDFExtractor:
    """
    Downloads a PDF from a URL and extracts its full text.

    Usage:
        extractor = PDFExtractor()
        text = extractor.extract(url="https://...")
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout

    def extract_from_bytes(
        self,
        pdf_bytes: bytes,
        url: str = "",
        agent: str = "pdf_extractor",
        run_id: Optional[str] = None,
    ) -> str:
        """
        Extract text from already-downloaded PDF bytes.

        Used when the caller must supply auth headers (e.g. Companies House Document API)
        and cannot pass a bare URL to extract().
        """
        if not pdf_bytes:
            return ""
        text = self._extract_with_pdfplumber(pdf_bytes)
        if len(text) >= _MIN_TEXT_CHARS:
            LOGGER.info("pdfplumber extracted %d chars (from bytes, url=%s)", len(text), url)
            return text
        LOGGER.info(
            "pdfplumber returned only %d chars — falling back to Gemini (from bytes, url=%s)",
            len(text),
            url,
        )
        return self._extract_with_gemini(
            pdf_bytes, url=url or "bytes://ch-document", agent=agent, run_id=run_id
        )

    def extract(self, url: str, agent: str = "pdf_extractor", run_id: Optional[str] = None) -> str:
        """
        Download PDF from url and return extracted text.

        Tries pdfplumber first; if output is too short (scanned PDF),
        falls back to Gemini 2.0 Flash with the raw PDF bytes as context.

        Args:
            url:     Direct URL to the PDF file
            agent:   Caller name for audit log
            run_id:  Airflow run ID (optional)

        Returns:
            Extracted text. Empty string if extraction fails entirely.
        """
        pdf_bytes = self._download(url)
        if not pdf_bytes:
            return ""

        text = self._extract_with_pdfplumber(pdf_bytes)

        if len(text) >= _MIN_TEXT_CHARS:
            LOGGER.info(f"pdfplumber extracted {len(text):,} chars from {url}")
            return text

        LOGGER.info(
            f"pdfplumber returned only {len(text)} chars — falling back to Gemini for {url}"
        )
        return self._extract_with_gemini(pdf_bytes, url=url, agent=agent, run_id=run_id)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _download(self, url: str) -> Optional[bytes]:
        LOGGER.info("PDFExtractor: downloading from %s (timeout=%ds)", url, self.timeout)
        try:
            resp = requests.get(url, timeout=self.timeout)
            resp.raise_for_status()
            LOGGER.info("PDFExtractor: downloaded %d bytes", len(resp.content))
            return resp.content
        except requests.exceptions.Timeout:
            LOGGER.warning("PDFExtractor: download timed out (%ds) for %s", self.timeout, url)
            return None
        except Exception as exc:
            LOGGER.warning("PDFExtractor: download failed for %s: %s", url, exc)
            return None

    def _extract_with_pdfplumber(self, pdf_bytes: bytes) -> str:
        try:
            pages = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        pages.append(page_text)
            return "\n\n".join(pages)
        except Exception as exc:
            LOGGER.warning(f"pdfplumber extraction failed: {exc}")
            return ""

    def _extract_with_gemini(
        self,
        pdf_bytes: bytes,
        url: str,
        agent: str,
        run_id: Optional[str],
    ) -> str:
        """
        Send raw PDF bytes to Gemini 2.5 Flash as an inline file part.
        Gemini's 1M token context window can accommodate most annual reports.

        Results are cached by URL so repeated runs use the local cache.
        All calls are logged to the audit log (same as call_claude / call_gemini).
        """
        # Cache key based on URL — avoids re-downloading and re-processing same PDF
        cache_key = hashlib.sha256(f"gemini_pdf::{_GEMINI_PDF_MODEL}::{url}".encode()).hexdigest()
        cache_file = _CACHE_DIR / f"{cache_key}.json"
        use_cached = _USE_CACHED or os.getenv("USE_CACHED", "false") == "true"

        if use_cached and cache_file.exists():
            try:
                cached = json.loads(cache_file.read_text())
                LOGGER.info("PDFExtractor: using cached Gemini result for %s", url)
                log_llm_call(
                    agent=agent,
                    model=_GEMINI_PDF_MODEL,
                    version=_GEMINI_PDF_MODEL,
                    purpose=f"PDF extraction (cached): {url[:80]}",
                    input_tokens=cached.get("input_tokens", 0),
                    output_tokens=cached.get("output_tokens", 0),
                    cost_usd=cached.get("cost_usd", 0.0),
                    cached=True,
                    run_id=run_id,
                )
                return cached.get("content", "")
            except Exception as exc:
                LOGGER.warning("PDFExtractor: cache read failed for %s: %s", url, exc)

        try:
            import base64

            import google.generativeai as genai

            encoded = base64.b64encode(pdf_bytes).decode("utf-8")
            prompt_parts = [
                {
                    "inline_data": {
                        "mime_type": "application/pdf",
                        "data": encoded,
                    }
                },
                "Extract and return the full text of this PDF document. "
                "Preserve the logical reading order. Do not summarise — return all text.",
            ]

            genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
            model = genai.GenerativeModel(_GEMINI_PDF_MODEL)
            response = model.generate_content(prompt_parts)
            content = response.text

            usage = getattr(response, "usage_metadata", None)
            input_tokens = getattr(usage, "prompt_token_count", 0) or 0
            output_tokens = getattr(usage, "candidates_token_count", 0) or 0
            cost_usd = compute_cost(
                model=_GEMINI_PDF_MODEL,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            )

            LOGGER.info("PDFExtractor: Gemini extracted %d chars from %s", len(content), url)

            # Persist to cache
            _CACHE_DIR.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(
                json.dumps(
                    {
                        "content": content,
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "cost_usd": cost_usd,
                        "model": _GEMINI_PDF_MODEL,
                        "url": url,
                    },
                    indent=2,
                )
            )

            log_llm_call(
                agent=agent,
                model=_GEMINI_PDF_MODEL,
                version=_GEMINI_PDF_MODEL,
                purpose=f"PDF extraction: {url[:80]}",
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cost_usd=cost_usd,
                cached=False,
                run_id=run_id,
            )
            return content

        except Exception as exc:
            LOGGER.warning("PDFExtractor: Gemini extraction failed for %s: %s", url, exc)
            return ""
