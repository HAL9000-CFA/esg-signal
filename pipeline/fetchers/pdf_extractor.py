"""
PDF text extraction for annual reports.

Extraction strategy (in order of preference):
  1. pdfplumber  — fast, accurate for text-layer PDFs (most modern filings)
  2. Gemini 2.0 Flash — fallback for scanned/image PDFs via 1M-token context window

LayoutParser is listed in requirements as a commented-out heavy dep.
When reinstated it can slot in between pdfplumber and Gemini as an OCR step.
"""

import io
import logging
from typing import Optional

import pdfplumber
import requests

LOGGER = logging.getLogger(__name__)

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
        Send raw PDF bytes to Gemini 2.0 Flash as an inline file part.
        Gemini's 1M token context window can accommodate most annual reports.
        """
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

            # call_gemini only handles plain string prompts (single-turn text).
            # For PDF bytes we call the Gemini SDK directly with the multimodal parts.
            import os

            genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
            model = genai.GenerativeModel("gemini-2.5-flash")
            response = model.generate_content(prompt_parts)
            content = response.text
            LOGGER.info(f"Gemini extracted {len(content):,} chars from {url}")
            return content
        except Exception as exc:
            LOGGER.warning(f"Gemini PDF extraction failed for {url}: {exc}")
            return ""
