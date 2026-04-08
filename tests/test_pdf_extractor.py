"""
Mocked unit tests for PDFExtractor.
No live HTTP calls, no live LLM calls.
"""

from unittest.mock import MagicMock, patch

from pipeline.fetchers.pdf_extractor import _MIN_TEXT_CHARS, PDFExtractor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pdf_bytes_mock(text: str) -> bytes:
    """Return dummy bytes — pdfplumber is mocked so content doesn't matter."""
    return b"%PDF fake content"


# ---------------------------------------------------------------------------
# _download
# ---------------------------------------------------------------------------


class TestDownload:
    def test_returns_bytes_on_success(self):
        extractor = PDFExtractor()
        mock_resp = MagicMock()
        mock_resp.content = b"%PDF-1.4 fake"
        mock_resp.raise_for_status.return_value = None

        with patch("pipeline.fetchers.pdf_extractor.requests.get", return_value=mock_resp):
            result = extractor._download("https://example.com/report.pdf")

        assert result == b"%PDF-1.4 fake"

    def test_returns_none_on_http_error(self):
        extractor = PDFExtractor()
        with patch(
            "pipeline.fetchers.pdf_extractor.requests.get",
            side_effect=Exception("connection refused"),
        ):
            result = extractor._download("https://example.com/report.pdf")

        assert result is None


# ---------------------------------------------------------------------------
# _extract_with_pdfplumber
# ---------------------------------------------------------------------------


class TestExtractWithPdfplumber:
    def test_returns_text_from_pages(self):
        extractor = PDFExtractor()

        mock_page_1 = MagicMock()
        mock_page_1.extract_text.return_value = "Page one text."
        mock_page_2 = MagicMock()
        mock_page_2.extract_text.return_value = "Page two text."

        mock_pdf = MagicMock()
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdf.pages = [mock_page_1, mock_page_2]

        with patch("pipeline.fetchers.pdf_extractor.pdfplumber.open", return_value=mock_pdf):
            result = extractor._extract_with_pdfplumber(b"fake pdf bytes")

        assert "Page one text." in result
        assert "Page two text." in result

    def test_skips_none_pages(self):
        extractor = PDFExtractor()

        mock_page = MagicMock()
        mock_page.extract_text.return_value = None

        mock_pdf = MagicMock()
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdf.pages = [mock_page]

        with patch("pipeline.fetchers.pdf_extractor.pdfplumber.open", return_value=mock_pdf):
            result = extractor._extract_with_pdfplumber(b"fake pdf bytes")

        assert result == ""

    def test_returns_empty_string_on_exception(self):
        extractor = PDFExtractor()
        with patch(
            "pipeline.fetchers.pdf_extractor.pdfplumber.open",
            side_effect=Exception("corrupt pdf"),
        ):
            result = extractor._extract_with_pdfplumber(b"bad bytes")

        assert result == ""


# ---------------------------------------------------------------------------
# extract — integration of download + pdfplumber + fallback
# ---------------------------------------------------------------------------


class TestExtract:
    def test_uses_pdfplumber_when_text_is_long_enough(self):
        extractor = PDFExtractor()
        long_text = "A" * (_MIN_TEXT_CHARS + 100)

        mock_resp = MagicMock()
        mock_resp.content = b"%PDF fake"
        mock_resp.raise_for_status.return_value = None

        mock_page = MagicMock()
        mock_page.extract_text.return_value = long_text
        mock_pdf = MagicMock()
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdf.pages = [mock_page]

        with (
            patch("pipeline.fetchers.pdf_extractor.requests.get", return_value=mock_resp),
            patch("pipeline.fetchers.pdf_extractor.pdfplumber.open", return_value=mock_pdf),
        ):
            result = extractor.extract("https://example.com/report.pdf")

        assert result == long_text

    def test_falls_back_to_gemini_when_pdfplumber_too_short(self):
        extractor = PDFExtractor()
        short_text = "tiny"
        gemini_text = "Full document text from Gemini." * 50

        mock_resp = MagicMock()
        mock_resp.content = b"%PDF fake"
        mock_resp.raise_for_status.return_value = None

        mock_page = MagicMock()
        mock_page.extract_text.return_value = short_text
        mock_pdf = MagicMock()
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdf.pages = [mock_page]

        with (
            patch("pipeline.fetchers.pdf_extractor.requests.get", return_value=mock_resp),
            patch("pipeline.fetchers.pdf_extractor.pdfplumber.open", return_value=mock_pdf),
            patch.object(extractor, "_extract_with_gemini", return_value=gemini_text),
        ):
            result = extractor.extract("https://example.com/scanned.pdf")

        assert result == gemini_text

    def test_returns_empty_string_when_download_fails(self):
        extractor = PDFExtractor()
        with patch(
            "pipeline.fetchers.pdf_extractor.requests.get", side_effect=Exception("timeout")
        ):
            result = extractor.extract("https://example.com/report.pdf")

        assert result == ""

    def test_returns_empty_string_when_all_extraction_fails(self):
        extractor = PDFExtractor()

        mock_resp = MagicMock()
        mock_resp.content = b"%PDF fake"
        mock_resp.raise_for_status.return_value = None

        with (
            patch("pipeline.fetchers.pdf_extractor.requests.get", return_value=mock_resp),
            patch(
                "pipeline.fetchers.pdf_extractor.pdfplumber.open", side_effect=Exception("corrupt")
            ),
            patch.object(extractor, "_extract_with_gemini", return_value=""),
        ):
            result = extractor.extract("https://example.com/report.pdf")

        assert result == ""
