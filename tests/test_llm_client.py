"""
Mocked unit tests for call_gemini() in pipeline/llm_client.py.
No live API calls.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

import pipeline.llm_client as llm_client

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_use_cached(monkeypatch):
    """Ensure USE_CACHED is false and Gemini availability is True for all tests."""
    monkeypatch.setattr(llm_client, "USE_CACHED", False)
    monkeypatch.setattr(llm_client, "_GEMINI_AVAILABLE", True)


# ---------------------------------------------------------------------------
# call_gemini — live path (no cache)
# ---------------------------------------------------------------------------


class TestCallGeminiLive:
    def test_returns_response_text(self, tmp_path, monkeypatch):
        monkeypatch.setattr(llm_client, "CACHE_DIR", tmp_path)

        mock_usage = MagicMock()
        mock_usage.prompt_token_count = 100
        mock_usage.candidates_token_count = 200

        mock_response = MagicMock()
        mock_response.text = "Extracted ESG text from annual report."
        mock_response.usage_metadata = mock_usage

        mock_model = MagicMock()
        mock_model.generate_content.return_value = mock_response

        mock_genai = MagicMock()
        mock_genai.GenerativeModel.return_value = mock_model
        mock_genai.GenerationConfig = MagicMock(return_value=MagicMock())

        with (
            patch("pipeline.llm_client.genai", mock_genai),
            patch("pipeline.llm_client.log_llm_call"),
        ):
            result = llm_client.call_gemini(
                agent="test_agent",
                version="1.0",
                purpose="unit test",
                prompt="Summarise this document.",
            )

        assert result == "Extracted ESG text from annual report."

    def test_writes_cache_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(llm_client, "CACHE_DIR", tmp_path)

        mock_usage = MagicMock()
        mock_usage.prompt_token_count = 50
        mock_usage.candidates_token_count = 100

        mock_response = MagicMock()
        mock_response.text = "Cached content."
        mock_response.usage_metadata = mock_usage

        mock_model = MagicMock()
        mock_model.generate_content.return_value = mock_response

        mock_genai = MagicMock()
        mock_genai.GenerativeModel.return_value = mock_model
        mock_genai.GenerationConfig = MagicMock(return_value=MagicMock())

        with (
            patch("pipeline.llm_client.genai", mock_genai),
            patch("pipeline.llm_client.log_llm_call"),
        ):
            llm_client.call_gemini(
                agent="test_agent",
                version="1.0",
                purpose="unit test",
                prompt="Extract text.",
            )

        cache_files = list(tmp_path.glob("*.json"))
        assert len(cache_files) == 1
        cached = json.loads(cache_files[0].read_text())
        assert cached["content"] == "Cached content."
        assert cached["model"] == "gemini-2.0-flash"

    def test_logs_llm_call(self, tmp_path, monkeypatch):
        monkeypatch.setattr(llm_client, "CACHE_DIR", tmp_path)

        mock_usage = MagicMock()
        mock_usage.prompt_token_count = 10
        mock_usage.candidates_token_count = 20

        mock_response = MagicMock()
        mock_response.text = "Some text."
        mock_response.usage_metadata = mock_usage

        mock_model = MagicMock()
        mock_model.generate_content.return_value = mock_response

        mock_genai = MagicMock()
        mock_genai.GenerativeModel.return_value = mock_model
        mock_genai.GenerationConfig = MagicMock(return_value=MagicMock())

        with (
            patch("pipeline.llm_client.genai", mock_genai),
            patch("pipeline.llm_client.log_llm_call") as mock_log,
        ):
            llm_client.call_gemini(
                agent="scorer",
                version="2.0",
                purpose="extraction",
                prompt="Extract.",
            )

        mock_log.assert_called_once()
        call_kwargs = mock_log.call_args.kwargs
        assert call_kwargs["agent"] == "scorer"
        assert call_kwargs["model"] == "gemini-2.0-flash"
        assert call_kwargs["cached"] is False


# ---------------------------------------------------------------------------
# call_gemini — cached path
# ---------------------------------------------------------------------------


class TestCallGeminiCached:
    def test_reads_from_cache_when_use_cached_true(self, tmp_path, monkeypatch):
        monkeypatch.setattr(llm_client, "CACHE_DIR", tmp_path)
        monkeypatch.setattr(llm_client, "USE_CACHED", True)

        # Pre-write a cache entry
        payload = {
            "model": "gemini-2.0-flash",
            "max_tokens": 8192,
            "temperature": 0.0,
            "prompt": "Cached prompt.",
        }
        cache_key = llm_client._make_cache_key(payload)
        cache_data = {
            "content": "Cached response text.",
            "input_tokens": 30,
            "output_tokens": 60,
            "cost_usd": 0.0004,
            "model": "gemini-2.0-flash",
            "agent": "test_agent",
            "purpose": "cached test",
            "temperature": 0.0,
        }
        (tmp_path / f"{cache_key}.json").write_text(json.dumps(cache_data))

        mock_genai = MagicMock()
        with (
            patch("pipeline.llm_client.genai", mock_genai),
            patch("pipeline.llm_client.log_llm_call") as mock_log,
        ):
            result = llm_client.call_gemini(
                agent="test_agent",
                version="1.0",
                purpose="cached test",
                prompt="Cached prompt.",
            )

        # Should not call the real API
        mock_genai.GenerativeModel.assert_not_called()
        assert result == "Cached response text."
        assert mock_log.call_args.kwargs["cached"] is True


# ---------------------------------------------------------------------------
# call_gemini — model validation
# ---------------------------------------------------------------------------


class TestCallGeminiModelValidation:
    def test_invalid_model_falls_back_to_default(self, tmp_path, monkeypatch):
        monkeypatch.setattr(llm_client, "CACHE_DIR", tmp_path)

        mock_usage = MagicMock()
        mock_usage.prompt_token_count = 5
        mock_usage.candidates_token_count = 10

        mock_response = MagicMock()
        mock_response.text = "ok"
        mock_response.usage_metadata = mock_usage

        mock_model_instance = MagicMock()
        mock_model_instance.generate_content.return_value = mock_response

        mock_genai = MagicMock()
        mock_genai.GenerativeModel.return_value = mock_model_instance
        mock_genai.GenerationConfig = MagicMock(return_value=MagicMock())

        with (
            patch("pipeline.llm_client.genai", mock_genai),
            patch("pipeline.llm_client.log_llm_call") as mock_log,
        ):
            llm_client.call_gemini(
                agent="test",
                version="1.0",
                purpose="test",
                prompt="hello",
                model="gemini-unknown-model",
            )

        used_model = mock_log.call_args.kwargs["model"]
        assert used_model == "gemini-2.0-flash"
