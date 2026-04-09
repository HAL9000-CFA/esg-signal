"""
Mocked unit tests for TalentSignal and its job fetchers.
No live HTTP calls, no filesystem side-effects.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipeline.fetchers.indeed_jobs import (
    _classify_seniority,
    _deduplicate,
    _parse_rss,
    fetch_indeed_jobs,
)
from pipeline.fetchers.serp_jobs import fetch_serp_jobs
from pipeline.models import JobPosting, TalentSignalResult
from pipeline.talent_signal import TalentSignal

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_posting(job_id="abc123", title="ESG Manager", seniority="senior", keywords=None):
    return JobPosting(
        job_id=job_id,
        title=title,
        date_posted="Mon, 01 Jan 2026 00:00:00 GMT",
        url="https://example.com/job",
        source="indeed",
        keywords_matched=keywords or ["sustainability"],
        seniority=seniority,
    )


_SAMPLE_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>Senior ESG Manager</title>
      <link>https://www.indeed.com/viewjob?jk=abc1</link>
      <pubDate>Mon, 01 Jan 2026 00:00:00 GMT</pubDate>
      <guid>https://www.indeed.com/viewjob?jk=abc1</guid>
    </item>
    <item>
      <title>Environmental Intern</title>
      <link>https://www.indeed.com/viewjob?jk=abc2</link>
      <pubDate>Tue, 02 Jan 2026 00:00:00 GMT</pubDate>
      <guid>https://www.indeed.com/viewjob?jk=abc2</guid>
    </item>
  </channel>
</rss>"""


# ---------------------------------------------------------------------------
# indeed_jobs — _classify_seniority
# ---------------------------------------------------------------------------


class TestClassifySeniority:
    @pytest.mark.parametrize(
        ("title", "expected"),
        [
            ("Senior ESG Manager", "senior"),
            ("Director of Sustainability", "senior"),
            ("VP Climate Risk", "senior"),
            ("Head of ESG", "senior"),
            ("ESG Lead", "senior"),
            ("Junior Sustainability Analyst", "junior"),
            ("Graduate Environmental Associate", "junior"),
            ("ESG Intern", "junior"),
            ("Sustainability Analyst", "mid"),
            ("Climate Data Scientist", "mid"),
        ],
    )
    def test_classify(self, title, expected):
        assert _classify_seniority(title) == expected


# ---------------------------------------------------------------------------
# indeed_jobs — _parse_rss
# ---------------------------------------------------------------------------


class TestParseRss:
    def test_returns_correct_count(self):
        postings = _parse_rss(_SAMPLE_RSS, "sustainability")
        assert len(postings) == 2

    def test_classifies_seniority(self):
        postings = _parse_rss(_SAMPLE_RSS, "sustainability")
        titles = {p.title: p.seniority for p in postings}
        assert titles["Senior ESG Manager"] == "senior"
        assert titles["Environmental Intern"] == "junior"

    def test_sets_source_and_keyword(self):
        postings = _parse_rss(_SAMPLE_RSS, "sustainability")
        assert all(p.source == "indeed" for p in postings)
        assert all("sustainability" in p.keywords_matched for p in postings)

    def test_returns_empty_on_malformed_xml(self):
        postings = _parse_rss("<not valid xml >>>", "sustainability")
        assert postings == []


# ---------------------------------------------------------------------------
# indeed_jobs — _deduplicate
# ---------------------------------------------------------------------------


class TestDeduplicate:
    def test_merges_duplicate_ids(self):
        p1 = _make_posting(job_id="x1", keywords=["sustainability"])
        p2 = _make_posting(job_id="x1", keywords=["climate"])
        result = _deduplicate([p1, p2])
        assert len(result) == 1
        assert set(result[0].keywords_matched) == {"sustainability", "climate"}

    def test_keeps_distinct_ids(self):
        p1 = _make_posting(job_id="x1")
        p2 = _make_posting(job_id="x2")
        result = _deduplicate([p1, p2])
        assert len(result) == 2


# ---------------------------------------------------------------------------
# fetch_indeed_jobs
# ---------------------------------------------------------------------------


class TestFetchIndeedJobs:
    def test_returns_postings_on_success(self):
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_RSS
        mock_resp.raise_for_status.return_value = None

        with patch("pipeline.fetchers.indeed_jobs.requests.get", return_value=mock_resp):
            result = fetch_indeed_jobs("Acme Corp", ["sustainability"])

        assert len(result) == 2

    def test_returns_empty_on_http_error(self):
        with patch(
            "pipeline.fetchers.indeed_jobs.requests.get",
            side_effect=Exception("timeout"),
        ):
            result = fetch_indeed_jobs("Acme Corp", ["sustainability"])

        assert result == []

    def test_deduplicates_across_keywords(self):
        mock_resp = MagicMock()
        mock_resp.text = _SAMPLE_RSS
        mock_resp.raise_for_status.return_value = None

        with patch("pipeline.fetchers.indeed_jobs.requests.get", return_value=mock_resp):
            result = fetch_indeed_jobs("Acme Corp", ["sustainability", "climate"])

        # Same RSS returned for both keywords — dedup should collapse duplicates
        ids = [p.job_id for p in result]
        assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# fetch_serp_jobs
# ---------------------------------------------------------------------------


class TestFetchSerpJobs:
    def test_returns_empty_when_no_key(self, monkeypatch):
        monkeypatch.delenv("SERPAPI_KEY", raising=False)
        result = fetch_serp_jobs("Acme Corp", ["sustainability"])
        assert result == []

    def test_returns_postings_on_success(self, monkeypatch):
        monkeypatch.setenv("SERPAPI_KEY", "test-key")
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "jobs_results": [
                {
                    "title": "Senior ESG Analyst",
                    "job_id": "serp001",
                    "share_link": "https://jobs.google.com/serp001",
                    "detected_extensions": {"posted_at": "3 days ago"},
                },
            ]
        }
        mock_resp.raise_for_status.return_value = None

        with patch("pipeline.fetchers.serp_jobs.requests.get", return_value=mock_resp):
            result = fetch_serp_jobs("Acme Corp", ["sustainability"])

        assert len(result) == 1
        assert result[0].seniority == "senior"
        assert result[0].source == "serp"

    def test_returns_empty_on_http_error(self, monkeypatch):
        monkeypatch.setenv("SERPAPI_KEY", "test-key")
        with patch(
            "pipeline.fetchers.serp_jobs.requests.get",
            side_effect=Exception("connection refused"),
        ):
            result = fetch_serp_jobs("Acme Corp", ["sustainability"])

        assert result == []

    def test_caps_keywords_at_three(self, monkeypatch):
        monkeypatch.setenv("SERPAPI_KEY", "test-key")
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"jobs_results": []}
        mock_resp.raise_for_status.return_value = None

        with patch("pipeline.fetchers.serp_jobs.requests.get", return_value=mock_resp) as mock_get:
            fetch_serp_jobs("Acme", ["a", "b", "c", "d", "e"])

        assert mock_get.call_count == 3


# ---------------------------------------------------------------------------
# TalentSignal.analyse
# ---------------------------------------------------------------------------


class TestTalentSignalAnalyse:
    def test_returns_talent_signal_result(self, tmp_path):
        postings = [
            _make_posting("j1", "Senior ESG Manager", "senior", ["sustainability"]),
            _make_posting("j2", "Climate Analyst", "mid", ["climate"]),
        ]
        ts = TalentSignal(ticker="TEST", ghost_store_dir=str(tmp_path))

        with (
            patch("pipeline.talent_signal.fetch_indeed_jobs", return_value=postings),
            patch("pipeline.talent_signal.fetch_serp_jobs", return_value=[]),
        ):
            result = ts.analyse("Test Corp")

        assert isinstance(result, TalentSignalResult)
        assert result.total_postings == 2
        assert result.company_name == "Test Corp"
        assert result.senior_ratio == 0.5
        assert "environment" in result.factor_scores

    def test_falls_back_to_serp_when_indeed_empty(self, tmp_path):
        serp_postings = [_make_posting("s1", "ESG Director", "senior", ["sustainability"])]
        ts = TalentSignal(ticker="TEST", ghost_store_dir=str(tmp_path))

        with (
            patch("pipeline.talent_signal.fetch_indeed_jobs", return_value=[]),
            patch("pipeline.talent_signal.fetch_serp_jobs", return_value=serp_postings),
        ):
            result = ts.analyse("Test Corp")

        assert result.total_postings == 1

    def test_records_error_when_no_postings(self, tmp_path):
        ts = TalentSignal(ticker="TEST", ghost_store_dir=str(tmp_path))

        with (
            patch("pipeline.talent_signal.fetch_indeed_jobs", return_value=[]),
            patch("pipeline.talent_signal.fetch_serp_jobs", return_value=[]),
        ):
            result = ts.analyse("Test Corp")

        assert result.total_postings == 0
        assert len(result.errors) > 0

    def test_factor_scores_bounded_zero_to_one(self, tmp_path):
        postings = [
            _make_posting(f"j{i}", "Senior ESG Manager", "senior", ["sustainability"])
            for i in range(10)
        ]
        ts = TalentSignal(ticker="TEST", ghost_store_dir=str(tmp_path))

        with (
            patch("pipeline.talent_signal.fetch_indeed_jobs", return_value=postings),
            patch("pipeline.talent_signal.fetch_serp_jobs", return_value=[]),
        ):
            result = ts.analyse("Test Corp")

        for score in result.factor_scores.values():
            assert 0.0 <= score <= 1.0


# ---------------------------------------------------------------------------
# TalentSignal — ghost detection
# ---------------------------------------------------------------------------


class TestGhostDetection:
    def test_no_ghosts_on_first_run(self, tmp_path):
        postings = [_make_posting("j1")]
        ts = TalentSignal(ticker="TEST", ghost_store_dir=str(tmp_path))
        _, ghost_count = ts._detect_ghosts(postings)
        assert ghost_count == 0

    def test_detects_ghost_after_threshold(self, tmp_path):
        ts = TalentSignal(ticker="TEST", ghost_store_dir=str(tmp_path))

        # Write a ghost store entry that is 31 days old
        old_date = (datetime.now(timezone.utc) - timedelta(days=31)).isoformat()
        store = {"old_job": {"first_seen": old_date, "last_seen": old_date, "ghost": False}}
        ts._save_ghost_store(store)

        # Current postings do NOT include old_job
        postings = [_make_posting("new_job")]
        _, ghost_count = ts._detect_ghosts(postings)
        assert ghost_count == 1

    def test_no_ghost_if_posting_still_present(self, tmp_path):
        ts = TalentSignal(ticker="TEST", ghost_store_dir=str(tmp_path))

        old_date = (datetime.now(timezone.utc) - timedelta(days=31)).isoformat()
        store = {"j1": {"first_seen": old_date, "last_seen": old_date, "ghost": False}}
        ts._save_ghost_store(store)

        # Same posting still present in results — not a ghost
        postings = [_make_posting("j1")]
        _, ghost_count = ts._detect_ghosts(postings)
        assert ghost_count == 0

    def test_ghost_store_persisted(self, tmp_path):
        ts = TalentSignal(ticker="TEST", ghost_store_dir=str(tmp_path))
        postings = [_make_posting("j1"), _make_posting("j2")]
        ts._detect_ghosts(postings)

        store = ts._load_ghost_store()
        assert "j1" in store
        assert "j2" in store
