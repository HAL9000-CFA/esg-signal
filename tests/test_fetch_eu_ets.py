"""
Unit tests for pipeline/fetchers/eu_ets.py.

The EUETSFetcher reads local XLSX files — there is no HTTP download.
Tests mock _find_xlsx to return a temporary XLSX created with pandas.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from pipeline.fetchers.eu_ets import EUETSFetcher

# ---------------------------------------------------------------------------
# Fixture: minimal EUTL-format XLSX
# ---------------------------------------------------------------------------

_COLUMNS = [
    "IDENTIFIER_IN_REG",
    "INSTALLATION_NAME",
    "REGISTRY_CODE",
    "MAIN_ACTIVITY_TYPE_CODE",
    "VERIFIED_EMISSIONS_2022",
    "ALLOCATION_2022",
    "VERIFIED_EMISSIONS_2021",
    "ALLOCATION_2021",
]

_DATA = [
    ["BP PLC", "Grangemouth CHP", "GB", "Energy", 850_000, 720_000, 920_000, 750_000],
    ["BP PLC", "Rotterdam Refinery", "NL", "Refining", 1_100_000, 900_000, 980_000, 820_000],
    ["BP PLC", "Lingen Refinery", "DE", "Refining", -1, 0, -1, 0],  # -1 = blank in EUTL
    ["Shell PLC", "Pernis Refinery", "NL", "Refining", 1_400_000, 1_100_000, 1_300_000, 1_000_000],
]


@pytest.fixture
def eu_ets_xlsx(tmp_path):
    """Create a minimal EUTL-format XLSX and return its path."""
    df = pd.DataFrame(_DATA, columns=_COLUMNS)
    path = tmp_path / "verified_emissions_2022_en.xlsx"
    df.to_excel(path, index=False)
    return path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_fetch_returns_dataframe(eu_ets_xlsx):
    with patch("pipeline.fetchers.eu_ets._find_xlsx", return_value=eu_ets_xlsx):
        df = EUETSFetcher().fetch("BP PLC", year=2022)

    assert isinstance(df, pd.DataFrame)
    # 3 BP rows (third row has -1 emissions → NaN, still included but emissions NaN)
    assert len(df) == 3
    assert "account_holder" in df.columns
    assert "verified_emissions_tco2e" in df.columns
    assert "year" in df.columns
    assert "country" in df.columns


def test_fetch_filters_by_company_name(eu_ets_xlsx):
    with patch("pipeline.fetchers.eu_ets._find_xlsx", return_value=eu_ets_xlsx):
        df = EUETSFetcher().fetch("BP PLC", year=2022)

    assert all(df["account_holder"].str.contains("BP", case=False))


def test_fetch_filters_by_year(eu_ets_xlsx):
    with patch("pipeline.fetchers.eu_ets._find_xlsx", return_value=eu_ets_xlsx):
        df = EUETSFetcher().fetch("BP PLC", year=2022)

    assert len(df) == 3
    assert all(df["year"] == 2022)


def test_fetch_returns_empty_when_company_not_found(eu_ets_xlsx):
    with patch("pipeline.fetchers.eu_ets._find_xlsx", return_value=eu_ets_xlsx):
        df = EUETSFetcher().fetch("UNKNOWN COMPANY XYZ", year=2022)

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_fetch_returns_empty_on_missing_xlsx():
    with patch("pipeline.fetchers.eu_ets._find_xlsx", return_value=None):
        df = EUETSFetcher().fetch("BP PLC", year=2022)

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_fetch_returns_empty_on_unreadable_xlsx(tmp_path):
    bad_path = tmp_path / "bad.xlsx"
    bad_path.write_bytes(b"not an xlsx file")

    with patch("pipeline.fetchers.eu_ets._find_xlsx", return_value=bad_path):
        df = EUETSFetcher().fetch("BP PLC", year=2022)

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_negative_emissions_become_nan(eu_ets_xlsx):
    with patch("pipeline.fetchers.eu_ets._find_xlsx", return_value=eu_ets_xlsx):
        df = EUETSFetcher().fetch("BP PLC", year=2022)

    # Lingen row has -1 emissions → should be NaN
    lingen = df[df["installation_name"].str.contains("Lingen", na=False)]
    assert lingen["verified_emissions_tco2e"].isna().all()


def test_save_writes_files(tmp_path):
    df = pd.DataFrame(
        {
            "account_holder": ["BP PLC"],
            "installation_name": ["Grangemouth CHP"],
            "country": ["GB"],
            "year": [2022],
            "verified_emissions_tco2e": [850_000],
        }
    )
    raw = str(tmp_path / "raw" / "eu_ets_BP.csv")
    processed = str(tmp_path / "processed" / "eu_ets_BP.csv")

    EUETSFetcher().save(df, raw_path=raw, processed_path=processed)

    assert pd.read_csv(raw).shape == df.shape
    assert pd.read_csv(processed).shape == df.shape
