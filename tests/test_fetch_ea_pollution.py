"""
Unit tests for pipeline/fetchers/ea_pollution.py.

EAPollutionFetcher reads XLSX files via openpyxl, with a 9-row preamble
before the header row and OPERATOR NAME at column index 2.
Tests mock _download_xlsx and create a properly-structured XLSX using openpyxl.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from pipeline.fetchers.ea_pollution import EAPollutionFetcher

# ---------------------------------------------------------------------------
# Fixture: minimal EA Pollution XLSX with correct sheet structure
# ---------------------------------------------------------------------------


def _write_ea_xlsx(path, rows: list):
    """
    Write a minimal EA Pollution XLSX at `path`.

    Sheet: '2022 Substances'
    Structure:
      Rows 0–8: empty preamble
      Row  9:   header (OPERATOR NAME at column index 2)
      Rows 10+: data
    """
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "2022 Substances"

    # Rows 0–8: preamble (empty)
    for _ in range(9):
        ws.append([None])

    # Row 9: headers — OPERATOR NAME must be at column index 2 (1-indexed col 3)
    ws.append(
        [
            None,
            None,
            "OPERATOR NAME",
            "SITE NAME",
            "TOWN",
            "COUNTY REGION",
            "SUBSTANCE NAME",
            "MEDIUM",
            "QUANTITY RELEASED (kg)",
            "REPORTING THRESHOLD (kg)",
            "YEAR",
            "NACE CODE",
            "NACE DESCRIPTION",
        ]
    )

    # Data rows
    for row in rows:
        ws.append(row)

    wb.save(str(path))
    return path


@pytest.fixture
def ea_xlsx(tmp_path):
    """XLSX with BP and Shell records in the Substances sheet."""
    rows = [
        # col0, col1, OPERATOR NAME, SITE NAME, TOWN, COUNTY, SUBSTANCE NAME, MEDIUM, QTY, THRESHOLD, YEAR, NACE, DESC
        [
            None,
            None,
            "BP PLC",
            "Grangemouth Refinery",
            "Grangemouth",
            "Scotland",
            "Carbon dioxide",
            "Air",
            1_250_000.0,
            100_000.0,
            2022,
            "19.20",
            "Petroleum",
        ],
        [
            None,
            None,
            "BP PLC",
            "Grangemouth Refinery",
            "Grangemouth",
            "Scotland",
            "NOx",
            "Air",
            850.0,
            100.0,
            2022,
            "19.20",
            "Petroleum",
        ],
        [
            None,
            None,
            "BP PLC",
            "Grangemouth Refinery",
            "Grangemouth",
            "Scotland",
            "Carbon dioxide",
            "Air",
            1_320_000.0,
            100_000.0,
            2021,
            "19.20",
            "Petroleum",
        ],
        [
            None,
            None,
            "Shell UK Ltd",
            "Stanlow Refinery",
            "Ellesmere Port",
            "Cheshire",
            "Carbon dioxide",
            "Air",
            980_000.0,
            100_000.0,
            2022,
            "19.20",
            "Petroleum",
        ],
    ]
    return _write_ea_xlsx(tmp_path / "2022.xlsx", rows)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_fetch_returns_dataframe(ea_xlsx):
    with patch("pipeline.fetchers.ea_pollution._download_xlsx", return_value=ea_xlsx):
        df = EAPollutionFetcher().fetch("BP PLC", year=2022)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3  # 3 BP rows (Shell excluded)
    assert "operator_name" in df.columns
    assert "substance_or_waste" in df.columns  # normalised from SUBSTANCE NAME
    assert "quantity" in df.columns  # normalised from QUANTITY RELEASED (kg)
    assert "year" in df.columns


def test_fetch_filters_by_year(ea_xlsx):
    with patch("pipeline.fetchers.ea_pollution._download_xlsx", return_value=ea_xlsx):
        df = EAPollutionFetcher().fetch("BP PLC", year=2022)

    assert len(df) == 3
    assert all(df["year"] == 2022)


def test_fetch_returns_empty_when_company_not_found(ea_xlsx):
    with patch("pipeline.fetchers.ea_pollution._download_xlsx", return_value=ea_xlsx):
        df = EAPollutionFetcher().fetch("UNKNOWN COMPANY XYZ", year=2022)

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_fetch_returns_empty_when_no_xlsx():
    with patch("pipeline.fetchers.ea_pollution._download_xlsx", return_value=None):
        df = EAPollutionFetcher().fetch("BP PLC", year=2022)

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_fetch_returns_empty_on_corrupt_xlsx(tmp_path):
    bad = tmp_path / "bad.xlsx"
    bad.write_bytes(b"not a valid xlsx")

    with patch("pipeline.fetchers.ea_pollution._download_xlsx", return_value=bad):
        df = EAPollutionFetcher().fetch("BP PLC", year=2022)

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_operator_name_normalised(ea_xlsx):
    with patch("pipeline.fetchers.ea_pollution._download_xlsx", return_value=ea_xlsx):
        df = EAPollutionFetcher().fetch("BP PLC", year=2022)

    assert all(df["operator_name"].str.contains("BP", case=False))


def test_save_writes_files(tmp_path):
    df = pd.DataFrame(
        {
            "operator_name": ["BP PLC"],
            "substance_or_waste": ["Carbon dioxide"],
            "quantity": [1_250_000.0],
            "year": [2022],
        }
    )
    raw = str(tmp_path / "raw" / "ea_pollution_BP.csv")
    processed = str(tmp_path / "processed" / "ea_pollution_BP.csv")

    EAPollutionFetcher().save(df, raw_path=raw, processed_path=processed)

    assert pd.read_csv(raw).shape == df.shape
    assert pd.read_csv(processed).shape == df.shape
