from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.fetchers.ghgrp import GHGRPFetcher

CSV_RESPONSE = """FACILITY_NAME,PARENT_CO_NAME,REPORTING_YEAR,GHG_QUANTITY,FACILITY_ID,STATE
Apple Cupertino HQ,APPLE INC,2022,32000.0,1001,CA
Apple Austin Campus,APPLE INC,2022,18500.0,1002,TX
Apple Cupertino HQ,APPLE INC,2021,35000.0,1001,CA
"""


@patch("pipeline.fetchers.ghgrp.requests.get")
def test_fetch_returns_dataframe(mock_get):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.text = CSV_RESPONSE
    mock_get.return_value = resp

    df = GHGRPFetcher().fetch("APPLE INC")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert "ghg_quantity_mtco2e" in df.columns
    assert "facility_name" in df.columns
    assert "year" in df.columns


@patch("pipeline.fetchers.ghgrp.requests.get")
def test_fetch_filters_by_year(mock_get):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.text = CSV_RESPONSE
    mock_get.return_value = resp

    df = GHGRPFetcher().fetch("APPLE INC", year=2022)

    assert len(df) == 2
    assert all(df["year"] == 2022)


@patch("pipeline.fetchers.ghgrp.requests.get")
def test_fetch_returns_empty_on_no_data(mock_get):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.text = ""
    mock_get.return_value = resp

    df = GHGRPFetcher().fetch("UNKNOWN CORP")

    assert isinstance(df, pd.DataFrame)
    assert df.empty


@patch("pipeline.fetchers.ghgrp.requests.get")
def test_fetch_returns_empty_on_http_error(mock_get):
    mock_get.side_effect = Exception("connection timeout")

    df = GHGRPFetcher().fetch("APPLE INC")

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_save_writes_files(tmp_path):
    df = pd.DataFrame(
        {
            "facility_name": ["Apple HQ"],
            "parent_company": ["APPLE INC"],
            "year": [2022],
            "ghg_quantity_mtco2e": [32000.0],
        }
    )
    raw = str(tmp_path / "raw" / "ghgrp_AAPL.csv")
    processed = str(tmp_path / "processed" / "ghgrp_AAPL.csv")

    GHGRPFetcher().save(df, raw_path=raw, processed_path=processed)

    assert pd.read_csv(raw).shape == df.shape
    assert pd.read_csv(processed).shape == df.shape
