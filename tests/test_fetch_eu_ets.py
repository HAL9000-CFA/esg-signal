import io
import zipfile
from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.fetchers.eu_ets import EUETSFetcher

CSV_DATA = """accountHolderName,installationName,country,mainActivityType,year,verifiedEmissions,allocatedFreeAllowances
BP PLC,Grangemouth CHP,GB,Energy,2022,850000,720000
BP PLC,Grangemouth CHP,GB,Energy,2021,920000,750000
BP PLC,Rotterdam Refinery,NL,Refining,2022,1100000,900000
Shell PLC,Pernis Refinery,NL,Refining,2022,1400000,1100000
"""


def make_zip_response(csv_content: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("euets.csv", csv_content)
    return buf.getvalue()


def make_mock_get(csv=CSV_DATA):
    def side_effect(url, **kwargs):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.content = make_zip_response(csv)
        return resp

    return side_effect


@patch("pipeline.fetchers.eu_ets.requests.get")
def test_fetch_returns_dataframe(mock_get):
    mock_get.side_effect = make_mock_get()

    df = EUETSFetcher().fetch("BP PLC")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert "account_holder" in df.columns
    assert "verified_emissions_tco2e" in df.columns
    assert "year" in df.columns
    assert "country" in df.columns


@patch("pipeline.fetchers.eu_ets.requests.get")
def test_fetch_filters_by_company_name(mock_get):
    mock_get.side_effect = make_mock_get()

    df = EUETSFetcher().fetch("BP PLC")

    assert all(df["account_holder"].str.contains("BP", case=False))


@patch("pipeline.fetchers.eu_ets.requests.get")
def test_fetch_filters_by_year(mock_get):
    mock_get.side_effect = make_mock_get()

    df = EUETSFetcher().fetch("BP PLC", year=2022)

    assert len(df) == 2
    assert all(df["year"] == 2022)


@patch("pipeline.fetchers.eu_ets.requests.get")
def test_fetch_returns_empty_when_company_not_found(mock_get):
    mock_get.side_effect = make_mock_get()

    df = EUETSFetcher().fetch("UNKNOWN COMPANY XYZ")

    assert isinstance(df, pd.DataFrame)
    assert df.empty


@patch("pipeline.fetchers.eu_ets.requests.get")
def test_fetch_returns_empty_on_http_error(mock_get):
    mock_get.side_effect = Exception("connection refused")

    df = EUETSFetcher().fetch("BP PLC")

    assert isinstance(df, pd.DataFrame)
    assert df.empty


@patch("pipeline.fetchers.eu_ets.requests.get")
def test_fetch_returns_empty_when_zip_has_no_csv(mock_get):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("readme.txt", "no csv here")

    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.content = buf.getvalue()
    mock_get.return_value = resp

    df = EUETSFetcher().fetch("BP PLC")

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_save_writes_files(tmp_path):
    df = pd.DataFrame(
        {
            "account_holder": ["BP PLC"],
            "installation_name": ["Grangemouth CHP"],
            "country": ["GB"],
            "year": [2022],
            "verified_emissions_tco2e": [850000],
        }
    )
    raw = str(tmp_path / "raw" / "eu_ets_BP.csv")
    processed = str(tmp_path / "processed" / "eu_ets_BP.csv")

    EUETSFetcher().save(df, raw_path=raw, processed_path=processed)

    assert pd.read_csv(raw).shape == df.shape
    assert pd.read_csv(processed).shape == df.shape
