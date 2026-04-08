from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.fetchers.ea_pollution import EAPollutionFetcher

CKAN_RESPONSE = {
    "result": {
        "resources": [
            {"format": "CSV", "url": "https://example.com/pollution-inventory.csv"},
            {"format": "PDF", "url": "https://example.com/notes.pdf"},
        ]
    }
}

CSV_DATA = """OperatorName,SiteName,Town,CountyRegion,Pollutant,Medium,TotalRelease,Threshold,Year,NACECode,NACEDescription
BP PLC,Grangemouth Refinery,Grangemouth,Scotland,Carbon dioxide,Air,1250000.0,100000.0,2022,19.20,Manufacture of refined petroleum products
BP PLC,Grangemouth Refinery,Grangemouth,Scotland,NOx,Air,850.0,100.0,2022,19.20,Manufacture of refined petroleum products
BP PLC,Grangemouth Refinery,Grangemouth,Scotland,Carbon dioxide,Air,1320000.0,100000.0,2021,19.20,Manufacture of refined petroleum products
Shell UK Ltd,Stanlow Refinery,Ellesmere Port,Cheshire,Carbon dioxide,Air,980000.0,100000.0,2022,19.20,Manufacture of refined petroleum products
"""


def make_mock_get(ckan=CKAN_RESPONSE, csv=CSV_DATA):
    def side_effect(url, **kwargs):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if "data.gov.uk" in url:
            resp.json.return_value = ckan
        else:
            resp.text = csv
        return resp

    return side_effect


@patch("pipeline.fetchers.ea_pollution.requests.get")
def test_fetch_returns_dataframe(mock_get):
    mock_get.side_effect = make_mock_get()

    df = EAPollutionFetcher().fetch("BP PLC")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert "operator_name" in df.columns
    assert "pollutant" in df.columns
    assert "total_release_tonnes" in df.columns
    assert "year" in df.columns


@patch("pipeline.fetchers.ea_pollution.requests.get")
def test_fetch_filters_by_company_name(mock_get):
    mock_get.side_effect = make_mock_get()

    df = EAPollutionFetcher().fetch("BP PLC")

    assert all(df["operator_name"].str.contains("BP", case=False))


@patch("pipeline.fetchers.ea_pollution.requests.get")
def test_fetch_filters_by_year(mock_get):
    mock_get.side_effect = make_mock_get()

    df = EAPollutionFetcher().fetch("BP PLC", year=2022)

    assert len(df) == 2
    assert all(df["year"] == 2022)


@patch("pipeline.fetchers.ea_pollution.requests.get")
def test_fetch_returns_empty_when_company_not_found(mock_get):
    mock_get.side_effect = make_mock_get()

    df = EAPollutionFetcher().fetch("UNKNOWN COMPANY XYZ")

    assert isinstance(df, pd.DataFrame)
    assert df.empty


@patch("pipeline.fetchers.ea_pollution.requests.get")
def test_fetch_returns_empty_when_no_csv_resource(mock_get):
    ckan_no_csv = {
        "result": {"resources": [{"format": "PDF", "url": "https://example.com/notes.pdf"}]}
    }
    mock_get.side_effect = make_mock_get(ckan=ckan_no_csv)

    df = EAPollutionFetcher().fetch("BP PLC")

    assert isinstance(df, pd.DataFrame)
    assert df.empty


@patch("pipeline.fetchers.ea_pollution.requests.get")
def test_fetch_returns_empty_on_http_error(mock_get):
    mock_get.side_effect = Exception("connection timeout")

    df = EAPollutionFetcher().fetch("BP PLC")

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_save_writes_files(tmp_path):
    df = pd.DataFrame(
        {
            "operator_name": ["BP PLC"],
            "pollutant": ["Carbon dioxide"],
            "total_release_tonnes": [1250000.0],
            "year": [2022],
        }
    )
    raw = str(tmp_path / "raw" / "ea_pollution_BP.csv")
    processed = str(tmp_path / "processed" / "ea_pollution_BP.csv")

    EAPollutionFetcher().save(df, raw_path=raw, processed_path=processed)

    assert pd.read_csv(raw).shape == df.shape
    assert pd.read_csv(processed).shape == df.shape
