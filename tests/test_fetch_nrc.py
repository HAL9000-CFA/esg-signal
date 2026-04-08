from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.fetchers.nrc import NRCFetcher

NRC_RESPONSE = [
    {
        "reportNumber": "1234567",
        "incidentDate": "2023-06-15",
        "material": "CRUDE OIL",
        "quantity": "500",
        "unit": "GALLONS",
        "description": "Pipeline rupture during maintenance.",
        "companyName": "APPLE INC",
        "state": "CA",
    },
    {
        "reportNumber": "1234568",
        "incidentDate": "2022-11-03",
        "material": "DIESEL FUEL",
        "quantity": "200",
        "unit": "GALLONS",
        "description": "Tank overflow at facility.",
        "companyName": "APPLE INC",
        "state": "TX",
    },
]

NRC_WRAPPED_RESPONSE = {"incidents": NRC_RESPONSE}


@patch("pipeline.fetchers.nrc.requests.post")
def test_fetch_returns_dataframe_list_response(mock_post):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = NRC_RESPONSE
    mock_post.return_value = resp

    df = NRCFetcher().fetch("APPLE INC")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "report_number" in df.columns
    assert "incident_date" in df.columns
    assert "material" in df.columns


@patch("pipeline.fetchers.nrc.requests.post")
def test_fetch_returns_dataframe_wrapped_response(mock_post):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = NRC_WRAPPED_RESPONSE
    mock_post.return_value = resp

    df = NRCFetcher().fetch("APPLE INC")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2


@patch("pipeline.fetchers.nrc.requests.post")
def test_fetch_returns_empty_when_no_incidents(mock_post):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = []
    mock_post.return_value = resp

    df = NRCFetcher().fetch("UNKNOWN CORP")

    assert isinstance(df, pd.DataFrame)
    assert df.empty


@patch("pipeline.fetchers.nrc.requests.post")
def test_fetch_returns_empty_on_api_unavailable(mock_post):
    mock_post.side_effect = Exception("NRC API timeout")

    df = NRCFetcher().fetch("APPLE INC")

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_save_writes_files(tmp_path):
    df = pd.DataFrame(
        {
            "report_number": ["1234567"],
            "incident_date": ["2023-06-15"],
            "material": ["CRUDE OIL"],
            "company_name": ["APPLE INC"],
        }
    )
    raw = str(tmp_path / "raw" / "nrc_AAPL.csv")
    processed = str(tmp_path / "processed" / "nrc_AAPL.csv")

    NRCFetcher().save(df, raw_path=raw, processed_path=processed)

    assert pd.read_csv(raw).shape == df.shape
    assert pd.read_csv(processed).shape == df.shape
