from unittest.mock import MagicMock, patch

import pandas as pd

from pipeline.fetchers.echo import ECHOFetcher

SEARCH_RESPONSE = {
    "Results": {
        "Facilities": [
            {"FacName": "Apple Cupertino", "RegistryID": "110012345678", "StateCode": "CA"},
            {"FacName": "Apple Austin", "RegistryID": "110098765432", "StateCode": "TX"},
        ]
    }
}

ENFORCEMENT_RESPONSE = {
    "Results": {
        "FACInfo": {
            "TotalPenalties": "125000",
            "FormalActions": "2",
            "CAAViolations": "1",
            "CWAViolations": "0",
            "RCRAViolations": "1",
            "LastInspectionDate": "2023-04-15",
        }
    }
}

EMPTY_SEARCH = {"Results": {"Facilities": []}}


@patch("pipeline.fetchers.echo.requests.get")
def test_fetch_returns_dataframe(mock_get):
    def side_effect(url, **kwargs):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if "facilities_search" in url:
            resp.json.return_value = SEARCH_RESPONSE
        else:
            resp.json.return_value = ENFORCEMENT_RESPONSE
        return resp

    mock_get.side_effect = side_effect

    df = ECHOFetcher().fetch("APPLE INC")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "facility_name" in df.columns
    assert "penalty_amount" in df.columns
    assert "caa_violations" in df.columns


@patch("pipeline.fetchers.echo.requests.get")
def test_fetch_returns_empty_when_no_facilities(mock_get):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = EMPTY_SEARCH
    mock_get.return_value = resp

    df = ECHOFetcher().fetch("UNKNOWN CORP")

    assert isinstance(df, pd.DataFrame)
    assert df.empty


@patch("pipeline.fetchers.echo.requests.get")
def test_fetch_returns_empty_on_http_error(mock_get):
    mock_get.side_effect = Exception("connection refused")

    df = ECHOFetcher().fetch("APPLE INC")

    assert isinstance(df, pd.DataFrame)
    assert df.empty


@patch("pipeline.fetchers.echo.requests.get")
def test_fetch_partial_row_when_enforcement_call_fails(mock_get):
    def side_effect(url, **kwargs):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if "facilities_search" in url:
            resp.json.return_value = SEARCH_RESPONSE
        else:
            resp.raise_for_status.side_effect = Exception("enforcement API down")
        return resp

    mock_get.side_effect = side_effect

    df = ECHOFetcher().fetch("APPLE INC")

    # still returns rows — just with facility identity, no enforcement detail
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "facility_name" in df.columns


def test_save_writes_files(tmp_path):
    df = pd.DataFrame(
        {
            "facility_name": ["Apple HQ"],
            "registry_id": ["110012345678"],
            "penalty_amount": ["125000"],
        }
    )
    raw = str(tmp_path / "raw" / "echo_AAPL.csv")
    processed = str(tmp_path / "processed" / "echo_AAPL.csv")

    ECHOFetcher().save(df, raw_path=raw, processed_path=processed)

    assert pd.read_csv(raw).shape == df.shape
    assert pd.read_csv(processed).shape == df.shape
