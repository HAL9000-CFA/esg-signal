from pipeline.fetchers.edgar import EDGARFetcher


def test_edgar_fetch():
    fetcher = EDGARFetcher(user_email="test@example.com")

    profile = fetcher.fetch("MSFT")

    assert profile.identifier is not None
    assert profile.revenue is not None
    assert profile.total_assets is not None
    assert profile.country == "US"
