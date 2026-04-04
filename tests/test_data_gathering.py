#!/usr/bin/env python3
"""
Test script for Data Gatherer
Demonstrates usage and validates functionality

NOTE: OUR UNITS TESTS SHOULD NOT USE LIVE APIS. Magic mock the HTTP responses and use those instead. THESE NEED REPLACING"
"""

import os

import pytest

from agents.data_gathering import DataGatherer


def print_section(title):
    """print a formatted section header"""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


@pytest.mark.skipif(
    not os.getenv("SEC_EMAIL"), reason="Skipping live API test in CI — set SEC_EMAIL to run locally"
)
def test_edgar_fetcher():
    """test EDGAR data fetching"""
    print_section("Testing EDGAR Fetcher")

    from agents.data_gathering import EDGARFetcher

    edgar = EDGARFetcher(user_email="test@example.com")

    # test ticker conversion
    print("Testing CIK lookup...")
    cik = edgar.get_cik_from_ticker("AAPL")
    print(f"✓ AAPL CIK: {cik}")

    # test 10-K fetch
    print("\nTesting 10-K fetch...")
    filing_info = edgar.get_latest_10k("AAPL")
    if filing_info:
        print(f"✓ Latest 10-K: {filing_info['filingDate']}")
        print(f"  Accession: {filing_info['accessionNumber']}")
    else:
        print("✗ Failed to fetch 10-K")

    return filing_info is not None


@pytest.mark.skipif(
    not os.getenv("SEC_EMAIL"), reason="Skipping live API test in CI — set SEC_EMAIL to run locally"
)
def test_gdelt_fetcher():
    """test GDELT data fetching"""
    print_section("Testing GDELT Fetcher")

    from agents.data_gathering import GDELTFetcher

    gdelt = GDELTFetcher()

    print("Fetching Apple news mentions...")
    result = gdelt.fetch("Apple", start_date="20240301", end_date="20240310")

    if result["status"] == "success":
        article_count = result["data"]["article_count"]
        print(f"✓ Found {article_count} articles")

        if article_count > 0:
            articles = result["data"]["articles"][:3]
            print("\nSample articles:")
            for i, article in enumerate(articles, 1):
                print(f"  {i}. {article.get('title', 'No title')[:60]}...")

        return True
    else:
        print(f"✗ GDELT fetch failed: {result.get('error', 'Unknown error')}")
        return False


@pytest.mark.skipif(
    not os.getenv("SEC_EMAIL"), reason="Skipping live API test in CI — set SEC_EMAIL to run locally"
)
def test_full_fetch(tmp_path):
    """test complete Data Gatherer fetch"""
    print_section("Testing Full Data Gatherer Fetch")

    # Initialise Data Gatherer
    agent = DataGatherer(sec_email="test@example.com", companies_house_key="TEST_KEY")

    # test with Apple
    ticker = "AAPL"
    company_name = "Apple Inc."

    print(f"Fetching data for {ticker}...")

    results = agent.fetch_all(
        ticker=ticker, company_name=company_name, date_range=("20240301", "20240310")
    )

    # print summary
    print("\nResults Summary:")
    for source, data in results["sources"].items():
        status = data.get("status", "unknown")
        status_symbol = "✓" if status == "success" else "⚠" if status == "partial" else "✗"
        print(f"  {status_symbol} {source.upper():20s} {status}")

    # save test results
    output_file = tmp_path / f"test_results_{ticker}.json"
    agent.save_results(results, str(output_file))

    print(f"\n✓ Test results saved to: {output_file}")

    return True


def run_example_queries():
    """Run example queries with different tickers"""
    print_section("Example Queries")

    examples = [
        {
            "ticker": "AAPL",
            "company": "Apple Inc.",
            "description": "Tech company with strong ESG focus",
        },
        {
            "ticker": "MSFT",
            "company": "Microsoft Corporation",
            "description": "Cloud and software leader",
        },
        {"ticker": "TSLA", "company": "Tesla Inc.", "description": "Electric vehicle manufacturer"},
    ]

    agent = DataGatherer(sec_email="test@example.com")

    for example in examples:
        print(f"\n{example['ticker']}: {example['description']}")
        print("-" * 50)

        # just fetch EDGAR as example
        edgar_result = agent.edgar.fetch(example["ticker"])

        if edgar_result["status"] == "success":
            filing_date = edgar_result["data"]["filing_info"]["filingDate"]
            print(f"  ✓ Latest 10-K: {filing_date}")
        else:
            print(f"  ✗ Failed: {edgar_result.get('error', 'Unknown error')}")


def main():
    """Run all tests"""
    print(
        """
    Data Gatherer: Multi-Source ESG Data Fetcher
    Test & Example
    """
    )

    tests = [
        ("EDGAR Fetcher", test_edgar_fetcher),
        ("GDELT Fetcher", test_gdelt_fetcher),
        ("Full Fetch", test_full_fetch),
    ]

    results = {}

    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"\n✗ {test_name} failed with error: {e}")
            results[test_name] = False

    # print test summary
    print_section("Test Summary")

    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status:10s} {test_name}")

    # run example queries
    try:
        run_example_queries()
    except Exception as e:
        print(f"\n✗ Example queries failed: {e}")

    print_section("Test Complete")

    total_tests = len(results)
    passed_tests = sum(1 for v in results.values() if v)

    print(f"Results: {passed_tests}/{total_tests} tests passed")

    if passed_tests == total_tests:
        print("\n🎉 All tests passed! Data Gatherer is ready for submission.")
    else:
        print(f"\n⚠ {total_tests - passed_tests} test(s) failed. Review errors above.")

    print("\nNext steps:")
    print("  1. Review output/test_results_*.json files")
    print("  2. Update .env with your real credentials")
    print("  3. Run: python scripts/fetch_data.py --ticker YOUR_TICKER --email YOUR_EMAIL")
    print()


if __name__ == "__main__":
    main()
