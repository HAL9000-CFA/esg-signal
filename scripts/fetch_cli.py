#!/usr/bin/env python3
"""
Agent 1 CLI - command-line interface for data fetching
Usage: python agent1_cli.py --ticker AAPL --email your@email.com
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from agent1fetchers import Agent1


def main():
    parser = argparse.ArgumentParser(
        description="Agent 1: Multi-Source ESG Data Fetcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # fetch data for Apple
  python agent1_cli.py --ticker AAPL --email your@email.com

  # fetch with company name for UK sources
  python agent1_cli.py --ticker BP --company "BP PLC" --email your@email.com

  # include PDF extraction
  python agent1_cli.py --ticker MSFT --pdf sustainability_report.pdf --email your@email.com

  # custom date range for GDELT
  python agent1_cli.py --ticker TSLA --start 20240101 --end 20240331 --email your@email.com
        """,
    )

    # Required arguments
    parser.add_argument(
        "--ticker", required=True, help="Stock ticker symbol (e.g., AAPL, MSFT, TSLA)"
    )

    parser.add_argument(
        "--email", required=True, help="Your email address (required for SEC EDGAR API compliance)"
    )

    # Optional arguments
    parser.add_argument(
        "--company", help="Full company name (for Companies House and other non-ticker sources)"
    )

    parser.add_argument("--pdf", help="Path to sustainability report PDF for extraction")

    parser.add_argument(
        "--companies-house-key",
        default="YOUR_API_KEY_HERE",
        help="Companies House API key (for UK companies)",
    )

    parser.add_argument("--start", help="Start date for GDELT search (YYYYMMDD format)")

    parser.add_argument("--end", help="End date for GDELT search (YYYYMMDD format)")

    parser.add_argument("--output", help="Output file path (default: agent1_TICKER_TIMESTAMP.json)")

    parser.add_argument(
        "--output-dir", default="./output", help="Output directory (default: ./output)"
    )

    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")

    args = parser.parse_args()

    # create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)

    # initialize Agent 1
    print(f"\n{'='*70}")
    print("  Agent 1: Multi-Source ESG Data Fetcher")
    print(f"{'='*70}\n")

    print("Configuration:")
    print(f"  Ticker: {args.ticker}")
    print(f"  Email: {args.email}")
    if args.company:
        print(f"  Company: {args.company}")
    if args.pdf:
        print(f"  PDF: {args.pdf}")
    if args.start and args.end:
        print(f"  Date Range: {args.start} to {args.end}")
    print()

    agent = Agent1(sec_email=args.email, companies_house_key=args.companies_house_key)

    # prepare date range for GDELT
    date_range = None
    if args.start and args.end:
        date_range = (args.start, args.end)

    # fetch all data
    try:
        results = agent.fetch_all(
            ticker=args.ticker, company_name=args.company, pdf_path=args.pdf, date_range=date_range
        )

        # determine output file
        if args.output:
            output_path = Path(args.output)
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = output_dir / f"agent1_{args.ticker}_{timestamp}.json"

        # save results
        agent.save_results(results, str(output_path))

        # print summary
        print(f"\n{'='*70}")
        print("  Summary")
        print(f"{'='*70}\n")

        for source, data in results["sources"].items():
            status = data.get("status", "unknown")
            status_symbol = "✓" if status == "success" else "⚠" if status == "partial" else "✗"
            print(f"{status_symbol} {source.upper():20s} {status}")

        print(f"\n{'='*70}")
        print(f"✓ Results saved to: {output_path}")
        print(f"{'='*70}\n")

        return 0

    except Exception as e:
        print(f"\n{'='*70}")
        print(f"✗ Error: {e}")
        print(f"{'='*70}\n")

        if args.verbose:
            import traceback

            traceback.print_exc()

        return 1


if __name__ == "__main__":
    sys.exit(main())
