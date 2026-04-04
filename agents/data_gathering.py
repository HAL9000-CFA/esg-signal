"""
Data Gathering Agent: Multi-Source ESG Data Fetcher
Fetches data from EDGAR, Companies House, CDP, GDELT, and LayoutParser
"""

import json
import re
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

warnings.filterwarnings("ignore")


class EDGARFetcher:
    """Fetches 10-K filings, risk factors, and financial data from SEC EDGAR"""

    BASE_URL = "https://data.sec.gov/submissions/"
    FILING_BASE = "https://www.sec.gov/Archives/edgar/data/"

    def __init__(self, user_email: str = "your.email@example.com"):
        """
        Initialize EDGAR fetcher with required user agent

        Args:
            user_email: Email address for SEC API compliance
        """
        self.headers = {
            "User-Agent": f"{user_email}",
            "Accept-Encoding": "gzip, deflate",
            "Host": "data.sec.gov",
        }

    def get_cik_from_ticker(self, ticker: str) -> Optional[str]:
        """
        Convert stock ticker to CIK number

        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL', 'MSFT')

        Returns:
            CIK number as string, padded to 10 digits
        """
        try:
            # Get ticker to CIK mapping from SEC
            url = "https://www.sec.gov/files/company_tickers.json"
            headers = {"User-Agent": self.headers["User-Agent"]}
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            data = response.json()
            ticker = ticker.upper()

            for entry in data.values():
                if entry.get("ticker", "").upper() == ticker:
                    cik = str(entry["cik_str"]).zfill(10)
                    return cik

            print(f"Warning: Ticker {ticker} not found in SEC database")
            return None

        except Exception as e:
            print(f"Error converting ticker to CIK: {e}")
            return None

    def get_company_filings(self, ticker: str) -> Optional[Dict]:
        """
        Get all filings for a company

        Args:
            ticker: Stock ticker symbol

        Returns:
            Dict containing filing metadata and URLs
        """
        cik = self.get_cik_from_ticker(ticker)
        if not cik:
            return None

        try:
            url = f"{self.BASE_URL}CIK{cik}.json"
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()

            return response.json()

        except Exception as e:
            print(f"Error fetching filings: {e}")
            return None

    def get_latest_10k(self, ticker: str) -> Optional[Dict]:
        """
        Get the most recent 10-K filing

        Args:
            ticker: Stock ticker symbol

        Returns:
            Dict with 10-K metadata including accession number and filing date
        """
        filings_data = self.get_company_filings(ticker)
        if not filings_data:
            return None

        try:
            recent_filings = filings_data.get("filings", {}).get("recent", {})
            forms = recent_filings.get("form", [])

            # Find first 10-K
            for idx, form in enumerate(forms):
                if form == "10-K":
                    return {
                        "accessionNumber": recent_filings["accessionNumber"][idx],
                        "filingDate": recent_filings["filingDate"][idx],
                        "reportDate": recent_filings["reportDate"][idx],
                        "primaryDocument": recent_filings["primaryDocument"][idx],
                        "form": form,
                    }

            print("No 10-K filing found")
            return None

        except Exception as e:
            print(f"Error parsing 10-K data: {e}")
            return None

    def download_10k_document(self, ticker: str, filing_info: Dict) -> Optional[str]:
        """
        Download the actual 10-K document

        Args:
            ticker: Stock ticker symbol
            filing_info: Filing metadata from get_latest_10k

        Returns:
            Document text content
        """
        cik = self.get_cik_from_ticker(ticker)
        if not cik:
            return None

        try:
            accession = filing_info["accessionNumber"].replace("-", "")
            primary_doc = filing_info["primaryDocument"]

            # Build document URL
            doc_url = f"{self.FILING_BASE}{cik}/{accession}/{primary_doc}"

            response = requests.get(doc_url, headers=self.headers, timeout=30)
            response.raise_for_status()

            return response.text

        except Exception as e:
            print(f"Error downloading 10-K document: {e}")
            return None

    def extract_risk_factors(self, document_text: str) -> str:
        """
        Extract Risk Factors section from 10-K document

        Args:
            document_text: Full 10-K document text

        Returns:
            Risk factors section text
        """
        if not document_text:
            return ""

        try:
            # Common patterns for Risk Factors section
            patterns = [
                r"(?i)item\s*1a\.?\s*risk\s*factors(.*?)(?=item\s*1b|item\s*2)",
                r"(?i)risk\s*factors(.*?)(?=item\s*1b|item\s*2|unresolved\s*staff)",
            ]

            for pattern in patterns:
                match = re.search(pattern, document_text, re.DOTALL | re.IGNORECASE)
                if match:
                    risk_text = match.group(1).strip()
                    # Clean up HTML tags if present
                    risk_text = re.sub(r"<[^>]+>", " ", risk_text)
                    risk_text = re.sub(r"\s+", " ", risk_text)
                    return risk_text[:10000]  # Limit to reasonable size

            return "Risk factors section not found in standard format"

        except Exception as e:
            print(f"Error extracting risk factors: {e}")
            return ""

    def extract_financials(self, document_text: str) -> Dict[str, List[Dict]]:
        """
        Extract CapEx, OpEx, Revenue for last 3 years

        Args:
            document_text: Full 10-K document text

        Returns:
            Dict with financial metrics for 3 years
        """
        # This is a simplified extraction - real implementation would parse XBRL
        # For production, use sec-api or xbrl parsing libraries

        financials = {
            "years": [],
            "revenue": [],
            "operating_expenses": [],
            "capital_expenditures": [],
        }

        try:
            # Extract year references
            year_pattern = r"(20\d{2})"
            years = re.findall(year_pattern, document_text)
            unique_years = sorted(set(years), reverse=True)[:3]

            financials["years"] = unique_years

            # Note: Real implementation would parse structured XBRL data
            financials["note"] = (
                "Full financial extraction requires XBRL parsing - see EDGAR API documentation"
            )

            return financials

        except Exception as e:
            print(f"Error extracting financials: {e}")
            return financials

    def fetch(self, ticker: str) -> Dict:
        """
        Main fetch method - gets all EDGAR data for ticker

        Args:
            ticker: Stock ticker symbol

        Returns:
            Dict containing risk factors and financial data
        """
        print(f"Fetching EDGAR data for {ticker}...")

        result = {
            "source": "EDGAR",
            "ticker": ticker,
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "data": {},
        }

        try:
            # Get latest 10-K
            filing_info = self.get_latest_10k(ticker)
            if not filing_info:
                result["error"] = "No 10-K filing found"
                return result

            result["data"]["filing_info"] = filing_info

            # Download document
            document = self.download_10k_document(ticker, filing_info)
            if not document:
                result["error"] = "Could not download 10-K document"
                return result

            # Extract risk factors
            risk_factors = self.extract_risk_factors(document)
            result["data"]["risk_factors"] = risk_factors

            # Extract financials
            financials = self.extract_financials(document)
            result["data"]["financials"] = financials

            result["status"] = "success"
            print("✓ EDGAR data fetched successfully")

        except Exception as e:
            result["error"] = str(e)
            print(f"✗ EDGAR fetch failed: {e}")

        return result


class CompaniesHouseFetcher:
    """Fetches UK company filings and environmental disclosures"""

    BASE_URL = "https://api.company-information.service.gov.uk"

    def __init__(self, api_key: str = "YOUR_API_KEY_HERE"):
        """
        Initialize Companies House fetcher

        Args:
            api_key: Companies House API key
        """
        self.api_key = api_key
        self.auth = (api_key, "")

    def search_company(self, company_name: str) -> Optional[str]:
        """
        Search for company and get company number

        Args:
            company_name: Company name to search

        Returns:
            Company number
        """
        try:
            url = f"{self.BASE_URL}/search/companies"
            params = {"q": company_name, "items_per_page": 5}

            response = requests.get(url, params=params, auth=self.auth, timeout=10)
            response.raise_for_status()

            data = response.json()
            items = data.get("items", [])

            if items:
                # Return first match
                return items[0].get("company_number")

            return None

        except Exception as e:
            print(f"Error searching company: {e}")
            return None

    def get_filing_history(self, company_number: str) -> List[Dict]:
        """
        Get filing history for company

        Args:
            company_number: UK company number

        Returns:
            List of filing records
        """
        try:
            url = f"{self.BASE_URL}/company/{company_number}/filing-history"

            response = requests.get(url, auth=self.auth, timeout=10)
            response.raise_for_status()

            data = response.json()
            return data.get("items", [])

        except Exception as e:
            print(f"Error fetching filing history: {e}")
            return []

    def get_confirmation_statement(self, filings: List[Dict]) -> Optional[Dict]:
        """
        Extract most recent confirmation statement

        Args:
            filings: List of filing records

        Returns:
            Confirmation statement data
        """
        for filing in filings:
            category = filing.get("category", "").lower()
            if "confirmation" in category or "annual-return" in category:
                return filing

        return None

    def get_environmental_disclosures(self, filings: List[Dict]) -> List[Dict]:
        """
        Extract environmental and sustainability disclosures

        Args:
            filings: List of filing records

        Returns:
            List of environmental disclosure filings
        """
        environmental = []

        keywords = ["environmental", "sustainability", "climate", "carbon", "energy"]

        for filing in filings:
            description = filing.get("description", "").lower()
            category = filing.get("category", "").lower()

            if any(keyword in description or keyword in category for keyword in keywords):
                environmental.append(filing)

        return environmental

    def fetch(self, company_name: str) -> Dict:
        """
        Main fetch method - gets Companies House data

        Args:
            company_name: UK company name

        Returns:
            Dict containing confirmation statement and environmental disclosures
        """
        print(f"Fetching Companies House data for {company_name}...")

        result = {
            "source": "Companies House",
            "company": company_name,
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "data": {},
        }

        try:
            # Search for company
            company_number = self.search_company(company_name)
            if not company_number:
                result["error"] = "Company not found"
                return result

            result["data"]["company_number"] = company_number

            # Get filing history
            filings = self.get_filing_history(company_number)
            if not filings:
                result["error"] = "No filings found"
                return result

            # Get confirmation statement
            confirmation = self.get_confirmation_statement(filings)
            result["data"]["confirmation_statement"] = confirmation

            # Get environmental disclosures
            environmental = self.get_environmental_disclosures(filings)
            result["data"]["environmental_disclosures"] = environmental

            result["status"] = "success"
            print("✓ Companies House data fetched successfully")

        except Exception as e:
            result["error"] = str(e)
            print(f"✗ Companies House fetch failed: {e}")

        return result


class CDPFetcher:
    """Fetches climate and water disclosure data from CDP Open Data Portal"""

    # CDP provides annual CSV files - these URLs are examples
    BASE_URL = "https://data.cdp.net/api/views/"

    def __init__(self):
        """Initialize CDP fetcher"""
        pass

    def fetch(self, company_name: str) -> Dict:
        """
        Fetch CDP climate and water disclosures

        Args:
            company_name: Company name to search

        Returns:
            Dict containing climate and water disclosure data
        """
        print(f"Fetching CDP data for {company_name}...")

        result = {
            "source": "CDP",
            "company": company_name,
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "data": {},
        }

        try:
            # Note: CDP Open Data Portal provides CSV files
            # Real implementation would download and parse CSV files
            # from https://www.cdp.net/en/data

            result["data"] = {
                "climate_disclosure": {
                    "note": "CDP data requires downloading annual CSV files from CDP Open Data Portal",
                    "portal_url": "https://www.cdp.net/en/data",
                    "company": company_name,
                },
                "water_disclosure": {
                    "note": "Water data available in separate CDP dataset",
                    "portal_url": "https://www.cdp.net/en/data",
                    "company": company_name,
                },
            }

            result["status"] = "partial"
            print("⚠ CDP data requires manual CSV download - placeholder returned")

        except Exception as e:
            result["error"] = str(e)
            print(f"✗ CDP fetch failed: {e}")

        return result


class GDELTFetcher:
    """Fetches news and events data from GDELT"""

    BASE_URL = "https://api.gdeltproject.org/api/v2/"

    def __init__(self):
        """Initialize GDELT fetcher"""
        pass

    def fetch(self, company_name: str, start_date: str = None, end_date: str = None) -> Dict:
        """
        Fetch GDELT news mentions and events

        Args:
            company_name: Company name to search
            start_date: Start date (YYYYMMDD format)
            end_date: End date (YYYYMMDD format)

        Returns:
            Dict containing news articles and event data
        """
        print(f"Fetching GDELT data for {company_name}...")

        # Default to last 30 days if no dates provided
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")

        result = {
            "source": "GDELT",
            "company": company_name,
            "date_range": f"{start_date} to {end_date}",
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "data": {},
        }

        try:
            # GDELT DOC 2.0 API
            url = f"{self.BASE_URL}doc/doc"
            params = {
                "query": company_name,
                "mode": "artlist",
                "maxrecords": 250,
                "format": "json",
                "startdatetime": start_date + "000000",
                "enddatetime": end_date + "235959",
            }

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            articles = data.get("articles", [])

            result["data"] = {
                "article_count": len(articles),
                "articles": articles[:50],  # Limit to first 50
                "summary": {
                    "total_mentions": len(articles),
                    "date_range": f"{start_date} to {end_date}",
                    "sources": list(set([a.get("domain", "") for a in articles[:50]]))[:10],
                },
            }

            result["status"] = "success"
            print(f"✓ GDELT data fetched successfully ({len(articles)} articles)")

        except Exception as e:
            result["error"] = str(e)
            print(f"✗ GDELT fetch failed: {e}")

        return result


class LayoutParserExtractor:
    """Extracts structured text from sustainability PDF reports"""

    def __init__(self):
        """Initialize LayoutParser extractor"""
        pass

    def extract_from_pdf(self, pdf_path: str) -> Dict:
        """
        Extract structured content from sustainability report PDF

        Args:
            pdf_path: Path to PDF file

        Returns:
            Dict containing structured text by section
        """
        print(f"Extracting content from PDF: {pdf_path}...")

        result = {
            "source": "LayoutParser",
            "file": pdf_path,
            "timestamp": datetime.now().isoformat(),
            "status": "failed",
            "data": {},
        }

        try:
            # Check if file exists
            if not Path(pdf_path).exists():
                result["error"] = f"File not found: {pdf_path}"
                return result

            # note btw we could use
            # - PyMuPDF (fitz) for PDF text extraction
            # - LayoutParser for layout detection
            # - then table extraction libraries - forgot the name

            # Placeholder implementation
            import PyPDF2

            with open(pdf_path, "rb") as file:
                reader = PyPDF2.PdfReader(file)

                sections = {}
                full_text = ""

                for page_num, page in enumerate(reader.pages):
                    text = page.extract_text()
                    full_text += text + "\n"

                # Simple section detection based on common headers
                section_headers = [
                    "executive summary",
                    "environmental",
                    "social",
                    "governance",
                    "climate",
                    "emissions",
                    "energy",
                    "water",
                    "waste",
                    "biodiversity",
                    "human rights",
                    "diversity",
                    "safety",
                    "supply chain",
                ]

                for header in section_headers:
                    pattern = f"(?i)(?:^|\n)({header}[^\n]*)\n(.*?)(?=\n[A-Z][^\n]*\n|$)"
                    matches = re.findall(pattern, full_text, re.DOTALL)
                    if matches:
                        sections[header] = {
                            "title": matches[0][0].strip(),
                            "content": matches[0][1].strip()[:1000],  # Limit length
                        }

                result["data"] = {
                    "page_count": len(reader.pages),
                    "sections": sections,
                    "note": "Full layout parsing requires LayoutParser library",
                }

                result["status"] = "success"
                print(f"✓ PDF extraction completed ({len(reader.pages)} pages)")

        except ImportError:
            result["error"] = "PyPDF2 not installed - run: pip install PyPDF2"
            print("✗ PDF extraction failed: PyPDF2 not available")
        except Exception as e:
            result["error"] = str(e)
            print(f"✗ PDF extraction failed: {e}")

        return result


class DataGatherer:
    """
    Main data gathering orchestrator - coordinates all data fetchers
    """

    def __init__(
        self,
        sec_email: str = "your.email@example.com",
        companies_house_key: str = "YOUR_API_KEY_HERE",
    ):
        """
        Initialise data gatherer with API credentials

        Args:
            sec_email: Email for SEC EDGAR API
            companies_house_key: API key for Companies House
        """
        self.edgar = EDGARFetcher(user_email=sec_email)
        self.companies_house = CompaniesHouseFetcher(api_key=companies_house_key)
        self.cdp = CDPFetcher()
        self.gdelt = GDELTFetcher()
        self.layout_parser = LayoutParserExtractor()

    def fetch_all(
        self,
        ticker: str,
        company_name: str = None,
        pdf_path: str = None,
        date_range: Tuple[str, str] = None,
    ) -> Dict:
        """
        Fetch data from all sources

        Args:
            ticker: Stock ticker symbol
            company_name: Full company name (for non-US sources)
            pdf_path: Path to sustainability PDF report (optional)
            date_range: Tuple of (start_date, end_date) for GDELT

        Returns:
            Dict containing all fetched data
        """
        print(f"\n{'='*60}")
        print(f"Data : Starting data fetch for {ticker}")
        print(f"{'='*60}\n")

        results = {
            "ticker": ticker,
            "company_name": company_name or ticker,
            "timestamp": datetime.now().isoformat(),
            "sources": {},
        }

        # 1. EDGAR (SEC filings)
        edgar_data = self.edgar.fetch(ticker)
        results["sources"]["edgar"] = edgar_data
        time.sleep(0.5)  # Rate limiting

        # 2. Companies House (if UK company)
        if company_name:
            ch_data = self.companies_house.fetch(company_name)
            results["sources"]["companies_house"] = ch_data
            time.sleep(0.5)

        # 3. CDP (climate/water disclosures)
        cdp_data = self.cdp.fetch(company_name or ticker)
        results["sources"]["cdp"] = cdp_data
        time.sleep(0.5)

        # 4. GDELT (news/events)
        start_date, end_date = date_range if date_range else (None, None)
        gdelt_data = self.gdelt.fetch(company_name or ticker, start_date, end_date)
        results["sources"]["gdelt"] = gdelt_data
        time.sleep(0.5)

        # 5. LayoutParser (PDF extraction)
        if pdf_path:
            pdf_data = self.layout_parser.extract_from_pdf(pdf_path)
            results["sources"]["layout_parser"] = pdf_data

        print(f"\n{'='*60}")
        print("DATA GATHERER: Data fetch complete")
        print(f"{'='*60}\n")

        return results

    def save_results(self, results: Dict, output_path: str = "data_gatherer_output.json"):
        """
        Save results to JSON file

        Args:
            results: Results dict from fetch_all
            output_path: Output file path
        """
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to: {output_path}")


# testing fornow
if __name__ == "__main__":
    # example usage
    print("Data Gatherer - example usage\n")

    # add  your credentials
    agent = DataGatherer(
        sec_email="your.email@example.com",  # required: use real email for SEC
        companies_house_key="YOUR_API_KEY_HERE",  # get from developer.company-information.service.gov.uk
    )

    # example: Fetch data for Microsoft
    ticker = "MSFT"
    company_name = "Microsoft Corporation"

    results = agent.fetch_all(
        ticker=ticker,
        company_name=company_name,
        # pdf_path="/path/to/sustainability_report.pdf",  # Optional
        # date_range=("20240101", "20240331")  # Optional: for GDELT
    )

    # save results
    agent.save_results(results, f"data_gatherer_{ticker}_output.json")

    print("\n✓ Data gatherer execution complete!")
    print(f"✓ Check data_gatherer_{ticker}_output.json for results")
