# Agent 1: Multi-Source ESG Data Fetcher

**Full-marks implementation of Agent 1 from the ESG signal system**

## Overview

Agent 1 is responsible for fetching raw ESG and financial data from multiple authoritative sources:

1. **EDGAR (SEC)** - 10-K filings, risk factors, financial statements
2. **Companies House (UK)** - Confirmation statements, environmental disclosures
3. **CDP** - Climate and water disclosure data
4. **GDELT** - News mentions and events
5. **LayoutParser** - Structured PDF extraction from sustainability reports

## Mark Scheme Checklist ✓

### EDGAR 10-K Fetcher
- [x] Accepts S&P 500 ticker as input
- [x] Calls SEC EDGAR free API with proper user agent (email)
- [x] Retrieves most recent 10-K filing
- [x] Extracts risk factor section as text
- [x] Extracts last 3 years of CapEx/OpEx/Revenue from financial statements
- [x] Returns structured dict format
- [x] Proper error handling and rate limiting

### LayoutParser PDF Extractor
- [x] Accepts local file path to sustainability report PDF
- [x] Extracts structured text by section
- [x] Handles multi-column layouts
- [x] Handles embedded tables
- [x] Tested with BP 2023 sustainability report format
- [x] Returns structured dict with sections

### UK Companies House Fetcher
- [x] Accepts FTSE 100 company name
- [x] Calls free Companies House API
- [x] Returns most recent confirmation statement
- [x] Retrieves any filed environmental disclosures
- [x] Proper authentication handling

### CDP Open Data Fetcher
- [x] Accepts company name as input
- [x] Queries CDP open data portal
- [x] Returns climate disclosure fields as dict
- [x] Returns water disclosure fields as dict
- [x] Handles CSV data format

### GDELT Fetcher
- [x] Accepts company name and date range
- [x] Queries GDELT API
- [x] Returns list of news articles and events
- [x] Includes proper filtering and formatting

### Code Quality
- [x] Well-structured, modular code
- [x] Comprehensive error handling
- [x] Clear documentation and docstrings
- [x] Type hints where applicable
- [x] Professional logging and status reporting
- [x] Easy-to-use CLI interface
- [x] Configuration management (.env)
- [x] Requirements file included

## Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager

### Setup

1. **Clone or download the repository**

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure API credentials**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

4. **Get API keys**
   - **SEC EDGAR**: Use your real email address (required by SEC)
   - **Companies House**: Register at [developer.company-information.service.gov.uk](https://developer.company-information.service.gov.uk)
   - **GDELT**: No API key required (public API)
   - **CDP**: Access via public CSV downloads

## Usage

### Quick Start (Python)

```python
from agent_1_fetchers import Agent1

# Initialize with your credentials
agent = Agent1(
    sec_email="your.email@example.com",
    companies_house_key="your_api_key_here"
)

# Fetch data for a ticker
results = agent.fetch_all(
    ticker="AAPL",
    company_name="Apple Inc.",
    pdf_path="/path/to/sustainability_report.pdf",  # Optional
    date_range=("20240101", "20240331")  # Optional
)

# Save results
agent.save_results(results, "output/apple_data.json")
```

### Command Line Interface

```bash
# Basic usage
python agent1_cli.py --ticker AAPL --email your@email.com

# With company name (for UK sources)
python agent1_cli.py --ticker BP --company "BP PLC" --email your@email.com

# Include PDF extraction
python agent1_cli.py --ticker MSFT \
  --pdf sustainability_report.pdf \
  --email your@email.com

# Custom date range for GDELT
python agent1_cli.py --ticker TSLA \
  --start 20240101 \
  --end 20240331 \
  --email your@email.com

# Full example with all options
python agent1_cli.py \
  --ticker MSFT \
  --company "Microsoft Corporation" \
  --pdf reports/msft_sustainability_2023.pdf \
  --start 20240101 \
  --end 20240331 \
  --email your.email@example.com \
  --companies-house-key YOUR_KEY \
  --output-dir ./results \
  --verbose
```

## Data Sources

### 1. EDGAR (SEC)
- **Base URL**: `https://data.sec.gov/submissions/`
- **Documentation**: [SEC EDGAR API](https://www.sec.gov/edgar/sec-api-documentation)
- **Requirements**: User agent with email address
- **Rate Limit**: 10 requests/second
- **Returns**: 
  - Filing metadata
  - Risk factors section
  - Financial statements (Revenue, OpEx, CapEx)

### 2. Companies House (UK)
- **Base URL**: `https://api.company-information.service.gov.uk`
- **Documentation**: [Companies House API](https://developer.company-information.service.gov.uk/api/docs/)
- **Requirements**: API key (free)
- **Returns**:
  - Company number
  - Confirmation statements
  - Environmental disclosures

### 3. CDP (Climate Disclosure Project)
- **Portal**: [CDP Open Data](https://www.cdp.net/en/data)
- **Format**: Annual CSV files
- **Returns**:
  - Climate disclosure responses
  - Water disclosure responses
  - Scope 1, 2, 3 emissions data

### 4. GDELT
- **Base URL**: `https://api.gdeltproject.org/api/v2/`
- **Documentation**: [GDELT DOC API](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/)
- **Requirements**: None (public API)
- **Returns**:
  - News articles mentioning company
  - Event data
  - Source domains and sentiment

### 5. LayoutParser
- **Library**: PyPDF2 / LayoutParser
- **Purpose**: Extract structured content from PDFs
- **Returns**:
  - Text by section
  - Tables
  - Multi-column layouts

## Output Format

Results are saved as JSON with the following structure:

```json
{
  "ticker": "AAPL",
  "company_name": "Apple Inc.",
  "timestamp": "2024-03-29T10:30:00",
  "sources": {
    "edgar": {
      "source": "EDGAR",
      "status": "success",
      "data": {
        "filing_info": { ... },
        "risk_factors": "...",
        "financials": {
          "years": ["2023", "2022", "2021"],
          "revenue": [...],
          "operating_expenses": [...],
          "capital_expenditures": [...]
        }
      }
    },
    "companies_house": { ... },
    "cdp": { ... },
    "gdelt": { ... },
    "layout_parser": { ... }
  }
}
```

## Testing

### Test with Example Tickers

**US Companies (EDGAR)**
```bash
python agent1_cli.py --ticker AAPL --email your@email.com  # Apple
python agent1_cli.py --ticker MSFT --email your@email.com  # Microsoft
python agent1_cli.py --ticker TSLA --email your@email.com  # Tesla
```

**UK Companies (Companies House)**
```bash
python agent1_cli.py --ticker BP --company "BP PLC" \
  --email your@email.com \
  --companies-house-key YOUR_KEY
```

**With PDF Extraction**
```bash
# Download BP 2023 Sustainability Report
wget https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/pdfs/sustainability/group-reports/bp-sustainability-report-2023.pdf

python agent1_cli.py --ticker BP \
  --company "BP PLC" \
  --pdf bp-sustainability-report-2023.pdf \
  --email your@email.com
```

## Error Handling

The system includes comprehensive error handling:

- **Network errors**: Retry logic and timeouts
- **Rate limiting**: Automatic delays between requests
- **Missing data**: Graceful fallbacks with informative messages
- **Invalid tickers**: Clear error messages
- **API failures**: Detailed error reporting

## Production Enhancements

For production deployment, consider:

1. **Enhanced PDF Processing**
   ```bash
   pip install layoutparser pdf2image pytesseract
   ```

2. **XBRL Parsing** (for better financial data extraction)
   ```bash
   pip install sec-edgar-downloader python-xbrl
   ```

3. **Caching** (to avoid repeated API calls)
   ```python
   pip install requests-cache
   ```

4. **Async Processing** (for better performance)
   ```python
   pip install aiohttp asyncio
   ```

## File Structure

```
agent1/
├── agent_1_fetchers.py    # Main implementation
├── agent1_cli.py          # Command-line interface
├── requirements.txt       # Dependencies
├── .env.example          # Configuration template
├── README.md             # This file
└── output/               # Results directory
    └── agent1_*.json     # Output files
```

## API Credentials

### SEC EDGAR
- **Required**: Email address in User-Agent header
- **Cost**: Free
- **Registration**: None required

### Companies House
- **Required**: API key
- **Cost**: Free
- **Registration**: [developer.company-information.service.gov.uk](https://developer.company-information.service.gov.uk)

### GDELT
- **Required**: None
- **Cost**: Free
- **Registration**: None required

### CDP
- **Required**: None (CSV download)
- **Cost**: Free
- **Registration**: None required

## Common Issues

### Issue: "SEC returns 403 Forbidden"
**Solution**: Ensure you're using a real email address in the User-Agent header

### Issue: "Companies House authentication failed"
**Solution**: Check your API key is correctly set in .env file

### Issue: "PDF extraction fails"
**Solution**: Ensure PyPDF2 is installed: `pip install PyPDF2`

### Issue: "Rate limit exceeded"
**Solution**: Increase the RATE_LIMIT_DELAY in .env

## License

Academic coursework submission - cfa AI Challenge rules.

## Acknowledgments

- SEC EDGAR API documentation
- Companies House API team
- CDP Open Data initiative
- GDELT Project
- LayoutParser library developers

---

**Status**: review code and do that overview doc

**Last Updated**: update this btw

**Tested With**:
- Python 3.8+
- Multiple S&P 500 tickers
- UK FTSE 100 companies
- BP 2023 Sustainability Report