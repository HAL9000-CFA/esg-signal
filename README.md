# ESG Signal

AI-powered ESG credibility briefing tool built for the CFA AI Investment Challenge 2025–2026.
Team HAL 9000, University of East Anglia.

ESG Signal analyses FTSE 100 and S&P 500 companies against publicly available regulatory data,
statutory filings, and job market signals to produce a per-factor credibility score, surfacing
where a company's ESG disclosures hold up and where they do not.

This submission represents the current working prototype. We have a clear roadmap toward a more useful (hopefully) contradiction-detection model tracking ESG commitments year-on-year, cross-referencing sustainability narratives against legally required risk disclosures, and ingesting material event filings as a ground-truth reality layer. See the [Roadmap](#roadmap) section for detail.

---

## Table of Contents

1. [Project Structure](#project-structure)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Environment Variables](#environment-variables)
5. [Running the Application](#running-the-application)
6. [Usage](#usage)
7. [Example Reports](#example-reports)
8. [Architecture Overview](#architecture-overview)
9. [Data Sources](#data-sources)
10. [Known Limitations](#known-limitations)
11. [Roadmap](#roadmap)
12. [Competition Rules Compliance](#competition-rules-compliance)
13. [AI Usage](#ai-usage)
14. [Development](#development)

---

## Project Structure

```
esg-signal/
├── agents/
│   ├── data_gathering.py           # Orchestrates all fetchers into a CompanyProfile
│   ├── relevance_filter.py         # SASB materiality lookup (Navigator API + SIC fallback)
│   ├── credibility_scorer.py       # 5-stream credibility scoring per material factor
│   ├── disclosure_checker.py       # Chunked LLM grading: QUANTIFIED / VAGUE / UNDISCLOSED
│   └── dcf_mapper.py               # Maps ESG factor scores to DCF line items and scenarios
│
├── pipeline/
│   ├── esg_signal_dag.py           # Airflow DAG wiring all agents
│   ├── llm_client.py               # Single entry point for all Claude / Gemini calls
│   ├── audit_log.py                # Append-only JSONL log for all LLM calls (tokens, cost)
│   ├── models.py                   # Shared dataclasses (CompanyProfile, FactorScore, etc.)
│   ├── validation_layer.py         # Deterministic post-scoring checks and flag correction
│   ├── words_vs_money.py           # Commitment extraction vs XBRL financials
│   ├── talent_signal.py            # Job posting scraper and scorer
│   └── fetchers/
│       ├── base.py                 # BaseFetcher: shared HTTP helpers
│       ├── base_regulatory.py      # BaseRegulatoryFetcher: save/load for regulatory CSVs
│       ├── edgar.py                # SEC EDGAR: 10-K / 20-F text + XBRL financials
│       ├── companies_house.py      # Companies House: iXBRL / PDF annual accounts
│       ├── pdf_extractor.py        # Gemini PDF text extraction (large doc fallback)
│       ├── sasb_topics.py          # SASB Navigator API: live GIC factor lists per industry
│       ├── ea_pollution.py         # UK Environment Agency Pollution Inventory
│       ├── eu_ets.py               # EU Emissions Trading Scheme verified emissions (EUTL)
│       ├── echo.py                 # US EPA ECHO enforcement, penalties, CAA/CWA/RCRA
│       ├── ghgrp.py                # US EPA GHGRP facility-level GHG emissions
│       ├── nrc.py                  # US NRC nuclear incident reports
│       ├── serp_jobs.py            # SerpAPI Google Jobs (primary talent signal source)
│       └── indeed_jobs.py          # Indeed RSS scraper (stale, kept for reference, using serp as SoT)
│
├── ui/
│   ├── app.py                      # Streamlit web interface
│   ├── components.py               # Reusable UI components (factor panels, badges, citations)
│   └── export.py                   # PDF (ReportLab) and JSON export
│
├── tests/
│   ├── fixtures/                   # Sample EDGAR responses, toy DCF, fixture data
│   ├── test_audit_log.py
│   ├── test_llm_client.py
│   ├── test_relevance_filter.py
│   ├── test_credibility.py
│   ├── test_words_vs_money.py
│   ├── test_validation_layer.py
│   ├── test_dcf_mapper.py
│   ├── test_talent_signal.py
│   ├── test_data_gathering.py
│   ├── test_pdf_extractor.py
│   ├── test_fetch_edgar.py
│   ├── test_fetch_companies_house.py
│   ├── test_fetch_ea_pollution.py
│   ├── test_fetch_eu_ets.py
│   ├── test_fetch_echo.py
│   ├── test_fetch_ghgrp.py
│   ├── test_fetch_nrc.py
│   └── test_fetch_sasb_topics.py
│
├── scripts/
│   ├── fetch_data.py               # Standalone data fetch script (outside Airflow)
│   └── init_airflow.sh             # Airflow initialisation helper
│
├── data/
│   ├── cache/                      # LLM response cache (keyed by payload hash, committed)
│   ├── sasb_map.json               # Finall fallback (after fetchers) SASB materiality map: 24 industries
│   └── audit_log.jsonl             # LLM call audit log (tokens, cost, cached flag)
│
├── config/
│   └── pricing.json                # Per-model token pricing for cost tracking
│
├── examples/
│   ├── esg_signal_BP.json          # Pre-run BP P.L.C. report
│   └── esg_signal_ULVR.json        # Pre-run Unilever PLC report
│
├── docs/
│   ├── cfa-ruleset.pdf             # CFA AI Investment Challenge official rules
│   └── concept-submission.pdf      # Round 1 concept submission
│
├── .github/
│   └── workflows/
│       └── ci.yml                  # CI: pytest + ruff on push/PR
│
├── docker-compose.yml
├── Dockerfile.airflow              # Custom Airflow image with pipeline dependencies
├── Dockerfile.streamlit            # Streamlit service image
├── Makefile                        # Shortcuts for common dev commands
├── pyproject.toml                  # Ruff and black configuration
├── requirements.txt                # Local dev and test dependencies
├── requirements-airflow.txt        # Docker-only
├── requirements-ui.txt             # Streamlit UI dependencies
├── .pre-commit-config.yaml         # Pre-commit hooks (ruff, black)
├── LICENSE
└── .env.example
```

---

## Prerequisites

The following must be installed before setup:

- **Python 3.11**, [python.org](https://www.python.org/downloads/)
- **Docker Desktop** (includes Docker Compose), [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop)
- **Git**, [git-scm.com](https://git-scm.com/)

Docker is required to run Airflow and Postgres. The Streamlit UI can be run either inside Docker or directly with Python.

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/HAL9000-CFA/esg-signal.git
cd esg-signal
```

### 2. Create and activate a virtual environment

```bash
python3.11 -m venv .venv
source .venv/bin/activate        # macOS / Linux
.venv\Scripts\activate           # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` and fill in all required keys. See [Environment Variables](#environment-variables) below for how to obtain each one.

### 5. Set directory permissions

The Airflow container runs as UID 50000. After cloning (or after any Docker volume reset), run:

```bash
chmod -R a+w data/
```

### 6. Initialise and start Docker services

Run the database migration and create the Airflow admin user (first time only):

```bash
docker compose run --rm airflow-init
```

Start all services:

```bash
docker compose up -d
```

Services will be available at:

| Service   | URL                   | Credentials   |
|-----------|-----------------------|---------------|
| Airflow   | http://localhost:8080 | admin / admin |
| Streamlit | http://localhost:8501 |,             |

---

## Environment Variables

Copy `.env.example` to `.env` and populate each value.

### Required for core pipeline

| Variable | Description | How to obtain |
|---|---|---|
| `ANTHROPIC_API_KEY` | Anthropic API key for Claude calls | [console.anthropic.com](https://console.anthropic.com/), create account, go to API Keys |
| `GOOGLE_API_KEY` | Google API key for Gemini (PDF extraction) | [aistudio.google.com](https://aistudio.google.com/), create project, generate API key |
| `SEC_EMAIL` | Email sent in the SEC EDGAR User-Agent header | Any valid email address, required by SEC fair-use policy. Fake addresses might be blocked. |

### Required for FTSE 100 companies

| Variable | Description | How to obtain |
|---|---|---|
| `COMPANIES_HOUSE_API_KEY` | Companies House REST API key | [developer.company-information.service.gov.uk](https://developer.company-information.service.gov.uk/), register a free account, create an application |

### Required for talent signal

| Variable | Description | How to obtain |
|---|---|---|
| `SERPAPI_KEY` | SerpAPI key for Google Jobs scraping | [serpapi.com](https://serpapi.com/), free tier provides 250 searches/month (requires email and mobile verification) |

### Optional / infrastructure

| Variable | Default | Description |
|---|---|---|
| `USE_CACHED` | `true` | Set to `true` to use cached LLM responses instead of live API calls. Recommended for reproducibility and cost control. |
| `CACHE_DIR` | `data/cache/` | Directory for LLM response cache files |
| `AUDIT_LOG_PATH` | `data/audit_log.jsonl` | Path for the LLM call audit log |
| `AIRFLOW__CORE__FERNET_KEY` | — | Fernet key for Airflow secret encryption. Generate with: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `AIRFLOW_URL` | `http://localhost:8080` | Airflow webserver URL — used by Streamlit to trigger DAGs. Default works for local Docker. |
| `AIRFLOW_USER` | `admin` | Airflow basic-auth username |
| `AIRFLOW_PASSWORD` | `admin` | Airflow basic-auth password |

The `SASB_API_BASE` entry in `.env.example` uses a public endpoint that requires no key. Leave it at its default value.

---

## Running the Application

### Starting services

```bash
docker compose up -d
```

### Activating the DAG in Airflow

The `esg_signal` DAG is **paused by default** when Airflow first starts. It must be manually unpaused before any analysis can run.

1. Open Airflow at [http://localhost:8080](http://localhost:8080) and log in with `admin` / `admin`
2. Find the `esg_signal` DAG in the DAGs list
3. Click the toggle switch on the left of the DAG row, **the DAG is active when the toggle is to the right and the button appears blue**. A grey toggle pointing left means the DAG is paused and will not run.

The Streamlit UI will show an error if the DAG is paused when you attempt to run an analysis.

### Useful operational commands

```bash
# Rebuild Streamlit after UI changes
docker compose build streamlit && docker compose up -d streamlit

# Restart the Airflow scheduler after editing a DAG
docker compose restart airflow-scheduler

# Stream logs for a service
docker compose logs -f airflow-scheduler

# Open a shell inside the Airflow container
docker compose exec airflow-webserver bash

# Full reset, removes the database and all volumes
docker compose down -v
```

---

## Usage

### Running a company analysis

1. Open [http://localhost:8501](http://localhost:8501) in your browser
2. Select the **Run analysis** tab
3. Enter the company's stock ticker (e.g. `BP` or `AAPL`)
4. Enter the company's full legal name (e.g. `BP p.l.c.` or `Apple Inc.`), this is used for filing searches and job posting queries. Use the registered legal name, not a shortened form.
5. Select the index: `SP500` or `FTSE100`
6. Optionally upload an analyst DCF model (`.xlsx`), only row labels are sent to the AI; financial figures remain on your machine
7. Click **Run Analysis**

The pipeline typically takes 3–8 minutes for new data, much less if it has been cached. A live task progress panel shows each stage as it completes. Results display automatically when the DAG finishes.

### Loading a previously exported report

1. Select the **Load previous report** tab
2. Upload a `.json` file exported from a previous ESG Signal run
3. The full report reloads instantly with no pipeline re-run or API calls

This is useful for reviewing or sharing results without incurring additional cost.

### Exporting results

From the results view, two export formats are available:

- **Download PDF**, formatted briefing report (ReportLab)
- **Download JSON**, full machine-readable output including pipeline trace, suitable for reloading or archiving

---

## Example Reports

Two pre-run example reports are included in the `examples/` directory. They can be loaded instantly via the **Load previous report** tab in the Streamlit UI without triggering a pipeline run or incurring any API cost.

| File | Company | Index | Filing type | Material factors |
|---|---|---|---|---|
| `examples/esg_signal_BP.json` | BP p.l.c. | FTSE 100 | 20-F (SEC, foreign private issuer) | 10 (Oil & Gas E&P) |
| `examples/esg_signal_ULVR.json` | Unilever PLC | FTSE 100 | 20-F (SEC, foreign private issuer) | 4 (Household & Personal Products) |

Both runs used `USE_CACHED=true` (zero live API cost at load time). The figures below reflect the cost of the original live runs that populated the cache, i.e., what a fresh uncached run of each company would cost.

### Cost breakdown by pipeline stage

**BP p.l.c.**, run `ui__BP__20260412T015910`, 157 calls, 841,784 tokens

| Stage | LLM calls | Tokens | Cost (USD) | Share |
|---|---|---|---|---|
| Disclosure quality grading | 68 | 383,765 | $2.56 | 48.5% |
| Words vs Money extraction | 79 | 449,456 | $2.66 | 50.4% |
| Narrative synthesis (×10 factors) | 10 | 8,563 | $0.06 | 1.1% |
| **Total** | **157** | **841,784** | **$5.28** | |

**Unilever PLC**, run `ui__ULVR__20260412T023707`, 100 calls, 543,128 tokens

| Stage | LLM calls | Tokens | Cost (USD) | Share |
|---|---|---|---|---|
| Disclosure quality grading | 48 | 263,818 | $1.46 | 50.2% |
| Words vs Money extraction | 48 | 275,640 | $1.43 | 49.0% |
| Narrative synthesis (×4 factors) | 4 | 3,670 | $0.02 | 0.8% |
| **Total** | **100** | **543,128** | **$2.91** | |

The dominant cost driver in both cases is document size, not factor count. Both companies file a 20-F with the SEC, BP's runs to 79 chunks versus Unilever's 48, reflecting a substantially larger filing. Disclosure grading and Words vs Money together account for approximately 99% of cost in both runs; narrative synthesis is negligible. Per-factor cost scales modestly: BP has 10 factors vs Unilever's 4, but the per-factor cost difference is small compared to the effect of document length on chunk count.

---

## Architecture Overview

The pipeline runs as an Airflow DAG with the following stages:

```
fetch_data
  → run_relevance_filter       (SASB materiality: Navigator API → SIC fallback)
  → run_disclosure_checker     (Stream 1: chunked LLM grading of annual filing)
  → run_words_vs_money         (Stream 4: commitment extraction vs XBRL financials)
  → run_talent                 (Stream 3: job posting signal via SerpAPI)
  → run_credibility_scorer     (5-stream weighted score per material factor)
  → run_validation_layer       (deterministic bounds and flag consistency checks)
  → run_dcf_mapper             (optional: map factor scores to DCF line items)
  → audit_summary              (aggregate token and cost summary)
```

### Scoring, 5 streams per material factor

| Stream | Weight | Source |
|---|---|---|
| Disclosure | 40% | Annual filing (chunked Claude grading) |
| Regulatory | 25% | EPA ECHO / GHGRP / EA Pollution / EU ETS / NRC |
| Talent | 15% | SerpAPI Google Jobs |
| Words vs Money | 10% | XBRL financials vs extracted commitments |
| Supply Chain | 10% | Derived from Supply Chain Management factor disclosure grade |

Score thresholds: green ≥ 0.80 (coverage ≥ 0.75), amber ≥ 0.40, red < 0.40.

All numerical scoring is computed in Python. LLMs are used only for text grading and narrative generation, never to produce scores directly.

---

## Data Sources

All sources are publicly accessible without paid subscriptions (SerpAPI has a free tier).

### Statutory filings

| Source | Coverage | What it provides |
|---|---|---|
| SEC EDGAR | US companies (10-K), foreign SEC filers (20-F) | Annual report text, XBRL financial statements |
| Companies House | UK companies | Annual accounts (iXBRL / PDF) |

### Regulatory and environmental

| Source | Coverage | What it provides |
|---|---|---|
| EPA ECHO | US companies | Enforcement actions, penalties, CAA / CWA / RCRA violations |
| EPA GHGRP | US companies | Facility-level GHG emissions (≥ 25,000 tonnes CO₂e/year, mandatory reporting) |
| US NRC | US nuclear operators | Incident and event reports |
| EA Pollution Inventory | UK companies | Facility-level pollutant releases (2013–2024) |
| EU ETS (EUTL) | EU-operating companies | Verified annual emissions from EU ETS installations |

### Industry materiality

| Source | What it provides |
|---|---|
| SASB Navigator API | Live GIC material factor lists per industry (public endpoint, no key required) |
| `data/sasb_map.json` | Static fallback covering 24 industries with financial impact mappings |

### Talent signal

| Source | What it provides |
|---|---|
| SerpAPI (Google Jobs) | ESG-related job postings per company (10 keyword queries per run) |

---

## Known Limitations

### Annual filing used as ESG document

The pipeline currently uses the SEC 10-K or 20-F as its primary document source. This is the legal filing, not a company's standalone sustainability or ESG report. For many companies, detailed ESG commitments, emissions targets, and social programmes are published in separate sustainability reports that are not fetched by the current pipeline. This means the Disclosure stream grades ESG content as it appears in legal filings, which is typically more compressed and cautious in wording than a dedicated sustainability report.

**Potential solution:** Add a sustainability report URL field to the run form. The existing `PDFExtractor` (Gemini-powered) can handle large PDFs, the only missing piece is the ingestion path.

### EA Pollution and EU ETS, subsidiary name matching

The EA Pollution Inventory and EUTL register facilities under legal subsidiary names (e.g. `BP Oil UK Limited`), not parent company names (`BP P.L.C.`). The pipeline uses word-boundary token matching, which works for most large FTSE 100 groups but will under-count subsidiaries that trade under a different brand.

**Potential solution:** Companies House group traversal (query all entities where the parent appears as a Person with Significant Control) or a manually maintained alias config at `data/ea_aliases.json`.

### Drift detection, not active in current pipeline

The `disclosure_checker.detect_drift()` function compares this year's disclosure grades against last year's to flag factors where reporting quality has changed. It is implemented and unit-tested but is not wired into the live pipeline. Two blockers remain:

1. `DataGatherer` fetches only the latest annual filing. Retrieving a prior-year report requires targeting an older EDGAR / Companies House URL.
2. Running disclosure grading twice roughly doubles the LLM cost per run. Grade stability across re-runs at temperature 0 also needs empirical verification before drift signals can be trusted.

### Talent signal, Indeed blocked

Indeed blocks RSS scraping (403). The pipeline falls back to SerpAPI (Google Jobs). If `SERPAPI_KEY` is not set or the free quota is exhausted, the talent stream is excluded from scoring rather than defaulting to a neutral score.

### SASB Navigator, short ticker unreliable

The SASB Navigator `companySearch` endpoint is unreliable for short tickers (e.g. `BP` matches unrelated companies). Industry resolution uses SIC code mapping via `data/sasb_map.json` as the reliable fallback. The Navigator is used for live factor lists once the industry code is known.

### PDF export — malformed output

The PDF export button is present but currently produces a malformed document. Use **Download JSON** instead, it captures the full report including pipeline trace and can be reloaded via the **Load previous report** tab with no loss of information.

---

## Roadmap

The following priorities have been identified for the next development stage, in order of impact:

**1. Sustainability report ingestion**
The most significant gap. Adding a sustainability report URL field (or automated discovery via SerpAPI) and feeding the document into the pipeline as a first-class input would give the disclosure stream a far richer and more appropriate source. The `PDFExtractor` already handles this document type.

**2. Commitment registry and year-on-year tracking**
Rather than treating each run in isolation, extracted commitments (targets, investment pledges, policy statements) should be stored in structured form per company and year. A comparison step would then diff these lists to surface commitments that were added, removed, quietly reworded, or had their deadlines extended. This is the primary mechanism for identifying greenwashing over time.

**3. EDGAR section extraction, risk factors**
The 10-K Item 1A and 20-F Item 3D sections contain legally required risk disclosures. Extracting these separately from the rest of the filing would enable a direct comparison between what a company says in its ESG narrative versus what it discloses as a material risk to its lawyers, a high-value contradiction signal.

**4. 8-K and 6-K material event ingestion**
8-K filings (US domestic) and 6-K filings (foreign private issuers) are filed within four business days of a material event: regulatory fines, environmental consent decrees, safety incidents, legal settlements. Ingesting these would provide a real-time ground truth layer to set against multi-year commitment claims.

**5. Cross-check output layer**
With the above in place, the output can evolve from a single credibility score toward a structured contradiction report: ESG narrative claims versus legal risk disclosures, and commitment history versus actual regulatory events. This is the end-state for the tool.

---

## Competition Rules Compliance

This section documents how ESG Signal meets the CFA AI Investment Challenge Rule 4 requirements.

### Approved AI models (Rule 4.3)

| Model | Provider | Use in pipeline |
|---|---|---|
| `claude-opus-4-5` | Anthropic | Disclosure grading, commitment extraction, narrative generation |
| `gemini-2.5-flash` | Google | PDF text extraction (large document fallback) |

Both models are on the approved list in Rule 4.3. No proprietary enterprise tools (Bloomberg, internal models) are used anywhere in the pipeline.

### Reproducibility, LLM response cache (Rule 4.4)

All LLM responses are cached to `data/cache/` immediately after each live call. Cache files are committed to the repository.

Cache keys are SHA-256 hashes of the full request payload (model, system prompt, user prompt, temperature). Any change to a prompt or parameter produces a cache miss automatically, there is no manual cache management.

To run the pipeline using cached responses only (zero API cost):

```bash
# In .env
USE_CACHED=true
```

With `USE_CACHED=true`, the pipeline replays all LLM calls from cache. No API keys are required and no cost is incurred. This is the recommended mode for judges reproducing results.

The `USE_CACHED` flag is the `--use-cached` mechanism referenced in Rule 4.4c.

### AI use disclosure, audit log (Rule 4.5)

Every LLM call is recorded to `data/audit_log.jsonl` by `pipeline/audit_log.py`. Each record includes:

- Timestamp and Airflow run ID
- Agent name and call purpose
- Model name and version
- Input tokens, output tokens, total tokens
- Cost in USD (computed from `config/pricing.json`)
- `cached: true/false` flag

To view a summary of all calls for a run:

```bash
python pipeline/audit_log.py <run_id>
# or for all runs:
python pipeline/audit_log.py
```

Token pricing is maintained in `config/pricing.json` and is updated when official pricing changes.

### Data sources, public accessibility (Rule 4.6)

All data sources used by ESG Signal are publicly accessible without paid subscriptions or institutional access:

| Source | Access |
|---|---|
| SEC EDGAR | Public API, email address in User-Agent header only |
| Companies House | Free API key (registration required, no payment) |
| EPA ECHO | Public REST API, no key required |
| EPA GHGRP | Public CSV download via EPA EnviroFacts, no key required |
| US NRC | Public CSV download, no key required |
| EA Pollution Inventory | Public XLSX download (GOV.UK), no key required |
| EU ETS (EUTL) | Public XLSX download (Union Registry Data Portal), no key required |
| SASB Navigator | Public AWS API Gateway endpoint, no key required |
| SerpAPI | Free tier: 250 searches/month, registration required |

The `data/sasb_map.json` materiality map is a derived dataset produced by the team using SASB Navigator data and is included in the repository.

---

## AI Usage

### AI as a component of the solution

The following models are called at runtime as part of the analysis pipeline:

| Model | Provider | Purpose |
|---|---|---|
| `claude-opus-4-5` | Anthropic | Disclosure quality grading, commitment extraction, narrative synthesis |
| `gemini-2.5-flash` | Google | PDF text extraction for large documents |

All calls go through `pipeline/llm_client.py`, which enforces caching, audit logging, and exponential backoff. LLMs are used exclusively for text classification and extraction tasks — all numerical scoring is computed deterministically in Python. LLM outputs are never used as scores directly.

Approximate cost per fresh (uncached) run: **$2–6 USD** depending on filing size. See the [Example Reports](#example-reports) section for a detailed breakdown of two real runs.

### AI used during development

Claude (Anthropic) and ChatGPT (OpenAI) were used throughout development as coding assistants and reflecting on report outputs for improving future iterations. This use is permitted under Rule 4.2.

---

## Development

### Running tests

```bash
pytest
```

All tests mock external APIs. No live API calls are made during the test suite.

### Linting and formatting

```bash
ruff check .
black .
```

### Common issues

**403 from SEC EDGAR**, The SEC blocks requests without a valid User-Agent. Ensure `SEC_EMAIL` in `.env` is a real, deliverable address.

**Companies House auth failure**, Ensure the key in `.env` has no surrounding quotes or spaces: `COMPANIES_HOUSE_API_KEY=abc123`, not `COMPANIES_HOUSE_API_KEY="abc123"`.

**PDF extraction failing**, Run `pip install pdfplumber` if the package is missing from the local environment.

**`pip` not found**, The virtual environment is not activated. Run `source .venv/bin/activate` (macOS/Linux) or `.venv\Scripts\activate` (Windows).

**PermissionError on `data/cache/` or `data/raw/`**, The Airflow container runs as UID 50000. Run `chmod -R a+w data/` from the repo root.
