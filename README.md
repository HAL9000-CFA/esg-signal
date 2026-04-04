# ESG Signal

AI ESG credibility tool built for the CFA AI Investment Challenge 2026.

---

## Project Structure

```
esg-signal/
├── agents/                   # Core analytical pipeline
│   ├── data_gathering.py     # SEC EDGAR + Companies House fetchers
│   ├── relevance_filter.py   # SASB materiality map filter
│   ├── credibility_scorer.py # 5-stream credibility scoring
│   └── dcf_mapper.py         # DCF Excel line item mapper
│
├── pipeline/                 # Airflow orchestration
│   ├── esg_signal_dag.py     # Main DAG wiring all agents
│   ├── llm_client.py         # Single entry point for all Claude calls
│   ├── audit_log.py          # Per-call LLM audit log (tokens, cost)
│   ├── validation_layer.py   # Deterministic checks on all outputs
│   └── talent_signal.py      # Job posting scraper and scorer
│
├── ui/                       # Streamlit web app
│   ├── app.py                # Main entrypoint
│   ├── components.py         # Confidence badge widgets
│   └── export.py             # PDF + JSON export
│
├── data/
│   ├── raw/                  # Source downloads (gitignored)
│   ├── processed/            # Cleaned datasets (gitignored)
│   ├── cache/                # LLM response cache (committed)
│   └── sasb_map.json         # SASB materiality fixture
│
├── tests/
│   ├── fixtures/             # Sample EDGAR responses, toy DCF
│   ├── test_data_gathering.py
│   ├── test_credibility.py
│   └── test_dcf_mapper.py
│
├── docker-compose.yml
├── Dockerfile.streamlit
├── pyproject.toml
├── requirements.txt
└── .env.example
```

---

## Getting Started

### 1. Clone repo

```bash
git clone https://github.com/HAL9000-CFA/esg-signal.git
# or ssh
git clone git@github.com:HAL9000-CFA/esg-signal.git
cd esg-signal
```

### 2. Set up your environment file

```bash
cp .env.example .env
```

Keys vars are placeholders atm, they'll probs be changed as we go but how to obtain them should be written here when they are first implemented and changed.

- `COMPANIES_HOUSE_API_KEY` - https://developer.company-information.service.gov.uk

### 3. Start everything with Docker

```bash
# First time only, migrates the database and creates the Airflow admin user
docker compose run --rm airflow-init

# Start all services
docker compose up -d
```

This should host the services on ports:

| Service   | URL                   | Login         |
| --------- | --------------------- | ------------- |
| Airflow   | http://localhost:8080 | admin / admin |
| Streamlit | http://localhost:8501 | -             |

### 4. Install requirements and run the tests

```bash
pip install -r requirements.txt
pytest
```

---

## Making Contributions

1. Pull latest `main`
2. Create branches as `issue/<number>-short-description` (or else 5 lashes)
3. Pre-commit hooks run automatically on `git commit` (it'll handle formatting)
4. Open a pull request into `main` when done, CI runs pytest and ruff
5. Ask another team member to review before merge

---

## Day-to-day commands (according to claude)

# Start all services

```bash
docker compose up -d
```

```bash
# Rebuild Streamlit after UI changes
docker compose build streamlit && docker compose up -d streamlit

# Restart the Airflow scheduler after editing a DAG
docker compose restart airflow-scheduler

# View logs for a service
docker compose logs -f airflow-scheduler

# Shell into the Airflow container
docker compose exec airflow-webserver bash

# Full reset (wipes the database)
docker compose down -v
```

---

## Common Issues

### Data Gatherer

**403 error from SEC** - use a real email address, they block fake ones

**Companies House auth failed** - check the API key in your .env has no extra spaces

**PDF extraction failing** - run `pip install PyPDF2`

**pip not recognised** - venv isnt activated, run `venv\Scripts\activate` first

---

## Agent Overviews

## Data Gatherer

Agent: /agents/data_gathering.py
Test: /tests/test_data_gathering.py

### Output Shape

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

Status values: `success`, `partial` (data returned but incomplete), `failed`.

### Data Sources

| Source          | Base URL                                         | Key required             | Rate limit   | What it provides                                    |
| --------------- | ------------------------------------------------ | ------------------------ | ------------ | --------------------------------------------------- |
| SEC EDGAR       | `https://data.sec.gov`                           | No (email in User-Agent) | 10 req/s     | 10-K filings, risk factors, financial statements    |
| Companies House | `https://api.company-information.service.gov.uk` | Yes (free)               | 600 req/5min | Confirmation statements, SIC codes, filing history  |
| CDP             | `https://www.cdp.net/en/data`                    | No (CSV download)        | —            | Climate and water disclosure responses, Scope 1/2/3 |
| GDELT           | `https://api.gdeltproject.org/api/v2`            | No                       | —            | News articles, events, sentiment by company         |
| LayoutParser    | local                                            | —                        | —            | Structured text extraction from sustainability PDFs |
