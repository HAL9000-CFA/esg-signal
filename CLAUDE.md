# ESG Signal — Claude Code Context

## Project

CFA AI Investment Challenge 2025-2026. Team HAL 9000, University of East Anglia.
AI-powered ESG credibility briefing tool for FTSE 100 and S&P 500 companies.
Full rules in docs/cfa-ruleset.pdf, concept in docs/concept-submission.pdf.

## Stack

Python 3.11, Apache Airflow 2.9.3 (Docker only), Streamlit, Postgres (Docker only),
Anthropic Claude Opus 4.5, Google Gemini 1.5 Pro, pytest, ruff, black.

## Repo structure

agents/data_gathering.py — DataGatherer class, EDGARFetcher, CompaniesHouseFetcher,
CDPFetcher, GDELTFetcher, LayoutParserExtractor
pipeline/audit_log.py — JSONL audit log for all LLM calls (issues #3/#4, COMPLETE)
pipeline/llm_client.py — single entry point for all Claude calls (issues #3/#4, COMPLETE)
pipeline/esg_signal_dag.py — Airflow DAG, currently stub only
pipeline/validation_layer.py — not yet built
pipeline/talent_signal.py — not yet built
ui/app.py — Streamlit stub only
ui/components.py — not yet built
ui/export.py — not yet built
agents/relevance_filter.py — not yet built
agents/credibility_scorer.py — not yet built
agents/dcf_mapper.py — not yet built
data/cache/ — committed LLM response cache (USE_CACHED=true to use)
data/sasb_map.json — not yet built
scripts/init_airflow.sh — Docker init script
scripts/fetch_cli.py — CLI for data_gathering
tests/test_audit_log.py — COMPLETE, all passing
tests/test_data_gathering.py — partial, live API tests skipped with @pytest.mark.skipif
tests/test_credibility.py — placeholder only
tests/test_dcf_mapper.py — placeholder only

## Environment

Copy .env.example to .env and fill in:
ANTHROPIC_API_KEY, COMPANIES_HOUSE_API_KEY, GOOGLE_API_KEY, SEC_EMAIL, CONTACT_EMAIL
USE_CACHED=true to use cached LLM responses instead of live API calls.

## Docker

docker compose run --rm airflow-init # first time only
docker compose up -d # start all services
Airflow: localhost:8080 (admin/admin)
Streamlit: localhost:8501
requirements.txt — local dev and tests
requirements-airflow.txt — Docker only (apache-airflow==2.9.3)

## Key conventions

- All LLM calls go through pipeline/llm_client.py call_claude() — never call anthropic directly
- All numerical outputs computed in Python, never by LLM
- Each agent returns structured data, never prints results
- Tests never hit live APIs — mock with unittest.mock
- Run tests: pytest
- Lint: ruff check . and black .
- Always run from repo root

## Issue progress

## Agent pipeline flow

fetch_company_profile(ticker) — agents/data_gathering.py
→ CompanyProfile
→ relevance_filter(CompanyProfile) — agents/relevance_filter.py
→ [material ESG factors]
→ credibility_scorer(CompanyProfile, factors) — agents/credibility_scorer.py
→ {factor: {score, flag, evidence, sources}}
→ dcf_mapper(scores, excel_path) — agents/dcf_mapper.py (optional)
→ {factor: {dcf_line, scenario_range}}
→ Streamlit renders briefing

## Scoring (competition rubric)

Round 2 code review: Functionality 40pts, Clarity/docs 30pts, Path to completion 30pts
Round 3 final: Delivery 50pts, Responsible AI 20pts, Innovation/Relevance/Impact 10pts each
Approved models: claude-opus-4-5, GPT-5.2, Gemini. No Bloomberg or enterprise-only tools.
All data must be publicly accessible. Reproduction cost must stay under $20 or cached.

## Pick up from:

❯ 52c6b9eee270
 **_ Found local files:
 _** \* /opt/airflow/logs/dag_id=esg_signal/run_id=ui**AAPL**20260409T094440/task_id=fetch_data/attempt=1.log
 [2026-04-09, 09:44:41 UTC] {local_task_job_runner.py:120} ▼ Pre task execution logs
 [2026-04-09, 09:44:41 UTC] {taskinstance.py:2076} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: esg_signal.fetch_data ui**AAPL**20260409T094440 [queued]>
 [2026-04-09, 09:44:41 UTC] {taskinstance.py:2076} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: esg_signal.fetch_data ui**AAPL**20260409T094440 [queued]>
 [2026-04-09, 09:44:41 UTC] {taskinstance.py:2306} INFO - Starting attempt 1 of 4
 [2026-04-09, 09:44:41 UTC] {taskinstance.py:2330} INFO - Executing <Task(PythonOperator): fetch_data> on 2026-04-09 09:44:40.622449+00:00
 [2026-04-09, 09:44:41 UTC] {standard_task_runner.py:64} INFO - Started process 184 to run task
 [2026-04-09, 09:44:41 UTC] {standard_task_runner.py:90} INFO - Running: ['***', 'tasks', 'run', 'esg_signal', 'fetch_data', 'ui__AAPL__20260409T094440', '--job-id', '15', '--raw', '--subdir',
 'DAGS_FOLDER/esg_signal_dag.py', '--cfg-path', '/tmp/tmpcfi011ao']
 [2026-04-09, 09:44:41 UTC] {standard_task_runner.py:91} INFO - Job 15: Subtask fetch_data
 [2026-04-09, 09:44:41 UTC] {task_command.py:426} INFO - Running <TaskInstance: esg_signal.fetch_data ui**AAPL**20260409T094440 [running]> on host 52c6b9eee270
 [2026-04-09, 09:44:42 UTC] {taskinstance.py:2648} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='esg-signal' AIRFLOW_CTX_DAG_ID='esg_signal' AIRFLOW_CTX_TASK_ID='fetch_data'
 AIRFLOW_CTX_EXECUTION_DATE='2026-04-09T09:44:40.622449+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='ui**AAPL**20260409T094440'
 [2026-04-09, 09:44:42 UTC] {taskinstance.py:430} ▲▲▲ Log group end
 [2026-04-09, 09:44:42 UTC] {taskinstance.py:441} ▼ Post task execution logs
 [2026-04-09, 09:44:42 UTC] {taskinstance.py:2905} ERROR - Task failed with exception
 Traceback (most recent call last):
 File "/home/airflow/.local/lib/python3.11/site-packages/airflow/models/taskinstance.py", line 460, in \_execute_task
 result = \_execute_callable(context=context, **execute_callable_kwargs)
 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 File "/home/airflow/.local/lib/python3.11/site-packages/airflow/models/taskinstance.py", line 432, in \_execute_callable
 return execute_callable(context=context, **execute_callable_kwargs)
 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 File "/home/airflow/.local/lib/python3.11/site-packages/airflow/models/baseoperator.py", line 401, in wrapper
 return func(self, *args, \*\*kwargs)
 ^^^^^^^^^^^^^^^^^^^^^^^^^^^
 File "/home/airflow/.local/lib/python3.11/site-packages/airflow/operators/python.py", line 235, in execute
 return_value = self.execute_callable()
 ^^^^^^^^^^^^^^^^^^^^^^^
 File "/home/airflow/.local/lib/python3.11/site-packages/airflow/operators/python.py", line 252, in execute_callable
 return self.python_callable(*self.op_args, \*\*self.op_kwargs)
 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 File "/opt/airflow/dags/esg_signal_dag.py", line 193, in \_task_fetch_data
 from agents.data_gathering import DataGatherer
 File "/opt/airflow/src/agents/data_gathering.py", line 13, in <module>
 from pipeline.fetchers.pdf_extractor import PDFExtractor
 File "/opt/airflow/src/pipeline/fetchers/pdf_extractor.py", line 16, in <module>
 import pdfplumber
 ModuleNotFoundError: No module named 'pdfplumber'
 [2026-04-09, 09:44:42 UTC] {taskinstance.py:1206} INFO - Marking task as UP_FOR_RETRY. dag_id=esg_signal, task_id=fetch_data, run_id=ui**AAPL**20260409T094440, execution_date=20260409T094440,
 start_date=20260409T094441, end_date=20260409T094442
 [2026-04-09, 09:44:42 UTC] {standard_task_runner.py:110} ERROR - Failed to execute job 15 for task fetch_data (No module named 'pdfplumber'; 184)
 [2026-04-09, 09:44:42 UTC] {local_task_job_runner.py:243} INFO - Task exited with return code 1
 [2026-04-09, 09:44:42 UTC] {taskinstance.py:3503} INFO - 0 downstream tasks scheduled from follow-on schedule check
 [2026-04-09, 09:44:42 UTC] {local_task_job_runner.py:222} ▲▲▲ Log group end

Read 1 file (ctrl+o to expand)

● The Airflow container only has apache-airflow installed — none of the pipeline dependencies. They need to be added to requirements-airflow.txt and the container needs to install them on startup.

Read 1 file (ctrl+o to expand)

● The init_airflow.sh runs pip install but only during airflow-init — the webserver and scheduler containers start directly with command: webserver/scheduler without installing anything. Need a custom
Airflow Dockerfile so packages are baked in at build time.

● Write(Dockerfile.airflow)
⎿  Wrote 7 lines to Dockerfile.airflow
1 FROM apache/airflow:2.9.3-python3.11
2
3 # Install pipeline dependencies into the Airflow image.
4 # apache-airflow itself is already in the base image — only add extras here.
5 COPY requirements-airflow.txt /requirements-airflow.txt
6 USER airflow
7 RUN pip install --no-cache-dir -r /requirements-airflow.txt
⎿  You've hit your limit · resets 12pm (Europe/London)
/upgrade to increase your usage limit.
