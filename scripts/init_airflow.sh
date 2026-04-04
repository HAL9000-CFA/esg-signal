#!/bin/bash
set -e

pip install -r /opt/airflow/src/requirements-airflow.txt --quiet

airflow db migrate

airflow users create \
  --username admin \
  --password admin \
  --firstname ESG \
  --lastname Signal \
  --role Admin \
  --email admin@example.com
