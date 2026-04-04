from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _placeholder(**context):
    print("ESG Signal pipeline — stub task. Real logic added in issue #14.")


with DAG(
    dag_id="esg_signal",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["esg-signal"],
) as dag:

    placeholder = PythonOperator(
        task_id="placeholder",
        python_callable=_placeholder,
    )
