from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.quality_checks import run_quality_checks


with DAG(
    dag_id="quality_checks",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["final_project", "quality", "checks"],
) as dag:

    run_quality_checks_task = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
    )
