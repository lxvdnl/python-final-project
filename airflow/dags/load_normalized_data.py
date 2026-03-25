from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.load_normalized import run_load


with DAG(
    dag_id="load_normalized_data",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["normalized"],
) as dag:

    load_normalized = PythonOperator(
        task_id="load_normalized",
        python_callable=run_load,
    )
