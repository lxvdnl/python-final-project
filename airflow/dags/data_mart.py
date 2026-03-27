
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.datamart import run_orders_datamart


with DAG(
    dag_id="orders_datamart",
    start_date=datetime(2024, 1, 1),
    schedule=None, 
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["datamart", "spark"],
) as dag:

    build_orders_datamart = PythonOperator(
        task_id="build_orders_datamart",
        python_callable=run_orders_datamart,
    )