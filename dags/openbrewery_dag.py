# dags/openbrewery_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys
import logging

sys.path.append("/opt/airflow/src")
from fetcher import run_fetch_and_persist
from transformer import run_transform

logger = logging.getLogger(__name__)

BRONZE_DIR = os.getenv("BRONZE_DIR", "/data/lake/bronze")
SILVER_DIR = os.getenv("SILVER_DIR", "/data/lake/silver")
GOLD_DIR = os.getenv("GOLD_DIR", "/data/lake/gold")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "marinalvinhaes@gmail.com")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": [ALERT_EMAIL],
    "email_on_failure": False,
    "email_on_retry": False,
}

def failure_callback(context):
    task = context.get('task_instance')
    msg = f"Task {task.task_id} failed in DAG {context['dag'].dag_id}"
    logger.error(msg)

with DAG(
    dag_id="openbrewery_medallion",
    default_args=default_args,
    start_date=datetime(2025,1,1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
) as dag:

    def task_fetch(**kwargs):
        # import inside callable so DAG file parses without requests/pandas
        return run_fetch_and_persist(bronze_dir=BRONZE_DIR)

    def task_transform(**kwargs):
        return run_transform(bronze_dir=BRONZE_DIR, silver_dir=SILVER_DIR, gold_dir=GOLD_DIR)

    fetch_task = PythonOperator(
        task_id="fetch_bronze",
        python_callable=task_fetch,
        retries=2,
        retry_delay=timedelta(minutes=2),
    )

    transform_task = PythonOperator(
        task_id="transform_silver_gold",
        python_callable=task_transform,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )

    fetch_task >> transform_task