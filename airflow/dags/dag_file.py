from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from sheet import sleep_health_sheet, pipeline

def run_dlt_pipeline():
    info = pipeline.run(sleep_health_sheet())
    print(info)

with DAG(
    dag_id="sleep_health_dlt",
    start_date=datetime(2024, 1, 1)
):
    run_task = PythonOperator(
        task_id="run_dlt",
        python_callable=run_dlt_pipeline
    )