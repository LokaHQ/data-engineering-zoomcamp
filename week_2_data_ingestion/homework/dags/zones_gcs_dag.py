import os

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import misc

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_name = "taxi+_zone_lookup"
dataset_file = f"{dataset_name}.csv"
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/misc/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace(".csv", ".parquet")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
        dag_id="zones_gcs_dag",
        schedule_interval="@once",
        start_date=datetime(year=2022, month=1, day=1),
        default_args=default_args,
        max_active_runs=1,
        tags=["dtc-de"],
) as dag:
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}",
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=misc.format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=misc.upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": "raw/zones.parquet",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )
    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task
