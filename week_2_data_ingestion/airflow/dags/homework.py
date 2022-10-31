import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
    

def generate_taxt_data_url(year, month):
    dataset_file = f"yellow_tripdata_{year}-{month}.parquet"
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    return base_url + dataset_file

def build_gcp_path():
    return "raw/yellow_tripdata/{{ execution_date.strftime(\'%Y\') }}/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

def build_local_path():
    return AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'


def upload_to_gcs(bucket, object_name, local_file):
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="homework_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    dataset_url = generate_taxt_data_url('execution_date.strftime(\'%Y\')', 'execution_date.strftime(\'%m\')')
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {build_local_path()}"
    )
    
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": build_gcp_path(),
            "local_file": build_local_path(),
        },
    )

    download_dataset_task >> local_to_gcs_task
