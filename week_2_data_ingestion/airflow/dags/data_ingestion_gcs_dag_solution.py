import os
import logging

from datetime import datetime

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

dataset_file = "yellow_tripdata_2021-01.csv"
dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
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
    "depends_on_past": False,
    "retries": 1,
}

NEW_START_DATE = datetime(2019, 1, 1) #date of the starting data
NEW_SCHEDULE_INTERVAL = "0 6 2 * *" #Every second of month at 6:00 AM

YELLOW_TRIP_DATA_PREFIX = '/yellow_tripdata_'

TAXI_TRIP_DATA_URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data' 
TAXI_TRIP_URL_TEMPLATE = TAXI_TRIP_DATA_URL_PREFIX + YELLOW_TRIP_DATA_PREFIX + '{{ execution_date.strftime(\'%Y-%m\') }}.csv'
TAXI_TRIP_CSV_TEMPLATE = AIRFLOW_HOME + YELLOW_TRIP_DATA_PREFIX + '{{ execution_date.strftime(\'%Y-%m\') }}.csv'
TAXI_TRIP_PARQUET_TEMPLATE = AIRFLOW_HOME + YELLOW_TRIP_DATA_PREFIX + '{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
TAXI_GCS_TEMPLATE = "raw" + YELLOW_TRIP_DATA_PREFIX + "/{{ execution_date.strftime(\'%Y\') }}" + YELLOW_TRIP_DATA_PREFIX + "{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag_solution",
    schedule_interval=NEW_SCHEDULE_INTERVAL,
    start_date=NEW_START_DATE,
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {TAXI_TRIP_URL_TEMPLATE} > {TAXI_TRIP_CSV_TEMPLATE}"#add an f to fail in case dataset_url is not defined
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": TAXI_TRIP_CSV_TEMPLATE,
            "dest_file": TAXI_TRIP_PARQUET_TEMPLATE,
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": TAXI_GCS_TEMPLATE,
            "local_file": TAXI_TRIP_PARQUET_TEMPLATE,
        },
    )

    rm_task = BashOperator(
        task_id="rm_task",
        bash_command=f"rm {TAXI_TRIP_CSV_TEMPLATE} {TAXI_TRIP_PARQUET_TEMPLATE}"
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task
