import logging
import os
from datetime import datetime

import pyarrow.csv as pv
import pyarrow.parquet as pq
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'


def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv'):
        logging.error(
            "Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, dest_file)


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
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    # "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

def donwload_parquetize_upload_dag(
    dag,
    url_template,
    local_csv_path_template,
    local_parquet_path_template,
    gcs_path_template
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSL {url_template} > {local_csv_path_template}"
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": local_csv_path_template,
                "dest_file": local_parquet_path_template
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_path_template,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_csv_path_template} {local_parquet_path_template}"
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task

def donwload_unzip_parquetize_upload_dag(
    dag,
    url_template,
    local_csv_path_template,
    local_parquet_path_template,
    gcs_path_template
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {url_template} > {local_csv_path_template}.gz"
        )

        unzip_dataset_task = BashOperator(
            task_id="unzip_dataset_task",
            bash_command=f"gzip -d --force {local_csv_path_template}.gz > {local_csv_path_template}"
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": local_csv_path_template,
                "dest_file": local_parquet_path_template
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_path_template,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_csv_path_template} {local_parquet_path_template}"
        )

        download_dataset_task >> unzip_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task


# YELLOW_TAXI DAG

YELLOW_TAXI_URL_TEMPLATE = URL_PREFIX + '/yellow/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
YELLOW_TAXI_CSV_FILE_TEMPLATE = AIRFLOW_HOME + \
    '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
YELLOW_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + \
    '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_GCS_PATH_TEMPLATE = "raw/yellow_tripdata/{{ execution_date.strftime(\'%Y\') }}/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

yellow_taxi_data_dag = DAG(
    dag_id="yellow_taxi_data_v3",
    # At 06:00 on day-of-month 2.
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 7, 3),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
)

donwload_unzip_parquetize_upload_dag(
    dag=yellow_taxi_data_dag,
    url_template=YELLOW_TAXI_URL_TEMPLATE,
    local_csv_path_template=YELLOW_TAXI_CSV_FILE_TEMPLATE,
    local_parquet_path_template=YELLOW_TAXI_PARQUET_FILE_TEMPLATE,
    gcs_path_template=YELLOW_TAXI_GCS_PATH_TEMPLATE
)

# GREEN_TAXI DAG

GREEN_TAXI_URL_TEMPLATE = URL_PREFIX + '/green/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
GREEN_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + \
    '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_TAXI_CSV_FILE_TEMPLATE = AIRFLOW_HOME + \
    '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
GREEN_TAXI_GCS_PATH_TEMPLATE = "raw/green_tripdata/{{ execution_date.strftime(\'%Y\') }}/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

green_taxi_data_dag = DAG(
    dag_id="green_taxi_data_v2",
    # At 07:00 on day-of-month 2.
    schedule_interval="0 7 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 7, 3),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

donwload_unzip_parquetize_upload_dag(
    dag=green_taxi_data_dag,
    url_template=GREEN_TAXI_URL_TEMPLATE,
    local_csv_path_template=GREEN_TAXI_CSV_FILE_TEMPLATE,
    local_parquet_path_template=GREEN_TAXI_PARQUET_FILE_TEMPLATE,
    gcs_path_template=GREEN_TAXI_GCS_PATH_TEMPLATE
)

# FHV_TAXI DAG

FHV_TAXI_URL_TEMPLATE = URL_PREFIX + '/fhv/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
FHV_TAXI_CSV_FILE_TEMPLATE = AIRFLOW_HOME + \
    '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
FHV_TAXI_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + \
    '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_TAXI_GCS_PATH_TEMPLATE = "raw/fhv_tripdata/{{ execution_date.strftime(\'%Y\') }}/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

fhv_taxi_data_dag = DAG(
    dag_id="fhv_taxi_data_v5",
    # At 08:00 on day-of-month 2.
    schedule_interval="0 8 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 7, 3),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
)

donwload_unzip_parquetize_upload_dag(
    dag=fhv_taxi_data_dag,
    url_template=FHV_TAXI_URL_TEMPLATE,
    local_csv_path_template=FHV_TAXI_CSV_FILE_TEMPLATE,
    local_parquet_path_template=FHV_TAXI_PARQUET_FILE_TEMPLATE,
    gcs_path_template=FHV_TAXI_GCS_PATH_TEMPLATE
)


# ZONES DAG

ZONES_URL_TEMPLATE = URL_PREFIX +'/misc/taxi_zone_lookup.csv'
ZONES_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/taxi_zone_lookup.csv'
ZONES_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + '/taxi_zone_lookup.parquet'
ZONES_GCS_PATH_TEMPLATE = "raw/taxi_zone/taxi_zone_lookup.parquet"


zones_data_dag = DAG(
    dag_id="zones_data_v1",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

donwload_parquetize_upload_dag(
    dag=zones_data_dag,
    url_template=ZONES_URL_TEMPLATE,
    local_csv_path_template=ZONES_CSV_FILE_TEMPLATE,
    local_parquet_path_template=ZONES_PARQUET_FILE_TEMPLATE,
    gcs_path_template=ZONES_GCS_PATH_TEMPLATE
)
