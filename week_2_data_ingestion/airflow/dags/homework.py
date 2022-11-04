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
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
FILE_NAME_TEMPLATE = "{{ execution_date.strftime(\'%Y-%m\') }}.parquet"


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error(
            "Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


def upload_to_gcs(bucket, object_name, local_file):
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "retries": 1,
}


def download_bash_operator(url_template, local_csv_path_template):
    return BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {url_template} > {local_csv_path_template}"
    )


def to_gcs_python_operator(gcs_path_template, local_parquet_path_template):
    return PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_path_template,
        },
    )


def rm_bash_operator(local_parquet_path_template):
    return BashOperator(
        task_id="rm_task",
        bash_command=f"rm {local_parquet_path_template}"
    )


def get_url(dataset_id):
    url_string = URL_PREFIX + f"{dataset_id}_tripdata_"
    return url_string + FILE_NAME_TEMPLATE


def get_local_path(dataset_id):
    path_string = AIRFLOW_HOME + f"/{dataset_id}_tripdata_"
    return path_string + FILE_NAME_TEMPLATE


def get_gcs_path(dataset_id):
    first_half_path_string = f"raw/{dataset_id}_tripdata/"
    second_half_path_string = f"/{dataset_id}_tripdata_"
    return first_half_path_string + "{{ execution_date.strftime(\'%Y\') }}" + second_half_path_string + FILE_NAME_TEMPLATE


def execute_dag(
    dag,
    url_template,
    local_path_template,
    gcs_path_template
):
    with dag:
        # TODO: Integrate zones better with other DAG's operators
        if "zones_data" in dag.dag_id:
            local_csv_path_template = local_path_template.replace(
                '.parquet', '.csv')
            download_dataset_task = BashOperator(
                task_id="download_dataset_task",
                bash_command=f"curl -sSLf {url_template} > {local_csv_path_template}"
            )

            format_to_parquet_task = PythonOperator(
                task_id="format_to_parquet_task",
                python_callable=format_to_parquet,
                op_kwargs={
                    "src_file": local_csv_path_template,
                    "dest_file": local_path_template
                },
            )

            local_to_gcs_task = to_gcs_python_operator(
                gcs_path_template, local_path_template)

            rm_task = BashOperator(
                task_id="rm_task",
                bash_command=f"rm {local_csv_path_template} {local_path_template}"
            )

            download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task
        else:
            download_dataset_task = download_bash_operator(
                url_template, local_path_template)
            local_to_gcs_task = to_gcs_python_operator(
                gcs_path_template, local_path_template)
            rm_task = rm_bash_operator(local_path_template)

            download_dataset_task >> local_to_gcs_task >> rm_task


# Define DAGs
yellow_taxi_dag = DAG(
    dag_id="yellow_taxi_data",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

green_taxi_dag = DAG(
    dag_id="green_taxi_data_v6",
    schedule_interval="0 7 2 * *",
    start_date=datetime(2019, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

fhv_taxi_dag = DAG(
    dag_id="fhv_taxi_data_v3",
    schedule_interval="0 8 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

zones_dag = DAG(
    dag_id="zones_data_v7",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

# Execute DAGs
execute_dag(
    dag=yellow_taxi_dag,
    url_template=get_url("yellow"),
    local_path_template=get_local_path("yellow"),
    gcs_path_template=get_gcs_path("yellow")
)

execute_dag(
    dag=green_taxi_dag,
    url_template=get_url("green"),
    local_path_template=get_local_path("green"),
    gcs_path_template=get_gcs_path("green")
)

execute_dag(
    dag=fhv_taxi_dag,
    url_template=get_url("fhv"),
    local_path_template=get_local_path("fhv"),
    gcs_path_template=get_gcs_path("fhv")
)

execute_dag(
    dag=zones_dag,
    url_template="https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv",
    local_path_template=AIRFLOW_HOME + '/taxi_zone_lookup.parquet',
    gcs_path_template="raw/taxi_zone/taxi_zone_lookup.parquet"
)

# Week 2 Homework

# Question 1: Start date for the Yellow taxi data

# R: 2019-01-01 - datetime(2019, 1, 1)

# Question 2: Frequency for the Yellow taxi data

# R: Monthly

# Question 3: DAG for FHV Data

# R: 12

# Question 4: DAG for Zones

# R: Once
