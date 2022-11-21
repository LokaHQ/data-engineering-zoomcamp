import os

from datetime import datetime

from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable
from format_to_csv import format_to_csv


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")


def download_format_to_csv_ingest(
    dag, source_url, output_file_parquet, output_file_csv, table_name
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {source_url} > {output_file_parquet}",
        )

        format_to_csv_task = PythonOperator(
            task_id="format_to_csv_task",
            python_callable=format_to_csv,
            op_kwargs={
                "src_file": output_file_parquet,
                "output_file": output_file_csv,
            },
        )

        ingest_task = PythonOperator(
            task_id="ingest",
            python_callable=ingest_callable,
            op_kwargs=dict(
                user=PG_USER,
                password=PG_PASSWORD,
                host=PG_HOST,
                port=PG_PORT,
                db=PG_DATABASE,
                table_name=table_name,
                csv_file=output_file_csv,
            ),
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {output_file_parquet} {output_file_csv}",
        )

        # Set the task dependencies
        download_dataset_task >> format_to_csv_task >> ingest_task >> rm_task


# YELLOW TAXI DATA
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
URL_TEMPLATE = (
    URL_PREFIX + "/yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
)
OUTPUT_FILE_TEMPLATE = (
    AIRFLOW_HOME + "/output_{{ execution_date.strftime('%Y-%m') }}.parquet"
)
OUTPUT_FILE_TEMPLATE_CSV = (
    AIRFLOW_HOME + "/output_{{ execution_date.strftime('%Y-%m') }}.parquet"
)
TABLE_NAME_TEMPLATE = "yellow_taxi_{{ execution_date.strftime('%Y_%m') }}"

local_ingestion_taxi_data = DAG(
    "LocalIngestionYellowTaxiV2",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    catchup=True,
    max_active_runs=1,
)

"""
download_format_to_csv_ingest(
    local_ingestion_taxi_data,
    URL_TEMPLATE,
    OUTPUT_FILE_TEMPLATE,
    OUTPUT_FILE_TEMPLATE_CSV,
    TABLE_NAME_TEMPLATE,
)
"""

# FHV DATA
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
URL_TEMPLATE = (
    URL_PREFIX + "/fhv_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
)
OUTPUT_FILE_TEMPLATE = (
    AIRFLOW_HOME + "/output_{{ execution_date.strftime('%Y-%m') }}.parquet"
)
OUTPUT_FILE_TEMPLATE_CSV = (
    AIRFLOW_HOME + "/output_{{ execution_date.strftime('%Y-%m') }}.csv"
)
TABLE_NAME_TEMPLATE = "fhv_{{ execution_date.strftime('%Y_%m') }}"


local_ingestion_fhv_data = DAG(
    "LocalIngestionFhvV2",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 2, 1),
    end_date=datetime(2020, 1, 1),
    catchup=True,
    max_active_runs=3,
)

download_format_to_csv_ingest(
    local_ingestion_fhv_data,
    URL_TEMPLATE,
    OUTPUT_FILE_TEMPLATE,
    OUTPUT_FILE_TEMPLATE_CSV,
    TABLE_NAME_TEMPLATE,
)


# Zones DATA
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/misc"
URL_TEMPLATE = URL_PREFIX + "/taxi+_zone_lookup.csv"
OUTPUT_FILE_TEMPLATE_CSV = AIRFLOW_HOME + "/taxi+_zone_lookup.csv"
TABLE_NAME_TEMPLATE = "zone"


local_ingestion_fhv_data = DAG(
    "LocalIngestionZones",
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=True,
    max_active_runs=3,
)

download_format_to_csv_ingest(
    local_ingestion_fhv_data,
    URL_TEMPLATE,
    OUTPUT_FILE_TEMPLATE_CSV,
    OUTPUT_FILE_TEMPLATE_CSV,
    TABLE_NAME_TEMPLATE,
)
