import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

from ingestion_dag_functions import ingest_gcs_callable, commit_file, format_to_parquet


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "airlines_data_all")


with DAG(
    "IngestionDAG",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    max_active_runs=2,
    tags=["zoom-camp"],
    params={
        "csv_file_download_url": Param(default="", type="string"),
        "year": Param(default="", type="string"),  # Example 1998
    },
    # {"csv_file_download_url":"https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/IXITH2","year":"1987"}
) as dag:

    wget_task = BashOperator(
        task_id="download-data",
        bash_command="curl -sSLf $csv_file_download_url "
        "--output {AIRFLOW_HOME}/$output_file".format(AIRFLOW_HOME=AIRFLOW_HOME),
        env={
            "csv_file_download_url": "{{ params.csv_file_download_url }}",
            "output_file": "airlines_{{ params.year }}.csv.bz2",
        },
    )

    bzip_task = BashOperator(
        task_id="unzip-file",
        bash_command="bzip2 -d $airflow_home/airlines_{{params.year}}.csv.bz2",
        env={"airflow_home": AIRFLOW_HOME},
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "csv_file": "airlines_{{ params.year }}.csv",
        },
    )

    upload_task_csv = PythonOperator(
        task_id="upload-to-gcs-csv",
        python_callable=ingest_gcs_callable,
        op_kwargs=dict(
            bucket=BUCKET,
            object_name="raw/csv/airlines_{{ params.year }}.csv",
            file_to_upload="airlines_{{ params.year }}.csv",
        ),
    )

    upload_task_parquet = PythonOperator(
        task_id="upload-to-gcs-parquet",
        python_callable=ingest_gcs_callable,
        op_kwargs=dict(
            bucket=BUCKET,
            object_name="raw/parquet/airlines_{{ params.year }}.parquet",
            file_to_upload="airlines_{{ params.year }}.parquet",
        ),
    )

    cleanup_task = BashOperator(
        task_id="cleanup",
        bash_command="rm -f $airflow_home/airlines_{{params.year}}.csv $airflow_home/airlines_{{params.year}}.parquet",
        env={"airflow_home": AIRFLOW_HOME},
    )

    commit_file_task = PythonOperator(
        task_id="commit-file",
        python_callable=commit_file,
        op_kwargs={
            "year": "{{ params.year }}",
            "download_link": "{{ params.csv_file_download_url }}",
        },
    )

    (
        wget_task
        >> bzip_task
        >> format_to_parquet_task
        >> [upload_task_csv, upload_task_parquet]
        >> cleanup_task
        >> commit_file_task
    )
