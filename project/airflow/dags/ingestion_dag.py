import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

from ingestion_dag_functions import ingest_gcs_callable, commit_file


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
        "output_file": Param(default="", type="string"),  # Example airlines_1998.csv
    },
    # {"csv_file_download_url":"https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/IXITH2","output_file":"airlines_1987.csv"}
):
    wget_task = BashOperator(
        task_id="download-data",
        bash_command="curl -sSLf $csv_file_download_url "
        "--output {AIRFLOW_HOME}/$output_file".format(AIRFLOW_HOME=AIRFLOW_HOME),
        env={
            "csv_file_download_url": "{{ params.csv_file_download_url }}",
            "output_file": "{{ params.output_file }}",
        },
    )

    ingest_task = PythonOperator(
        task_id="upload-to-gcs",
        python_callable=ingest_gcs_callable,
        op_kwargs=dict(
            bucket=BUCKET,
            object_name="raw/{{ params.output_file}} ",
            csv_file="{{ params.output_file}}",
        ),
    )

    cleanup_task = BashOperator(
        task_id="cleanup",
        bash_command="rm -f $airflow_home/{{params.output_file}}",
        env={"airflow_home": AIRFLOW_HOME},
    )

    commit_file_task = PythonOperator(
        task_id="commit_file",
        python_callable=commit_file,
        op_kwargs={
            "output_file": "{{ params.output_file }}",
            "download_link": "{{ params.csv_file_download_url }}",
        },
    )

    wget_task >> ingest_task >> cleanup_task >> commit_file_task
