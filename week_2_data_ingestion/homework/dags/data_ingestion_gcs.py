import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script_gcs import ingest_gcs_callable


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "")


URL_BASE = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
URL_TEMPLATE = URL_BASE + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

with DAG(
    "GCSIngestionDAG1",
    # schedule=None,
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1),
    # end_date=datetime(2021, 3, 1),
    max_active_runs=1,
    catchup=True,
):
    wget_task = BashOperator(
        task_id='download-data',
        bash_command=f'curl -sSLf {URL_TEMPLATE} --output {OUTPUT_FILE_TEMPLATE}'
    )

    ingest_task = PythonOperator(
        task_id="upload-to-gcs",
        python_callable=ingest_gcs_callable,
        op_kwargs=dict(
            bucket=BUCKET,
            object_name=f"raw/{OUTPUT_FILE_TEMPLATE}",
            parquet_file=OUTPUT_FILE_TEMPLATE
        ),
    )

    cleanup_task = BashOperator(
        task_id='cleanup',
        bash_command=f'rm {OUTPUT_FILE_TEMPLATE}'
    )

    wget_task >> ingest_task >> cleanup_task
