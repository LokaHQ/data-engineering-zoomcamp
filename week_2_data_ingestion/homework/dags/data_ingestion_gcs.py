import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from ingest_script_gcs import ingest_gcs_callable


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dezc_data_lake_dataengineeringzoomcamp-368119")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


URL_TEMPLATE = 'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GCS_OBJECT_NAME = 'raw/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GCS_SOURCE_OBJECT_NAME = '/raw/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

with DAG(
    "GCSIngestionDAG10",
    # schedule=None,
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 1),
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
            object_name=GCS_OBJECT_NAME,
            parquet_file=OUTPUT_FILE_TEMPLATE
        ),
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}{GCS_SOURCE_OBJECT_NAME}"],
            },
        },
    )

    cleanup_task = BashOperator(
        task_id='cleanup',
        bash_command=f'rm {OUTPUT_FILE_TEMPLATE}'
    )

    wget_task >> ingest_task >> bigquery_external_table_task >> cleanup_task
