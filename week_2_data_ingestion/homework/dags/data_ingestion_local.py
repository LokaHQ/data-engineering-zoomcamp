import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script_local import ingest_callable


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST', "postgres")
PG_USER = os.getenv('PG_USER', "airflow")
PG_PASSWORD = os.getenv('PG_PASSWORD', "airflow")
PG_PORT = os.getenv('PG_PORT', "5432")
PG_DATABASE = os.getenv('PG_DATABASE', "airflow")


local_workflow = DAG(
    "LocalIngestionDAG9",
    # schedule=None,
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    catchup=True,
)

URL_BASE = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
URL_TEMPLATE = URL_BASE + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
TABLE_NAME_TEMPLATE = 'fhv_{{ execution_date.strftime(\'%Y_%m\') }}'

with local_workflow:
    wget_task = BashOperator(
        task_id='download-data',
        bash_command=f'curl -sSLf {URL_TEMPLATE} --output {OUTPUT_FILE_TEMPLATE}'
    )

    ingest_task = PythonOperator(
        task_id="write-to-postgres",
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            parquet_file=OUTPUT_FILE_TEMPLATE
        ),
    )

    cleanup_task = BashOperator(
        task_id='cleanup',
        bash_command=f'rm {OUTPUT_FILE_TEMPLATE}'
    )

    wget_task >> ingest_task >> cleanup_task
