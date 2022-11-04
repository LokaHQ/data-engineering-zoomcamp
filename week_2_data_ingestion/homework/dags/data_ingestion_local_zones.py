import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable_zones


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST', "postgres")
PG_USER = os.getenv('PG_USER', "airflow")
PG_PASSWORD = os.getenv('PG_PASSWORD', "airflow")
PG_PORT = os.getenv('PG_PORT', "5432")
PG_DATABASE = os.getenv('PG_DATABASE', "airflow")

URL_ZONES = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
OUTPUT_FILE_TEMPLATE_ZONE = AIRFLOW_HOME + '/taxi+_zone_lookup.csv'
TABLE_NAME_TEMPLATE = 'taxi_zone_lookup'

with DAG(
    "LocalIngestionZonesDAG1",
    schedule=None,
    start_date=datetime(2021, 1, 1),
):
    wget_task_zones = BashOperator(
        task_id='download-data-zones',
        bash_command=f'curl -sSLf {URL_ZONES} --output {OUTPUT_FILE_TEMPLATE_ZONE}'
    )

    ingest_task_zones = PythonOperator(
        task_id="write-to-postgres-zones",
        python_callable=ingest_callable_zones,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            csv_file=OUTPUT_FILE_TEMPLATE_ZONE
        ),
    )

    cleanup_task_zones = BashOperator(
        task_id='cleanup-zones',
        bash_command=f'rm {OUTPUT_FILE_TEMPLATE_ZONE}'
    )

    wget_task_zones >> ingest_task_zones >> cleanup_task_zones
