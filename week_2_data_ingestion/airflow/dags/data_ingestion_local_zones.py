import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_csv import ingest_callable


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = 5432
PG_DATABASE = os.getenv('PG_DATABASE')


local_workflow = DAG(
    "local_ingest_zones",
    schedule_interval=None,
    max_active_runs=1,
    start_date=days_ago(1),
)


URL = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
OUTPUT = AIRFLOW_HOME + '/zones.csv'
TABLE_NAME_TEMPLATE = 'zones'

with local_workflow:
    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSL {URL} > {OUTPUT}'
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
            table_name=TABLE_NAME_TEMPLATE,
            file=OUTPUT
        ),
    )

    wget_task >> ingest_task