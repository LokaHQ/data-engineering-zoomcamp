import os

from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_parquet import ingest_callable


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = 5432
PG_DATABASE = os.getenv('PG_DATABASE')


local_workflow = DAG(
    "local_ingest_fhv",
    schedule_interval="@monthly",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 12, 1),
    catchup=True,
    max_active_runs=1,
)


# URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
TABLE_NAME_TEMPLATE = 'fhv_tripdata_{{ execution_date.strftime(\'%Y_%m\') }}'

with local_workflow:
    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
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
            file=OUTPUT_FILE_TEMPLATE,
            protect_ts_cols=[
                "pickup_datetime",
                "dropOff_datetime",
            ],
        ),
    )

    wget_task >> ingest_task