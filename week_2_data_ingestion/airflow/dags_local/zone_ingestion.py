import os

from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable


class Zone:
    url: str = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
    csv: str = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow/"), 'zones.csv')
    table: str = "zones"


zone_workflow = DAG(
    "ZoneIngestionDag",
    schedule_interval="0 0 1 * *",  # Minute Hour Day Month Weekday
    start_date=datetime(2022, 11, 1),
)


with zone_workflow:

    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSL {Zone.url} > {Zone.csv}',
    )

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST'),
            port=os.getenv('POSTGRES_PORT'),
            db=os.getenv('POSTGRES_DB'),
            table_name=Zone.table,
            csv_file=Zone.csv,
        ),
    )

    wget_task >> ingest_task
