import os

from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable


# https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-01.parquet
class Fhv:
    url: str = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-02.csv.gz"
    csv: str = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow/"), 'fhv_tripdata_2019-02.csv')
    table: str = "fhv_tripdata"


fhv_workflow = DAG(
    "FHVIngestionDag",
    schedule_interval="0 0 * * *",  # Minute Hour Day Month Weekday
    start_date=datetime(2022, 11, 1),
)


with fhv_workflow:

    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSL {Fhv.url} > {Fhv.csv}.gz',
    )

    unzip_task = BashOperator(
        task_id='unzip',
        bash_command=f'gunzip {Fhv.csv}.gz',
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
            table_name=Fhv.table,
            csv_file=Fhv.csv,
        ),
    )

    # wget_task >> convert_task >> ingest_task
    wget_task >> unzip_task >> ingest_task
