import os
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
from google.cloud import storage
from datetime import datetime
from pathlib import Path

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

class Base:
    def __init__(self, download_url, local_output_path, gcs_path):
        self.DOWNLOAD_URL = download_url
        self.LOCAL_OUTPUT_PATH = local_output_path
        self.GCS_PATH = gcs_path
    
    def upload_to_gcs(self, bucket, object_name, local_file):
        # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
        # (Ref: https://github.com/googleapis/python-storage/issues/74)
        storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
        storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
        # End of Workaround
        client = storage.Client()
        bucket = client.bucket(bucket)
        blob = bucket.blob(object_name)
        blob.upload_from_filename(local_file)
    
    def run_dag(self, DAG):
        with DAG:
            download_dataset_task = BashOperator(
                task_id="download_dataset_task",
                bash_command=f"curl -sSLf {self.DOWNLOAD_URL} > {self.LOCAL_OUTPUT_PATH}"
            )
            
            local_to_gcs_task = PythonOperator(
                task_id="local_to_gcs_task",
                python_callable=self.upload_to_gcs,
                op_kwargs={
                    "bucket": BUCKET,
                    "object_name": self.GCS_PATH,
                    "local_file": self.LOCAL_OUTPUT_PATH,
                },
            )
            
            rm_task = BashOperator(
                task_id="rm_rask",
                bash_command=f"rm {self.LOCAL_OUTPUT_PATH}"
            )
            
            download_dataset_task >> local_to_gcs_task >> rm_task
            
class InputCSV(Base):
    def __init__(self, download_url, local_output_path, gcs_path):
        super().__init__(download_url, local_output_path, gcs_path)
        
    def format_to_parquet(self, src_file, dest_file):
        if not src_file.endswith('.csv'):
            logging.error("Can only accept source files in CSV format, for the moment")
            return
        table = pv.read_csv(src_file)
        pq.write_table(table, dest_file)
        
    def run_dag(self, DAG):
        with DAG:
            download_dataset_task = BashOperator(
                task_id="download_dataset_task",
                bash_command=f"curl -sSLf {self.DOWNLOAD_URL} > {self.LOCAL_OUTPUT_PATH}"
            )
            
            formatted_output =  Path(self.LOCAL_OUTPUT_PATH).with_suffix('.parquet')
            format_to_parquet_task = PythonOperator(
                task_id="format_to_parquet_task",
                python_callable=self.format_to_parquet,
                op_kwargs={
                    "src_file": self.LOCAL_OUTPUT_PATH,
                    "dest_file": formatted_output
                },
            )
            
            local_to_gcs_task = PythonOperator(
                task_id="local_to_gcs_task",
                python_callable=self.upload_to_gcs,
                op_kwargs={
                    "bucket": BUCKET,
                    "object_name": self.GCS_PATH,
                    "local_file": self.LOCAL_OUTPUT_PATH,
                },
            )
            
            rm_task = BashOperator(
                task_id="rm_rask",
                bash_command=f"rm {self.LOCAL_OUTPUT_PATH}"
            )
            
            download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task

##### TAXI DAG #####
taxi_dag = DAG(
    dag_id="taxi_dag",
    schedule_interval="@monthly",
    start_date=datetime(2019,1,1),
    end_date=datetime(2020,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de']
)

taxi = Base(
    download_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet',
    local_output_path = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet',
    gcs_path = "raw/yellow_tripdata/{{ execution_date.strftime(\'%Y\') }}/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
)
taxi.run_dag(taxi_dag)


##### FHV DAG #####
fhv_dag = DAG(
    dag_id="fhv_dag",
    schedule_interval="@monthly",
    start_date=datetime(2019,1,1),
    end_date=datetime(2020,1,1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de']
)
fhv = Base(
    download_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet',
    local_output_path = AIRFLOW_HOME + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet',
    gcs_path = "raw/fhv_tripdata/{{ execution_date.strftime(\'%Y\') }}/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
)
fhv.run_dag(fhv_dag)

##### ZONES #####
zone_dag = DAG(
    dag_id="zone_dag",
    schedule_interval="@once",
    start_date=datetime.today(),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de']
)
zone = Base(
    download_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv',
    local_output_path = AIRFLOW_HOME + '/fhv_tripdata_zone_lookup.csv',
    gcs_path = "raw/taxi_zone/taxi_zone_lookup.parquet"
)
zone.run_dag(zone_dag)