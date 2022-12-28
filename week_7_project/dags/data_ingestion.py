import logging
import os
from datetime import datetime
import pyarrow.csv as pv
import pyarrow.parquet as pq
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def format_to_parquet(prefixFolder, suffixSource, suffixDestiny):
    file = prefixFolder + suffixSource
    if not file.endswith('.csv'):
        logging.error(
            "Can only accept source files in CSV format, for the moment")
        return
    print("reading" + file)
    table = pv.read_csv(file)
    outputFile = prefixFolder + suffixDestiny
    print("writing" + outputFile)
    pq.write_table(table, outputFile)

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed


def upload_to_gcs(bucket, googleCloudFolder, localFolder, suffixParquet):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)
    file = localFolder + suffixParquet
    print("uploading" + file)
    target = googleCloudFolder + suffixParquet
    print("to" + target)
    blob = bucket.blob(target)
    blob.upload_from_filename(file)


default_args = {
    "owner": "airflow",
    # "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


def transfer_data_from_kaggle_to_gcs(
    dag,
    prefixFolder,
    prefixGCSFolder,
    suffixCsv,
    suffixParquet
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"kaggle datasets download -d victorsoeiro/netflix-tv-shows-and-movies -p /opt/airflow/data"
        )
        unzip_dataset_task = BashOperator(
            task_id="unzip_dataset_task",
            bash_command=f"unzip -o /opt/airflow/data/netflix-tv-shows-and-movies.zip -d /opt/airflow/data"
        )
        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "prefixFolder": prefixFolder,
                "suffixSource": suffixCsv,
                "suffixDestiny": suffixParquet,
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "googleCloudFolder": prefixGCSFolder,
                "localFolder": prefixFolder,
                "suffixParquet": suffixParquet,
            },
        )
        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm --force data/*"
        )
        download_dataset_task >> unzip_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task


NETFLIX_PREFIX_FOLDER = AIRFLOW_HOME + \
    '/data/'
NETFLIX_PREFIX_GCS_FOLDER = "raw/dataset/"

# DOWNLOAD CREDITS

CREDITS_NETFLIX_CSV_SUFFIX = 'credits.csv'
CREDITS_NETFLIX_PARQUET_SUFFIX = 'credits.parquet'

credits_netflix_data_dag = DAG(
    dag_id="credits_netflix_data",
    schedule_interval="@monthly",
    start_date=datetime(2019, 1, 1),
    default_args=default_args,
    max_active_runs=1,
    tags=['dtc-de'],
)

transfer_data_from_kaggle_to_gcs(
    dag=credits_netflix_data_dag,
    prefixFolder=NETFLIX_PREFIX_FOLDER,
    prefixGCSFolder=NETFLIX_PREFIX_GCS_FOLDER,
    suffixCsv=CREDITS_NETFLIX_CSV_SUFFIX,
    suffixParquet=CREDITS_NETFLIX_PARQUET_SUFFIX,
)

# DOWNLOAD TITLES

TITLES_NETFLIX_CSV_SUFFIX = 'titles.csv'
TITLES_NETFLIX_PARQUET_SUFFIX = 'titles.parquet'

titles_netflix_data_dag = DAG(
    dag_id="titles_netflix_data",
    schedule_interval="@monthly",
    start_date=datetime(2019, 1, 1),
    default_args=default_args,
    max_active_runs=1,
    tags=['dtc-de'],
)

transfer_data_from_kaggle_to_gcs(
    dag=titles_netflix_data_dag,
    prefixFolder=NETFLIX_PREFIX_FOLDER,
    prefixGCSFolder=NETFLIX_PREFIX_GCS_FOLDER,
    suffixCsv=TITLES_NETFLIX_CSV_SUFFIX,
    suffixParquet=TITLES_NETFLIX_PARQUET_SUFFIX,
)
