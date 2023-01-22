import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


BUCKET = os.environ.get("GCP_GCS_BUCKET")
METADATA_FILE_GCS_PATH = "metadata/source_files.csv"
METADATA_COMMIT_LOG_FILE_GCS_PATH = "metadata/source_files_commit_log.csv"


def list_files_to_ingest():
    import pandas as pd

    df_source_file = pd.read_csv(f"gs://{BUCKET}/{METADATA_FILE_GCS_PATH}")
    df_commit_log = pd.read_csv(f"gs://{BUCKET}/{METADATA_COMMIT_LOG_FILE_GCS_PATH}")
    df_merge = df_source_file.merge(
        df_commit_log.drop_duplicates(),
        on=["year", "download_link"],
        how="left",
        indicator=True,
    )
    df_not_ingested = df_merge[df_merge["_merge"] == "left_only"]
    return [
        {
            "csv_file_download_url": item["download_link"],
            "year": f"{item['year']}",
        }
        for index, item in df_not_ingested.iterrows()
    ]


with DAG(
    "IngestionTriggerDAG",
    schedule=None,
    # schedule_interval="0 * * * *",
    start_date=datetime(2023, 1, 1),
    tags=["zoom-camp"],
    catchup=False,
    max_active_runs=1,
):

    get_files_to_ingest = PythonOperator(
        task_id="get-files-to-ingest",
        python_callable=list_files_to_ingest,
    )

    ingestion_trigger = TriggerDagRunOperator.partial(
        task_id="ingestion-dag-trigger",
        trigger_dag_id="IngestionDAG",
        wait_for_completion=True,
    ).expand(conf=get_files_to_ingest.output)

    get_files_to_ingest >> ingestion_trigger
