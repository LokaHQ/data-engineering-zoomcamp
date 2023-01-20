import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectUpdateSensor


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
METADATA_FILE_GCS_PATH = "metadata/source_files.csv"


def list_files_to_ingest():
    import pandas as pd
    df_source_file = pd.read_csv(AIRFLOW_HOME + "/dags/source_files.csv")
    df_commit_log = pd.read_csv(AIRFLOW_HOME + "/dags/source_files_commit_log.csv")
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
            "output_file": f"airlines_{item['year']}.csv",
        }
        for index, item in df_not_ingested.iterrows()
    ]


# copied from sensor source code
def ts_function_overwrite(context):
    """
    Default callback for the GoogleCloudStorageObjectUpdatedSensor. The default
    behaviour is check for the object being updated after the data interval's
    end, or execution_date + interval on Airflow versions prior to 2.2 (before
    AIP-39 implementation).
    """
    try:
        return context["data_interval_end"]
    except KeyError:
        return context["dag"].following_schedule(context["execution_date"])


with DAG(
    "IngestionTriggerDAG",
    schedule=None,
    # schedule_interval="0 * * * *",
    start_date=datetime(2023, 1, 1),
    tags=["zoom-camp"],
    catchup=False,
    max_active_runs=1,
):

    gcs_sensor = GCSObjectUpdateSensor(
        task_id="gcs_metadata_sensor",
        bucket=BUCKET,
        object=METADATA_FILE_GCS_PATH,
        ts_func=ts_function_overwrite,
    )

    get_files_to_ingest = PythonOperator(
        task_id="get_files_to_ingest_step",
        python_callable=list_files_to_ingest,
    )

    ingestion_trigger = TriggerDagRunOperator.partial(
        task_id="ingestion_dag",
        trigger_dag_id="IngestionDAG",
        wait_for_completion=True,
    ).expand(conf=get_files_to_ingest.output)

    gcs_sensor >> get_files_to_ingest >> ingestion_trigger
