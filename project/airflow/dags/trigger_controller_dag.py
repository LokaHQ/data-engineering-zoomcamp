import os
from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import task
from airflow.providers.google.cloud.sensors.gcs import GCSObjectUpdateSensor


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
METADATA_FILE = "metadata/source_files.csv"


@task
def get_files_to_download():
    return [
        {
            "csv_file_download_url": "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/IXITH2",
            "output_file": "airlines_1987.csv",
        },
        {
            "csv_file_download_url": "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HG7NV7/TUYWU3",
            "output_file": "airlines_1988.csv",
        }
    ]


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
        object=METADATA_FILE,
        ts_func=ts_function_overwrite,
    )

    ingestion_trigger = TriggerDagRunOperator.partial(
        task_id="ingestion_dag",
        trigger_dag_id="IngestionDAG",
        wait_for_completion=True,
    ).expand(conf=get_files_to_download())

    gcs_sensor >> ingestion_trigger
