import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

DATASET = "tripdata"
COLOUR_RANGE = {
    'yellow': 'tpep_pickup_datetime',
    'green': 'lpep_pickup_datetime',
    'fhv': 'pickup_datetime'
}
INPUT_PART = "github_raw"
INPUT_FILETYPE = "parquet"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_2_bq_dag_v2",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for colour, ds_col in COLOUR_RANGE.items():
        move_files_gcs_task = GCSToGCSOperator(
            task_id=f'move_{colour}_{DATASET}_files_task',
            source_bucket=BUCKET,
            source_object=f'{INPUT_PART}/{colour}_{DATASET}*.{INPUT_FILETYPE}',
            destination_bucket=BUCKET,
            destination_object=f'{INPUT_PART}/{colour}/{colour}_{DATASET}',
            move_object=True
        )

        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bq_{colour}_{DATASET}_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"git_{colour}_{DATASET}_external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": True,
                    "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                    "sourceUris": [f"gs://{BUCKET}/{INPUT_PART}/{colour}/*"],
                },
            },
        )

        # CSV Heads
        # yellow
        # VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge

        # Green
        # VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type,congestion_surcharge

        # FHV
        # dispatching_base_num,pickup_datetime,dropoff_datetime,PULocationID,DOLocationID,SR_Flag

        if colour == "green":
            CREATE_BQ_TBL_QUERY = (
                f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{DATASET} \
                PARTITION BY DATE({ds_col}) \
                AS \
                SELECT * EXCEPT (`ehail_fee`) FROM {BIGQUERY_DATASET}.git_{colour}_{DATASET}_external_table;"
            )
        else:
            CREATE_BQ_TBL_QUERY = (
                f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{DATASET} \
                PARTITION BY DATE({ds_col}) \
                AS \
                SELECT * FROM {BIGQUERY_DATASET}.git_{colour}_{DATASET}_external_table;"
            )

        # if colour == "yellow":
        #     CREATE_BQ_TBL_QUERY = (
        #         f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{DATASET} \
        #         PARTITION BY DATE({ds_col}) \
        #         AS \
        #         SELECT * EXCEPT (`airport_fee`) FROM {BIGQUERY_DATASET}.{colour}_{DATASET}_external_table;"
        #     )
        # elif colour == "green":
        #     CREATE_BQ_TBL_QUERY = (
        #         f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{DATASET} \
        #         PARTITION BY DATE({ds_col}) \
        #         AS \
        #         SELECT * EXCEPT (`ehail_fee`) FROM {BIGQUERY_DATASET}.{colour}_{DATASET}_external_table;"
        #     )
        # else:
        #     CREATE_BQ_TBL_QUERY = (
        #         f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{DATASET} \
        #         PARTITION BY DATE({ds_col}) \
        #         AS \
        #         SELECT * EXCEPT (`SR_Flag`, `DOlocationID`, `PUlocationID`) FROM {BIGQUERY_DATASET}.{colour}_{DATASET}_external_table;"
        #     )

        # Create a partitioned table from external table
        bq_create_partitioned_table_job = BigQueryInsertJobOperator(
            task_id=f"bq_create_{colour}_{DATASET}_partitioned_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

        move_files_gcs_task >> bigquery_external_table_task >> bq_create_partitioned_table_job