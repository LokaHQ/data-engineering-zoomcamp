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

"""
Homework

Question 1:
What is count for fhv vehicles data for year 2019
Can load the data for cloud storage and run a count(*)

  CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-366709.trips_data_all.git_fhv_tripdata_external_table_2019`
  OPTIONS (
    format = 'parquet',
    uris = ['gs://dtc_data_lake_dtc-de-course-366709/github_raw/fhv/fhv_tripdata/2019/fhv_tripdata_2019-*.parquet']
  );

  SELECT COUNT(*) FROM `dtc-de-course-366709.trips_data_all.git_fhv_tripdata_external_table_2019`;

  42084899

Question 2:
How many distinct dispatching_base_num we have in fhv for 2019
Can run a distinct query on the table from question 1

  SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `dtc-de-course-366709.trips_data_all.git_fhv_tripdata_external_table_2019`;

  792

Question 3:
Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num
Review partitioning and clustering video.
We need to think what will be the most optimal strategy to improve query performance and reduce cost.

  Partition by dropoff_datetime and cluster dispatching_base_num

Question 4:
What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279
Create a table with optimized clustering and partitioning, and run a count(*). Estimated data processed can be found in top right corner and actual data processed can be found after the query is executed.

  ???

Question 5:
What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag
Review partitioning and clustering video. Partitioning cannot be created on all data types.

  Clustering - Multiple criteria

Question 6:
What improvements can be seen by partitioning and clustering for data size less than 1 GB
Partitioning and clustering also creates extra metadata.
Before query execution this metadata needs to be processed.

  little to none

(Not required) Question 7:
In which format does BigQuery save data
Review big query internals video.

  Columnar
"""
