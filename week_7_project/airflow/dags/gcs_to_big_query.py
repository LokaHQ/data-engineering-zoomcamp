import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'twitter_stocks_all')

default_args = {
    "owner": "airflow",
    "retries": 1,
}

def execute_data_dag(
    dag,
    data_nature,
    path
):
    with dag:
        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bq_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{path}_{data_nature}_external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": True,
                    "sourceFormat": f"CSV",
                    "sourceUris": [f"gs://{BUCKET}/{data_nature}/{path}/*"],
                },
            },
        )

        bq_create_partitioned_table_job = BigQueryInsertJobOperator(
            task_id=f"bq_create_partitioned_table_task",
            configuration={
                "query": {
                    "query": f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{path}_{data_nature}_partition_date \
                                PARTITION BY DATE_TRUNC(date, YEAR) \
                                AS \
                                SELECT * FROM {BIGQUERY_DATASET}.{path}_{data_nature}_external_table;",
                    "useLegacySql": False,
                }
            }
        )

        bigquery_external_table_task >> bq_create_partitioned_table_job

def execute_analysis_dag(dag):
    with dag:
        bq_create_analysis_table_task = BigQueryInsertJobOperator(
            task_id=f"bq_create_analysis_table_task",
            configuration={
                "query": {
                    "query": f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.analysis_twitter_stocks \
                                PARTITION BY DATE_TRUNC(date, YEAR) \
                                AS \
                                SELECT \
                                    twitter.date, \
                                    replies_count, \
                                    retweets_count, \
                                    likes_count, \
                                    AAPL, \
                                    AMZN, \
                                    GOOG, \
                                    INTC, \
                                    JPM, \
                                    NFLX, \
                                    PFE, \
                                    TWTR, \
                                    V, \
                                    XOM \
                                FROM {BIGQUERY_DATASET}.stocks_warehouse_all as stocks \
                                INNER JOIN {BIGQUERY_DATASET}.twitter_warehouse_external_table_by_date as twitter \
                                ON twitter.date = stocks.date;",
                    "useLegacySql": False,
                }
            }
        )

        bq_create_analysis_table_task

def execute_ingest_dag(dag):
    with dag:
        execute_ingest_dag = BashOperator(
            task_id="execute_ingest_dag",
            bash_command="jupyter nbconvert --to notebook --execute ./main.ipynb"
        )

        execute_ingest_dag

# Define DAGs
twitter_dag_warehouse = DAG(
    dag_id="twitter_data_warehouse",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['twitter'],
)

twitter_dag_lake = DAG(
    dag_id="twitter_data_lake",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['twitter'],
)

stocks_dag_warehouse = DAG(
    dag_id="stock_data_warehouse",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['stocks'],
)

stocks_dag_lake = DAG(
    dag_id="stock_data_lake",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['stocks'],
)

analysis_dag = DAG(
    dag_id="analysis_dag",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['analysis'],
)

ingest_dag = DAG(
    dag_id="ingest_dag",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['ingest'],
)

# Execute DAGs
execute_data_dag(
    dag=twitter_dag_warehouse,
    data_nature='warehouse',
    path='twitter'
)

execute_data_dag(
    dag=twitter_dag_lake,
    data_nature='raw',
    path='twitter'
)

execute_data_dag(
    dag=stocks_dag_warehouse,
    data_nature='warehouse',
    path='stocks'
)

execute_data_dag(
    dag=stocks_dag_lake,
    data_nature='raw',
    path='stocks'
)

execute_analysis_dag(
    dag=analysis_dag,
)

execute_ingest_dag(
    dag=ingest_dag,
)
