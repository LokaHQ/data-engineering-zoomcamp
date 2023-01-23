# Course Project

![Screenshot 2023-01-13 at 2 02 53 PM](https://user-images.githubusercontent.com/59669948/212337940-b7567144-ff0a-4943-a94f-70e4fda86c37.png)

## Data ingestion

![Screenshot 2023-01-05 at 1 00 32 PM](https://user-images.githubusercontent.com/59669948/212338202-e29e705d-57cd-43c6-a7be-c7a9399378f7.png)

![Screenshot 2023-01-12 at 8 07 40 PM](https://user-images.githubusercontent.com/59669948/212338335-9bbcfcf7-59cb-4283-91cb-8dfc69c2a909.png)

### Create Partitioned tables

```
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
```

### Create all data table

```
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
```

## Analysis

### Big Query

![Screenshot 2023-01-05 at 12 56 17 PM](https://user-images.githubusercontent.com/59669948/212338442-69346694-4e3f-41c6-9e65-4d932e2ce111.png)

```
#
# Twitter analysis
#

#
# Lake

# Twitter all

SELECT * FROM `dtc-de-course-366709.twitter_stocks_all.twitter_lake_all` LIMIT 1000;

#
# Warehouse

SELECT * FROM `dtc-de-course-366709.twitter_stocks_all.twitter_warehouse_external_table` LIMIT 1000;

SELECT * FROM `dtc-de-course-366709.twitter_stocks_all.twitter_warehouse_external_table_by_date` WHERE date = "2023-01-03" LIMIT 1000;

# Create table partitioned by date
CREATE OR REPLACE TABLE `dtc-de-course-366709.twitter_stocks_all.twitter_warehouse_external_table_by_date`
  PARTITION BY DATE_TRUNC(date, YEAR)
  AS
  SELECT * FROM `dtc-de-course-366709.twitter_stocks_all.twitter_warehouse_external_table`;
```

```
#
# Stocks analysis
#

#
# Lake

# Stocks all
SELECT * FROM `dtc-de-course-366709.twitter_stocks_all.stocks_lake_all` LIMIT 1000;

SELECT * FROM `dtc-de-course-366709.twitter_stocks_all.stocks_warehouse_AAPL` LIMIT 1000;

CREATE OR REPLACE TABLE `dtc-de-course-366709.twitter_stocks_all.stocks_warehouse_all`
PARTITION BY DATE_TRUNC(date, YEAR)
AS
SELECT
  AAPL.date,
  AAPL.Adjusted_Close as AAPL,
  AMZN.Adjusted_Close as AMZN,
  GOOG.Adjusted_Close as GOOG,
  INTC.Adjusted_Close as INTC,
  JPM.Adjusted_Close as JPM,
  NFLX.Adjusted_Close as NFLX,
  PFE.Adjusted_Close as PFE,
  TWTR.Adjusted_Close as TWTR,
  V.Adjusted_Close as V,
  XOM.Adjusted_Close as XOM
FROM `dtc-de-course-366709.twitter_stocks_all.stocks_warehouse_AAPL` as AAPL
INNER JOIN `dtc-de-course-366709.twitter_stocks_all.stocks_warehouse_AMZN` as AMZN ON AAPL.date = AMZN.date
INNER JOIN `dtc-de-course-366709.twitter_stocks_all.stocks_warehouse_GOOG` as GOOG ON AAPL.date = GOOG.date
INNER JOIN `dtc-de-course-366709.twitter_stocks_all.stocks_warehouse_INTC` as INTC ON AAPL.date = INTC.date
INNER JOIN `dtc-de-course-366709.twitter_stocks_all.stocks_warehouse_JPM` as JPM ON AAPL.date = JPM.date
INNER JOIN `dtc-de-course-366709.twitter_stocks_all.stocks_warehouse_NFLX` as NFLX ON AAPL.date = NFLX.date
INNER JOIN `dtc-de-course-366709.twitter_stocks_all.stocks_warehouse_PFE` as PFE ON AAPL.date = PFE.date
INNER JOIN `dtc-de-course-366709.twitter_stocks_all.stocks_warehouse_TWTR` as TWTR ON AAPL.date = TWTR.date
INNER JOIN `dtc-de-course-366709.twitter_stocks_all.stocks_warehouse_V` as V ON AAPL.date = V.date
INNER JOIN `dtc-de-course-366709.twitter_stocks_all.stocks_warehouse_XOM` as XOM ON AAPL.date = XOM.date;

SELECT * FROM `dtc-de-course-366709.twitter_stocks_all.analysis_twitter_stocks` LIMIT 1000;
```

```
#
# Analysis
#

# Create table partitioned by date
CREATE OR REPLACE TABLE `dtc-de-course-366709.twitter_stocks_all.analysis_twitter_stocks`
  PARTITION BY DATE_TRUNC(date, YEAR)
  AS
  SELECT
    twitter.date,
    replies_count,
    retweets_count,
    likes_count,
    AAPL,
    AMZN,
    GOOG,
    INTC,
    JPM,
    NFLX,
    PFE,
    TWTR,
    V,
    XOM
  FROM `dtc-de-course-366709.twitter_stocks_all.stocks_warehouse_all` as stocks
  INNER JOIN `dtc-de-course-366709.twitter_stocks_all.twitter_warehouse_external_table_by_date` as twitter
  ON twitter.date = stocks.date;

SELECT * FROM `dtc-de-course-366709.twitter_stocks_all.analysis_twitter_stocks` WHERE date BETWEEN '2022-01-01' and '2022-01-28'
```

### Google Data Studio

![Screenshot 2023-01-13 at 2 06 56 PM](https://user-images.githubusercontent.com/59669948/212338743-1529d0f1-c049-4fe1-b2b8-b1b3c355ea14.png)

![Screenshot 2023-01-13 at 2 08 54 PM](https://user-images.githubusercontent.com/59669948/212339154-51e2bd91-457d-42e0-8855-9d5e6d222cc4.png)
