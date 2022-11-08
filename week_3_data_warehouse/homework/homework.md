

-- Q1
-- Answer: 43261276
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-366808.nytaxi.fhv_tripdata`
OPTIONS (
  format='parquet',
  uris = [
    ´gs://dtc_data_lake_dtc-de-366808/raw/fhv_tripdata/2019/*´
  ]
);

SELECT * EXCEPT(DOlocationID) FROM `dtc-de-366808.nytaxi.fhv_tripdata` LIMIT 10;

SELECT COUNT(*) FROM `dtc-de-366808.nytaxi.fhv_tripdata`;

-- Q2
-- Answer: 800

-- Q3
-- Partition by drop off time and cluster by dispatching_base_num (as it is a string and is ordered by that)

-- Q4
-- Answer: 25672

CREATE OR REPLACE TABLE dtc-de-366808.nytaxi.fhv_tripdata_partitoned
PARTITION BY DATE(dropOff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * EXCEPT(DOlocationID) FROM `dtc-de-366808.nytaxi.fhv_tripdata;

SELECT count(*) FROM  dtc-de-366808.nytaxi.fhv_tripdata_partitioned
WHERE dropOff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');

-- Q5
-- Answer: Both are strings so cluster by dispatching_base_num and SR_Flag

-- Q6
-- Answer: There are no improvements because there is very little data

-- Q7
-- Answer: Columnar
