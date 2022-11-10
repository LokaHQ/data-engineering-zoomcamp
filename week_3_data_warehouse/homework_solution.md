## Homework
[Form](https://forms.gle/ytzVYUh2RptgkvF79)  
We will use all the knowledge learned in this week. Please answer your questions via form above.  
**Deadline** for the homework is 14th Feb 2022 17:00 CET.

### Question 1: 
**What is count for fhv vehicles data for year 2019**  
```
CREATE OR REPLACE EXTERNAL TABLE `amazing-office-366810.trips_data_all.fhv_tripdata`
OPTIONS (
  format='parquet',
  uris = [
    'gs://dtc_data_lake_amazing-office-366810/raw/fhv_tripdata/2019/*'
  ]
);

SELECT count(1) FROM trips_data_all.fhv_tripdata;
```

Answer: 42084899

### Question 2: 
**How many distinct dispatching_base_num we have in fhv for 2019**  
```
SELECT count(distinct(dispatching_base_num)) FROM trips_data_all.fhv_tripdata;
```

Answer: 792

### Question 3: 
**Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num**  
Partition by dropoff_datetime and cluster by dispatching_base_num

### Question 4: 
**What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279**  
```
CREATE OR REPLACE TABLE `trips_data_all.fhv_nonpartitioned_tripdata`
as SELECT * from `trips_data_all.fhv_tripdata`

CREATE OR REPLACE TABLE `trips_data_all.fhv_partitioned_tripdata`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
 SELECT * from `trips_data_all.fhv_tripdata`
);

SELECT COUNT(*) FROM `trips_data_all.fhv_nonpartitioned_tripdata`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');

SELECT COUNT(*) FROM `trips_data_all.fhv_partitioned_tripdata`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');

Non-partitioned: This query will process 642.97 MB when run

Answer: 26558

```

### Question 5: 
**What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag**  
Cluster by dispatching_base_num and SR_Flag, it is not possible to partition by data type string

### Question 6: 
**What improvements can be seen by partitioning and clustering for data size less than 1 GB**  
No improvements
Can be worse due to metadata (by partitioning and clustering our tables we have additional metadata)

### (Not required) Question 7: 
**In which format does BigQuery save data**  
Columnar
