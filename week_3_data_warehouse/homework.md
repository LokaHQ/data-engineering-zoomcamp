## Homework
[Form](https://forms.gle/ytzVYUh2RptgkvF79)  
We will use all the knowledge learned in this week. Please answer your questions via form above.  
**Deadline** for the homework is 14th Feb 2022 17:00 CET.

### Question 1: 
**What is count for fhv vehicles data for year 2019**  

Can load the data for cloud storage and run a count(*)

`45289863`

### Question 2: 
**How many distinct dispatching_base_num we have in fhv for 2019**  
Can run a distinct query on the table from question 1

`799`
```
SELECT COUNT(DISTINCT dispatching_base_num) FROM `leafy-garden-366619.trips_data_all.fhv_tripdata_external_table`
```

### Question 3: 
**Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num**  

Review partitioning and clustering video.   
We need to think what will be the most optimal strategy to improve query 
performance and reduce cost.

`Partitioning by date, and clustering by dispatching_base_num`

```
-- Bytes processed 1.28 GB
SELECT * EXCEPT(DOlocationID, SR_Flag, PUlocationID) 
FROM `leafy-garden-366619.trips_data_all.fhv_tripdata_external_table`
WHERE dropOff_datetime >= '2019-01-01'
ORDER BY dispatching_base_num;

CREATE OR REPLACE TABLE `leafy-garden-366619.trips_data_all.fhv_tripdata_external_table_partitioned_by_dropOff_datetime`
PARTITION BY DATE(dropOff_datetime) AS
SELECT * EXCEPT(DOlocationID, SR_Flag, PUlocationID) 
FROM `leafy-garden-366619.trips_data_all.fhv_tripdata_external_table`;

-- Bytes processed 1.4 KB
SELECT * FROM `leafy-garden-366619.trips_data_all.fhv_tripdata_external_table_partitioned_by_dropOff_datetime`
WHERE dropOff_datetime >= '2019-01-01'
ORDER BY dispatching_base_num;

CREATE OR REPLACE TABLE `leafy-garden-366619.trips_data_all.fhv_tripdata_external_table_clustered_by_dropOff_datetime`
PARTITION BY DATE(dropOff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * EXCEPT(DOlocationID, SR_Flag, PUlocationID) 
FROM `leafy-garden-366619.trips_data_all.fhv_tripdata_external_table`;

-- Bytes processed 1.4 KB
SELECT * FROM `leafy-garden-366619.trips_data_all.fhv_tripdata_external_table_clustered_by_dropOff_datetime`
WHERE dropOff_datetime >= '2019-01-01'
ORDER BY dispatching_base_num;
```

### Question 4: 
**What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279**  

Create a table with optimized clustering and partitioning, and run a 
count(*). Estimated data processed can be found in top right corner and
actual data processed can be found after the query is executed.

```
CREATE OR REPLACE TABLE `leafy-garden-366619.trips_data_all.fhv_tripdata_external_table_clustered_by_pickup_datetime`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * EXCEPT(DOlocationID, SR_Flag, PUlocationID) 
FROM `leafy-garden-366619.trips_data_all.fhv_tripdata_external_table`;

SELECT * FROM `leafy-garden-366619.trips_data_all.fhv_tripdata_external_table_clustered_by_pickup_datetime`
WHERE pickup_datetime BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02060', 'B02279');
```
Count: `0`

Estimated: `This query will process 0 B when run.`

Actual: `0 B`

Weird... I have no data when I partition and cluster, but I do in the original tables.

In the solution, he paritions by `pickup_datetime`. Why?

### Question 5: 
**What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag**  

Review partitioning and clustering video. 
Partitioning cannot be created on all data types.

According to this [reference](https://www.educative.io/answers/what-is-partitioning-and-clustering-in-bigquery), clustering can only be done over a partioned table. 

In the solution, he says that clustering is the best strategy. Why?

### Question 6: 
**What improvements can be seen by partitioning and clustering for data size less than 1 GB**  
Partitioning and clustering also creates extra metadata.  
Before query execution this metadata needs to be processed.

I think there's no improvement. Partitioning is recommended when your data exceeds 1GB.

### (Not required) Question 7: 
**In which format does BigQuery save data**  
Review big query internals video.

`Column-oriented`
[Reference](https://cloud.google.com/bigquery/docs/storage_overview#:~:text=BigQuery%20stores%20table%20data%20in,very%20large%20number%20of%20records.)
