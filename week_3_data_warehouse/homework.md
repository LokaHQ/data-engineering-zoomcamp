## Homework
[Form](https://forms.gle/ytzVYUh2RptgkvF79)  
We will use all the knowledge learned in this week. Please answer your questions via form above.  
**Deadline** for the homework is 14th Feb 2022 17:00 CET.

### Question 1: 
**What is count for fhv vehicles data for year 2019**  
Can load the data for cloud storage and run a count(*)

```
SELECT count(*) FROM `dataengineeringzoomcamp-368119.trips_data_all.external_table`;	
```

### Question 2: 
**How many distinct dispatching_base_num we have in fhv for 2019**  
Can run a distinct query on the table from question 1

```
SELECT count(DISTINCT(dispatching_base_num)) FROM `dataengineeringzoomcamp-368119.trips_data_all.external_table`;	
```


### Question 3: 
**Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num**  
Review partitioning and clustering video.   
We need to think what will be the most optimal strategy to improve query 
performance and reduce cost.

```
1. Partitioning by dropoff_datetime
2. Clustering by dispatching_base_num
```

```
-- Create a non partitioned internal table from external table (partitioning not possible for external data sources) 
CREATE OR REPLACE TABLE trips_data_all.fhv_data_non_partitoned AS
SELECT * FROM trips_data_all.external_table;
```

Is there an alternative for the concept predicate push down from Apache Spark??

```
-- Now we can see the costs estimation because we have internal table
SELECT count(DISTINCT(dispatching_base_num)) FROM trips_data_all.fhv_data_non_partitoned;
```

```
-- Create a non partitioned and clustered table
CREATE OR REPLACE TABLE trips_data_all.fhv_data_partitioned_clustered
PARTITION BY DATE(dropOff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM trips_data_all.fhv_data_non_partitoned;
```



### Question 4: 
**What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279**  
Create a table with optimized clustering and partitioning, and run a 
count(*). Estimated data processed can be found in top right corner and
actual data processed can be found after the query is executed.

```
-- Not Optimized, estimation 354.19MB, Bytes processed 354.19 MB

SELECT count(*) FROM trips_data_all.fhv_data_partitioned_clustered 
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
AND dispatching_base_num IN ("B00987", "B02060", "B02279");
```

```
-- Optimized, estimation 354.19MB, Bytes processed 87.56 MB

SELECT count(*) FROM trips_data_all.fhv_data_partitioned_clustered 
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
AND dispatching_base_num IN ("B00987", "B02060", "B02279");
```


### Question 5: 
**What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag**  
Review partitioning and clustering video. 
Partitioning cannot be created on all data types.

```
Clustering on both dispatching_base_num and SR_Flag.
```

### Question 6: 
**What improvements can be seen by partitioning and clustering for data size less than 1 GB**  
Partitioning and clustering also creates extra metadata.  
Before query execution this metadata needs to be processed.

```
For small data size, less than 1GB, partitioning and clustering can actually ADD significant cost.
```

### (Not required) Question 7: 
**In which format does BigQuery save data**  
Review big query internals video.

```
Columnar format in so called Colossus.
```