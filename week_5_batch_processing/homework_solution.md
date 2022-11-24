## Week 5 Homework

In this homework we'll put what we learned about Spark
in practice.

We'll use high volume for-hire vehicles (HVFHV) dataset for that.

## Question 1. Install Spark and PySpark

* Install Spark
* Run PySpark
* Create a local spark session 
* Execute `spark.version`

What's the output?
'3.3.1'

## Question 2. HVFHW February 2021
No access to fhvhv_tripdata_2021-02, used fhvhv_tripdata_2021-01 file instead

Read it with Spark using the same schema as we did 
in the lessons. We will use this dataset for all
the remaining questions.

Repartition it to 24 partitions and save it to
parquet.

What's the size of the folder with results (in MB)?

Answer: 215M


## Question 3. Count records 

How many taxi trips were there on February 15?

Answer: 443059


## Question 4. Longest trip for each day

Now calculate the duration for each trip.

Trip starting on which day was the longest? 

+-----------+-------------+
|pickup_date|max(duration)|
+-----------+-------------+
| 2021-01-27|        59143|
| 2021-01-05|        45012|
| 2021-01-30|        41193|
| 2021-01-04|        39967|
| 2021-01-06|        38417|
+-----------+-------------+

Answer: 2021-01-27


## Question 5. Most frequent `dispatching_base_num`

Now find the most frequently occurring `dispatching_base_num` 
in this dataset.

How many stages this spark job has?

+--------------------+--------+
|dispatching_base_num|count(1)|
+--------------------+--------+
|              B02510| 3091000|
|              B02764| 1009388|
|              B02872|  924960|
|              B02875|  735450|
|              B02765|  591242|
+--------------------+--------+

Answer: 1 (1 skipped)


## Question 6. Most common locations pair

Find the most common pickup-dropoff pair. 

+--------------------+--------+
|          pu_do_pair|count(1)|
+--------------------+--------+
|East New York / E...|   47637|
|Borough Park / Bo...|   30920|
| Canarsie / Canarsie|   29897|
|Crown Heights Nor...|   28851|
|Central Harlem No...|   17379|
+--------------------+--------+

[Row(pu_do_pair='East New York / East New York', count(1)=47637),
 Row(pu_do_pair='Borough Park / Borough Park', count(1)=30920),
 Row(pu_do_pair='Canarsie / Canarsie', count(1)=29897),
 Row(pu_do_pair='Crown Heights North / Crown Heights North', count(1)=28851),
 Row(pu_do_pair='Central Harlem North / Central Harlem North', count(1)=17379)]

 Answer: East New York / East New York


## Bonus question. Join type

(not graded) 

For finding the answer to Q6, you'll need to perform a join.

What type of join is it?

And how many stages your spark job has?


## Submitting the solutions

* Form for submitting: https://forms.gle/dBkVK9yT8cSMDwuw7
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 07 March (Monday), 22:00 CET
