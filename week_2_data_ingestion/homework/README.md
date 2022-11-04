Homework 2

Latest docker-compose deployment:
https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html


Question 1: Start date for the Yellow taxi data (1 point)
You'll need to parametrize the DAG for processing the yellow taxi data that we created in the videos.

What should be the start date for this dag?

2019-01-01 <- (we need data for 2019 and 2020)
2020-01-01
2021-01-01
days_ago(1)


I think the point of the question is to learn about `start_date`:
https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html


Question 2: Frequency for the Yellow taxi data (1 point)
How often do we need to run this DAG?

Daily
Monthly <- (a file per month, better on an event!?)
Yearly
Once

BUT https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-append

psql -U airflow -h homework-postgres-1 -p 5432 airflow
SELECT * FROM taxi_zone_lookup;
