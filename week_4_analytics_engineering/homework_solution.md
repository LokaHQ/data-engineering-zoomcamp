## Week 4 Homework 
[Form](https://forms.gle/B5CXshja3MRbscVG8) 

We will use all the knowledge learned in this week. Please answer your questions via form above.  
* You can submit your homework multiple times. In this case, only the last submission will be used. 

**Deadline** for the homework is 23rd Feb 2022 22:00 CET.


In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

We will use the data loaded for:
* Building a source table: stg_fhv_tripdata
* Building a fact table: fact_fhv_trips
* Create a dashboard 

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres -
only if you want to.

### Question 1: 
**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)**  
```
select count(1) from `amazing-office-366810.production.fact_trips` where extract(year from pickup_datetime) in (2019, 2020)

select count(1) from `amazing-office-366810.production.fact_trips` where pickup_datetime between('2019-01-01 00:00:00 UTC') AND ('2020-12-31 23:59:59 UTC')
```

Answer: 61603016

### Question 2: 
**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos**
Obtained from service type distribution pie chart 

10.2% - Green
89.8% - Yellow

### Question 3: 
**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)**  
```
select count(1) from `amazing-office-366810.production.stg_fhv_tripdata` where extract(year from pickup_datetime) in (2019)
```

Answer: 42084899

### Question 4: 
**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)**  
```
select count(1) from `amazing-office-366810.production.fact_fhv_trips`
```

Answer: 22676253

### Question 5: 
**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table**

January


