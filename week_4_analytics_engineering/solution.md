## Week 4: Analytics Engineering

### Install dbt
Follow the [pip-install docs](https://docs.getdbt.com/docs/get-started/pip-install)

    pip install dbt-postgres

Alternatively use [homebrew-install docs](https://docs.getdbt.com/docs/get-started/homebrew-install)

### Configure dbt
[Postgres setup docs](https://docs.getdbt.com/reference/warehouse-setups/postgres-setup)

    cd taxi_rides_ny
    vi ~/.dbt/profiles.yaml
    dbt debug
    dbt deps
    dbt seed
    dbt run
    dbt test

### Question 1: 
**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)**

    select count(1) from fact_trips where extract(year from pickup_datetime) in (2019, 2020)
    select count(1) from fact_trips where pickup_datetime between('2019-01-01 00:00:00 UTC') AND ('2020-12-31 23:59:59 UTC')

Answer: 61603016

### Question 2: 
**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos**
Obtained from service type distribution pie chart 

    10.2% - Green
    89.8% - Yellow

### Question 3: 
**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)**  

    select count(1) from stg_fhv_tripdata where extract(year from pickup_datetime) in (2019)

Answer: 42084899

### Question 4: 
**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)**  

    select count(1) from fact_fhv_trips;

Answer: 22676253

### Question 5: 
**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table**

    January