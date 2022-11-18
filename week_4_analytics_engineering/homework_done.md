
DBT REPO: https://github.com/ricardommarques/ny_taxi_rides_zoomcamp

--Q1
select count(*) from `dtc-de-366808.dbt_rmarques.fact_trips`
where extract(year from pickup_datetime) in (2019,2020)

--Q2
-- All because I only processed yellow taxi data

--Q3
select count(*) from `dtc-de-366808.dbt_rmarques.stg_fhv_tripdata`
where extract(year from pickup_datetime) in (2019)

--Q4
select count(*) from `dtc-de-366808.dbt_rmarques.fact_fhv_trips`
where extract(year from pickup_datetime) in (2019)

--Q5
--January