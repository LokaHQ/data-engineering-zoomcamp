{{ config(materialized='table') }}


with flights as (
    select * from {{ ref('fact_flights') }}
)
	select
		date_trunc(flight_date, month) as flight_month,
		COUNT(*) as flights_count
	from flights
	where diverted = 1
	group by flight_month
	order by flight_month
