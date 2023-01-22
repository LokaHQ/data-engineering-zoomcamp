{{ config(materialized='table') }}


with flights as (
    select * from {{ ref('fact_flights') }}
)
	select
		date_trunc(flight_date, year) as flight_year,
		MAX(carrier_delay) as max_carrier_delay,
		AVG(carrier_delay) as avg_carrier_delay,
		MAX(weather_delay) as max_weather_delay,
		AVG(weather_delay) as avg_weather_delay,
		MAX(nas_delay) as max_nas_delay,
		AVG(nas_delay) as avg_nas_delay,
		MAX(security_delay) as max_security_delay,
		AVG(security_delay) as avg_security_delay,
		MAX(late_aircraft_delay) as max_late_aircraft_delay,
		AVG(late_aircraft_delay) as avg_late_aircraft_delay,
		MAX(carrier_delay + weather_delay + nas_delay + security_delay + late_aircraft_delay) as max_total_delay,
		AVG(carrier_delay + weather_delay + nas_delay + security_delay + late_aircraft_delay) as avg_total_delay,
		COUNT(*) as flights_count
	from flights
	group by flight_year
	order by flight_year
