{{ config(materialized='table') }}

with flights_data as (
    select *
	from {{ ref('stg_flights_data') }}
),

dim_airport_codes as (
    select *
	from {{ ref('dim_airport_codes') }}
)

select
	flights_data.Year as year,
	flights_data.Month as month,
	flights_data.DayofMonth as day,
	flights_data.DayOfWeek as week_day,
	flights_data.Origin as origin_airport_code,
	flights_data.Dest as dest_airport_code,
	flights_data.DepTime as departure_time,
	flights_data.CRSDepTime as scheduled_departure_time,
	flights_data.ArrTime as arrival_time,
	flights_data.CRSArrTime as scheduled_arrival_time,
	flights_data.ActualElapsedTime as actual_ellapsed_time,
	flights_data.CRSElapsedTime as expected_time,
	flights_data.AirTime as air_time,
	flights_data.ArrDelay as arrival_delay,
	flights_data.DepDelay as departure_delay,
	flights_data.Distance as distance,
	flights_data.TaxiIn as taxi_in_time,
	flights_data.TaxiOut as taxi_out_time,
	flights_data.Cancelled as canceled,
	flights_data.CancellationCode as cancellation_code,
	flights_data.Diverted as diverted,
	flights_data.CarrierDelay as carrier_delay,
	flights_data.WeatherDelay as weather_delay,
	flights_data.NASDelay as nas_delay,
	flights_data.SecurityDelay as security_delay,
	flights_data.LateAircraftDelay as late_aircraft_delay,
	airport_codes_origin.name as origin_airport_name,
	airport_codes_dest.name as dest_airport_name,
	airport_codes_origin.coordinates as origin_airport_coordinates,
	airport_codes_dest.coordinates as dest_airport_coordinates
from flights_data
inner join dim_airport_codes as airport_codes_origin
on flights_data.Origin = airport_codes_origin.iata_code
inner join dim_airport_codes as airport_codes_dest
on flights_data.Dest = airport_codes_dest.iata_code
