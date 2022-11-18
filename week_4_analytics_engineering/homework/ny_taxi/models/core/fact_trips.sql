{{ config(materialized='table') }}

with fhv_data as (
    select *
    from {{ ref('stg_fhv_data') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
	fhv_data.tripid,
	fhv_data.SR_Flag,
	fhv_data.pickup_datetime,
	fhv_data.dropOff_datetime,
	fhv_data.dispatching_base_num,
	fhv_data.Affiliated_base_number,
	pickup_zone.locationid as pickup_zone_id,
	pickup_zone.zone as pickup_zone,
	dropoff_zone.locationid as dropoff_zone_id,
	dropoff_zone.zone as dropoff_zone,
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.DOlocationID = dropoff_zone.locationid
