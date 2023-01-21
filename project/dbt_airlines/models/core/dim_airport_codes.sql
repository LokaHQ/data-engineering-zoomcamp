{{ config(materialized='view') }}


select
	ident,
    iata_code,
    -- local_code,
    name,
    -- type,
    elevation_ft,
    -- continent,
    -- iso_country,
    -- iso_region,
	coordinates
from {{ ref('airports') }}