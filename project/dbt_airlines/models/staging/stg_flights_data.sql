{{ config(materialized='view') }}

select *
from {{ source('staging', 'airlines_data') }}
