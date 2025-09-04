{{ config(materialized='table') }}

select
  {{ dbt_utils.generate_surrogate_key(['borough_name']) }} as borough_key,
  borough_name,
  population,
  land_area_km2,
  density_km2
from {{ ref('stg_boroughs') }}