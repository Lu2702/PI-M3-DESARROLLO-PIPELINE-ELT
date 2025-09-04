{{ config(materialized='table') }}

with base as (
  select distinct borough_name, neighbourhood_name
  from {{ ref('stg_ab_nyc') }}
  where borough_name is not null and neighbourhood_name is not null
)
select
  {{ dbt_utils.generate_surrogate_key(['borough_name','neighbourhood_name']) }} as neighbourhood_key,
  neighbourhood_name,
  {{ dbt_utils.generate_surrogate_key(['borough_name']) }} as borough_key
from base