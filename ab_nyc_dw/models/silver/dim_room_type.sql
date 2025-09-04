{{ config(materialized='table') }}

with src as (
  select distinct room_type
  from {{ ref('stg_ab_nyc') }}
  where room_type is not null
)
select
  {{ dbt_utils.generate_surrogate_key(['room_type']) }} as room_type_key,
  room_type
from src