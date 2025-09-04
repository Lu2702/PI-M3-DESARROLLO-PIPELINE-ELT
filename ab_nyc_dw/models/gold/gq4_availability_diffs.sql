{{ config(materialized='view') }}

with last_snapshot as (
  -- último día realmente cargado en la FACT
  select max(snapshot_date_key) as dk
  from {{ ref('fct_listing_snapshot') }}
),
base as (
  select
    b.borough_name,
    rt.room_type,
    f.availability_365
  from {{ ref('fct_listing_snapshot') }} f
  join last_snapshot ls
    on f.snapshot_date_key = ls.dk
  join {{ ref('dim_borough') }}     b  using (borough_key)
  join {{ ref('dim_room_type') }}   rt using (room_type_key)
  -- Si quieres contar solo ofertas activas, descomenta:
  -- where f.is_active
)
select
  borough_name,
  room_type,
  avg(availability_365)::numeric(10,2) as avg_availability_yr,
  percentile_cont(0.5) within group (order by availability_365) as p50_availability
from base
group by 1,2
order by 1,2
