{{ config(materialized='table') }}

{% set date_floor = var('date_floor', '2025-08-28') %}   -- piso mínimo
{% set days_ahead = var('date_days_ahead', 7) %}         -- colchón hacia adelante

-- Límites usados SOLO desde modelos que NO dependen de dim_date
with stg_bounds as (
  select min(snapshot_date)::date as min_d,
         max(snapshot_date)::date as max_d
  from {{ ref('stg_ab_nyc') }}
),
fx_bounds as (
  select min(rate_date)::date as min_d,
         max(rate_date)::date as max_d
  from {{ ref('dim_exchange_rate') }}   -- este modelo no depende de dim_date
),
used_bounds as (
  select
    -- mínimo real observado (staging / fx); si no hubiera datos, usa hoy
    least(
      coalesce((select min_d from stg_bounds), current_date),
      coalesce((select min_d from fx_bounds),  current_date)
    ) as min_d,
    greatest(
      coalesce((select max_d from stg_bounds), current_date),
      coalesce((select max_d from fx_bounds),  current_date),
      current_date
    ) as max_d
),
bounds as (
  select
    -- No bajar del piso fijado (tu límite inferior es 2025-08-28)
    greatest(to_date('{{ date_floor }}','YYYY-MM-DD'), min_d)              as min_date,
    -- Cubre el máximo observado + colchón (p. ej., 7 días)
    (max_d + interval '{{ days_ahead }} day')                              as max_date
  from used_bounds
),
dates as (
  select generate_series(min_date, max_date, interval '1 day')::date as dt
  from bounds
)
select
  to_char(dt,'YYYYMMDD')::int as date_key,
  dt                          as date,
  extract(year  from dt)::int as year,
  extract(month from dt)::int as month,
  extract(day   from dt)::int as day
from dates
order by dt