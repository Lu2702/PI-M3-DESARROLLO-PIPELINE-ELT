{{ config(materialized='view') }}

-- Último snapshot (clave y fecha)
with last_snapshot as (
  select max(snapshot_date_key) as dk
  from {{ ref('fct_listing_snapshot') }}
),
snap_date as (
  select d.date_key, d.date
  from {{ ref('dim_date') }} d
  join last_snapshot ls on ls.dk = d.date_key
),

-- FX a usar (por si quieres mostrar MXN también)
fx_asof as (
  select e.usd_to_mxn
  from {{ ref('dim_exchange_rate') }} e
  join snap_date s on e.rate_date <= s.date
  order by e.rate_date desc
  limit 1
),
fx_latest as (
  select e.usd_to_mxn
  from {{ ref('dim_exchange_rate') }} e
  order by e.rate_date desc
  limit 1
),

-- Hechos del último snapshot (solo USD)
f as (
  select
    f.borough_key,
    f.neighbourhood_key,
    f.price_usd
  from {{ ref('fct_listing_snapshot') }} f
  join last_snapshot ls on f.snapshot_date_key = ls.dk
),

base as (
  select
    b.borough_name,
    nb.neighbourhood_name,
    f.price_usd,
    (f.price_usd * (select usd_to_mxn from fx_asof))   ::numeric(12,2) as price_mxn_asof,
    (f.price_usd * (select usd_to_mxn from fx_latest)) ::numeric(12,2) as price_mxn_fx_latest
  from f
  join {{ ref('dim_borough') }}       b  using (borough_key)
  join {{ ref('dim_neighbourhood') }} nb using (neighbourhood_key)
),

agg as (
  select
    borough_name,
    neighbourhood_name,
    avg(price_usd)           ::numeric(12,2) as avg_price_usd,
    avg(price_mxn_asof)      ::numeric(12,2) as avg_price_mxn_asof,
    avg(price_mxn_fx_latest) ::numeric(12,2) as avg_price_mxn_fx_latest
  from base
  group by 1,2
)

select
  a.*,
  rank() over (order by a.avg_price_usd desc) as price_rank_usd
from agg a
order by a.avg_price_usd desc, a.borough_name, a.neighbourhood_name
