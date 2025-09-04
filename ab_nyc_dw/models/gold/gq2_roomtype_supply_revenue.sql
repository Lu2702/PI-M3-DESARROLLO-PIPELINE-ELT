{{ config(materialized='view') }}



with fx_latest as (
  
  select e.usd_to_mxn
  from {{ ref('dim_exchange_rate') }} e
  where e.rate_date = (select max(rate_date) from {{ ref('dim_exchange_rate') }})
  limit 1
),

base as (
  select
    rt.room_type,
    f.is_active,
    f.revenue_proxy_mxn,           
    f.price_usd,
    f.availability_365
  from {{ ref('fct_listing_snapshot') }} f
  join {{ ref('dim_room_type') }} rt using (room_type_key)
),

calc as (
  select
    room_type,
    is_active,
    revenue_proxy_mxn,
    price_usd,
    greatest(365 - availability_365, 0)::int as nights_booked
  from base
),

agg as (
  select
    room_type,
    count(*) filter (where is_active)                                  as active_listings,
    -- MXN as-of (del snapshot)
    sum(revenue_proxy_mxn)::numeric(14,2)                              as est_revenue_mxn_asof,
    -- USD del snapshot
    sum(price_usd * nights_booked)::numeric(14,2)                      as est_revenue_usd,
    -- MXN revaluado con el último FX disponible
    sum(price_usd * nights_booked * fx.usd_to_mxn)::numeric(14,2)      as est_revenue_mxn_fx_latest
  from calc c
  cross join fx_latest fx
  group by room_type
)

select
  *,
  rank() over (order by active_listings desc)           as popularity_rank,
  rank() over (order by est_revenue_mxn_fx_latest desc) as revenue_rank
from agg
order by est_revenue_mxn_fx_latest desc