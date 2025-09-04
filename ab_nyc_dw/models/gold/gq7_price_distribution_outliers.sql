{{ config(materialized='view') }}

with fx_latest as (
  select e.usd_to_mxn
  from {{ ref('dim_exchange_rate') }} e
  where e.rate_date = (select max(rate_date) from {{ ref('dim_exchange_rate') }})
  limit 1
),
base as (
  select
    b.borough_name,
    rt.room_type,
    f.price_usd
  from {{ ref('fct_listing_snapshot') }} f
  join {{ ref('dim_borough') }}   b  using (borough_key)
  join {{ ref('dim_room_type') }} rt using (room_type_key)
  where f.price_usd is not null
),
stats as (
  select
    borough_name, room_type,
    avg(price_usd)                                          as mean_usd,
    stddev_samp(price_usd)                                  as sd_usd,
    percentile_cont(0.25) within group (order by price_usd) as p25_usd,
    percentile_cont(0.50) within group (order by price_usd) as p50_usd,
    percentile_cont(0.75) within group (order by price_usd) as p75_usd
  from base
  group by 1,2
)
select
  b.borough_name,
  b.room_type,
  b.price_usd,
  (b.price_usd * fx.usd_to_mxn)::numeric(12,2)             as price_mxn_fx_latest,

  -- estadísticos por grupo (USD)
  s.mean_usd, s.sd_usd, s.p25_usd, s.p50_usd, s.p75_usd,

  -- umbrales IQR (USD) 
  (s.p75_usd + 1.5*(s.p75_usd - s.p25_usd))                as iqr_high_usd,
  (s.p25_usd - 1.5*(s.p75_usd - s.p25_usd))                as iqr_low_usd,

  -- flag de outlier (alto o bajo) en USD
  case
    when b.price_usd > (s.p75_usd + 1.5*(s.p75_usd - s.p25_usd))
      or b.price_usd < (s.p25_usd - 1.5*(s.p75_usd - s.p25_usd))
    then true else false
  end                                                      as is_outlier_iqr_usd,

  --etiqueta del lado
  case
    when b.price_usd > (s.p75_usd + 1.5*(s.p75_usd - s.p25_usd)) then 'high'
    when b.price_usd < (s.p25_usd - 1.5*(s.p75_usd - s.p25_usd)) then 'low'
    else null
  end                                                      as outlier_side
from base b
join stats s using (borough_name, room_type)
cross join fx_latest fx