{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['listing_key','snapshot_date_key'],
  on_schema_change='sync_all_columns'
) }}

with base as (
  select
    listing_id_nat, host_id_nat, borough_name, neighbourhood_name, room_type,
    snapshot_date::date as snapshot_date,
    price_usd::numeric  as price_usd,
    availability_365::int, minimum_nights::int,
    number_of_reviews::int, reviews_per_month::numeric,
    last_review_date::date
  from {{ ref('stg_ab_nyc') }}
),
keys as (
  select
    {{ dbt_utils.generate_surrogate_key(['listing_id_nat']) }}                     as listing_key,
    {{ dbt_utils.generate_surrogate_key(['host_id_nat']) }}                        as host_key,
    {{ dbt_utils.generate_surrogate_key(['borough_name']) }}                       as borough_key,
    {{ dbt_utils.generate_surrogate_key(['borough_name','neighbourhood_name']) }}  as neighbourhood_key,
    {{ dbt_utils.generate_surrogate_key(['room_type']) }}                          as room_type_key,
    *
  from base
),
fx_join as (
  select
    k.*,
    dd.date_key                                         as snapshot_date_key,
    to_char(fx.rate_date,'YYYYMMDD')::int               as exchange_rate_date_key,  
    fx.usd_to_mxn,
    (k.price_usd * fx.usd_to_mxn)::numeric(12,2)        as price_mxn
  from keys k
  join {{ ref('dim_date') }} dd
    on dd.date = k.snapshot_date
  left join lateral (
    select e.rate_date, e.usd_to_mxn
    from {{ ref('dim_exchange_rate') }} e
    where e.rate_date <= k.snapshot_date
    order by e.rate_date desc
    limit 1
  ) fx on true
)
select
  listing_key, host_key, borough_key, neighbourhood_key, room_type_key,
  snapshot_date_key,
  exchange_rate_date_key,                
  price_usd, price_mxn,
  availability_365, minimum_nights,
  number_of_reviews, reviews_per_month, last_review_date,
  (price_usd is not null and availability_365 < 365) as is_active,
  (price_mxn * (365 - availability_365))::numeric(14,2) as revenue_proxy_mxn
from fx_join
