-- models/staging/stg_ab_nyc.sql
{% set snap_date = var('snapshot_date', run_started_at.strftime('%Y-%m-%d')) %}
{% set snap_key  = var('snapshot_date_key', run_started_at.strftime('%Y%m%d')) %}

with src as (select * from {{ source('raw_ext','ab_nyc_latest') }})
select
  id::bigint                          as listing_id_nat,
  left(name, 300)                     as listing_name,
  host_id::bigint                     as host_id_nat,
  nullif(trim(host_name),'')          as host_name,
  nullif(neighbourhood_group,'')      as borough_name,
  nullif(neighbourhood,'')            as neighbourhood_name,
  latitude::numeric(9,6)              as latitude,
  longitude::numeric(9,6)             as longitude,
  nullif(room_type,'')                as room_type,
  price::numeric(12,2)                as price_usd,
  minimum_nights::int                 as minimum_nights,
  number_of_reviews::int              as number_of_reviews,
  nullif(trim(last_review::text), '')::date       as last_review_date,
  nullif(trim(reviews_per_month::text),'')::numeric(10,3) as reviews_per_month,
  calculated_host_listings_count::int as calculated_host_listings_count,
  availability_365::int               as availability_365,
  '{{ snap_date }}'::date             as snapshot_date,
  '{{ snap_key }}'::int               as snapshot_date_key
from src
where id is not null