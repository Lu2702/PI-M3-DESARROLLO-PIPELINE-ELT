{{ config(materialized='view') }}

with last_snapshot as (
  
  select max(snapshot_date_key) as dk
  from {{ ref('fct_listing_snapshot') }}
),

base as (
  select
    f.availability_365,
    f.reviews_per_month
  from {{ ref('fct_listing_snapshot') }} f
  join last_snapshot ls
    on f.snapshot_date_key = ls.dk
  where f.reviews_per_month is not null
),

bands as (
  select
    -- 10 bandas de disponibilidad entre 0 y 365 días
    width_bucket(availability_365::numeric, 0, 365.000001, 10) as availability_band,
    avg(availability_365)::numeric(10,2)  as avg_availability_days,
    avg(reviews_per_month)::numeric(10,2) as avg_reviews_per_month,
    count(*)                               as listings_count
  from base
  group by 1
),

labels as (
  select
    availability_band,
    avg_availability_days,
    avg_reviews_per_month,
    listings_count,
    -- rangos legibles para cada banda (≈ 36.5 días por banda)
    floor((availability_band - 1) * (365.0/10.0))::int as lo,
    case when availability_band = 10
         then 365
         else floor(availability_band * (365.0/10.0))::int
    end as hi
  from bands
)

select
  availability_band,
  (lo || '–' || hi) as availability_range_days,  
  avg_availability_days,
  avg_reviews_per_month,
  listings_count
from labels
