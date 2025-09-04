{{ config(materialized='view') }}

with last_snapshot as (
  -- último día realmente cargado en la FACT
  select max(snapshot_date_key) as dk
  from {{ ref('fct_listing_snapshot') }}
),
counts as (
  select
    b.borough_name,
    nb.neighbourhood_name,
    count(*) filter (where f.is_active) as active_listings
  from {{ ref('fct_listing_snapshot') }} f
  join last_snapshot ls
    on f.snapshot_date_key = ls.dk
  join {{ ref('dim_borough') }}       b  using (borough_key)
  join {{ ref('dim_neighbourhood') }} nb using (neighbourhood_key)
  group by 1,2
)
select
  borough_name,
  neighbourhood_name,
  active_listings
from counts
order by active_listings desc, borough_name, neighbourhood_name
limit 50