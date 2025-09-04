{{ config(materialized='view') }}

with f as (
  select
    borough_key,
    listing_key,
    reviews_per_month,
    last_review_date,
    snapshot_date_key
  from {{ ref('fct_listing_snapshot') }}
  where last_review_date is not null
),
dd as (
  select date_key, date
  from {{ ref('dim_date') }}
),
f_enriched as (
  select
    f.borough_key,
    f.listing_key,
    f.reviews_per_month,
    f.last_review_date,
    dd.date as snapshot_date
  from f
  join dd on dd.date_key = f.snapshot_date_key
),
by_listing_month as (
  select
    borough_key,
    listing_key,
    date_trunc('month', last_review_date)::date as last_review_month,
    reviews_per_month,
    snapshot_date,
    row_number() over (
      partition by borough_key, listing_key, date_trunc('month', last_review_date)
      order by snapshot_date desc
    ) as rn
  from f_enriched
)
select
  b.borough_name,
  to_char(last_review_month, 'YYYY-MM') as ym,
  avg(reviews_per_month)::numeric(10,2) as avg_reviews_per_month,
  count(*) as listings_with_review 
from by_listing_month x
join {{ ref('dim_borough') }} b using (borough_key)
where rn = 1
group by 1,2
