{{ config(materialized='table') }}

-- dim_listing.sql (Silver) — con listing_name (SCD2)
select
  {{ dbt_utils.generate_surrogate_key(['listing_id_nat']) }} as listing_key,
  listing_id_nat,
  listing_name,

  {{ dbt_utils.generate_surrogate_key(['host_id_nat']) }}                        as host_key,
  {{ dbt_utils.generate_surrogate_key(['borough_name']) }}                       as borough_key,
  {{ dbt_utils.generate_surrogate_key(['borough_name','neighbourhood_name']) }}  as neighbourhood_key,
  {{ dbt_utils.generate_surrogate_key(['room_type']) }}                          as room_type_key,

  latitude,
  longitude,

  
  dbt_valid_from::date                         as effective_from,
  coalesce(dbt_valid_to::date, '9999-12-31')   as effective_to,
  (dbt_valid_to is null)                       as is_current
from {{ ref('listing_snapshot') }}