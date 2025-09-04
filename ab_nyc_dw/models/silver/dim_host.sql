{{ config(materialized='table') }}

select
  {{ dbt_utils.generate_surrogate_key(['host_id_nat']) }} as host_key,
  host_id_nat,
  host_name,
  calculated_host_listings_count,
  dbt_valid_from::date as effective_from,
  coalesce(dbt_valid_to::date, '9999-12-31') as effective_to,
  (dbt_valid_to is null) as is_current
from {{ ref('host_snapshot') }}