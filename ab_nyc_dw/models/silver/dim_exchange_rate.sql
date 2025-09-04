{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='rate_date',
    on_schema_change='sync_all_columns'
) }}

with src as (
  select
    rate_date::date as rate_date,
    usd_to_mxn::numeric(12,6) as usd_to_mxn
  from {{ ref('stg_banxico') }}
)
select
  {{ dbt_utils.generate_surrogate_key(['rate_date']) }} as rate_key,
  rate_date,
  to_char(rate_date,'YYYYMMDD')::int as rate_date_key,
  usd_to_mxn,
  current_timestamp as updated_at
from src