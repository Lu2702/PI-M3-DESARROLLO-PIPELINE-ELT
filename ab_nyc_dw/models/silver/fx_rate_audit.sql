{{ config(materialized='incremental', incremental_strategy='append') }}

with src as (
  select
    rate_date::date                as rate_date,
    usd_to_mxn::numeric(12,6)      as usd_to_mxn
  from {{ ref('stg_banxico') }}
),
read_ts as (
  -- momento único de la corrida (se usa para todas las filas de esta ejecución)
  select current_timestamp as read_at
)

select
  {{ dbt_utils.generate_surrogate_key(['s.rate_date','r.read_at']) }} as audit_key,
  r.read_at::timestamp                        as read_at,
  to_char(r.read_at::date,'YYYYMMDD')::int    as read_date_key,
  s.rate_date                                 as rate_date,
  to_char(s.rate_date,'YYYYMMDD')::int        as rate_date_key,
  s.usd_to_mxn                                as usd_to_mxn,
  'banxico'                                   as source
from src s
cross join read_ts r
-- append-only: cada corrida añade un nuevo bloque con el mismo read_at