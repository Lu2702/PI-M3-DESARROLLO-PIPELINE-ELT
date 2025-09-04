{{ config(materialized='view') }}

-- g_top_hosts_by_properties_usd.sql
-- Top de hosts por número de propiedades (foto del último snapshot) + métricas de precio en USD

with last_snapshot as (
  -- último día realmente cargado en la FACT
  select max(snapshot_date_key) as dk
  from {{ ref('fct_listing_snapshot') }}
),
last_date as (
  select d.date
  from {{ ref('dim_date') }} d
  join last_snapshot ls on d.date_key = ls.dk
),
f as (
  -- foto del día: una fila por (listing, host) del último snapshot
  select
    host_key,
    listing_key,
    price_usd,
    is_active
  from {{ ref('fct_listing_snapshot') }}
  join last_snapshot ls on snapshot_date_key = ls.dk
  where price_usd is not null
),
per_host as (
  select
    host_key,
    count(distinct listing_key)                                                as properties,          -- total de propiedades
    count(distinct listing_key) filter (where is_active)                       as properties_active,   -- opcional: solo activas
    avg(price_usd)::numeric(12,2)                                              as avg_price_usd,
    stddev_samp(price_usd)::numeric(12,2)                                      as std_price_usd
  from f
  group by 1
),
host_name_asof as (
  -- nombre del host válido en la fecha del último snapshot (SCD2 as-of)
  select h.host_key, h.host_name
  from {{ ref('dim_host') }} h
  join last_date ld
    on ld.date >= h.effective_from
   and ld.date <  h.effective_to
),
ranked as (
  select
    p.host_key,
    p.properties,
    p.properties_active,
    p.avg_price_usd,
    p.std_price_usd,
    row_number() over (order by p.properties desc, p.avg_price_usd desc) as rn
  from per_host p
)
select
  r.rn,
  coalesce(h.host_name, '(sin nombre)') as host_name,
  r.properties,
  r.properties_active,
  r.avg_price_usd,
  r.std_price_usd
from ranked r
left join host_name_asof h using (host_key)
where r.rn <= 20
