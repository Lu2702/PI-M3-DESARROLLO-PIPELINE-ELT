{{ config(materialized='view') }}


with last_snapshot as (
  select max(snapshot_date_key) as dk
  from {{ ref('fct_listing_snapshot') }}
),

active_by_borough as (
  select
    b.borough_name,
    count(*) filter (where f.is_active) as active_listings
  from {{ ref('fct_listing_snapshot') }} f
  join last_snapshot ls on f.snapshot_date_key = ls.dk
  join {{ ref('dim_borough') }} b using (borough_key)
  group by 1
),

enriched as (
  select
    ab.borough_name,
    ab.active_listings,
    db.population,
    db.land_area_km2,
    case when db.population > 0
         then (ab.active_listings::numeric * 100000.0) / db.population
         else null::numeric end                      as oferta_per_capita_100k,
    case when db.land_area_km2 > 0
         then (ab.active_listings::numeric / db.land_area_km2)
         else null::numeric end                      as densidad_oferta_km2
  from active_by_borough ab
  join {{ ref('dim_borough') }} db on db.borough_name = ab.borough_name
),

ranked as (
  select
    e.*,
    
    round((100.0 * cume_dist() over (order by e.oferta_per_capita_100k))::numeric, 0)::int as pctl_per_capita,
    round((100.0 * cume_dist() over (order by e.densidad_oferta_km2))::numeric, 0)::int     as pctl_density
  from enriched e
),

scored as (
  select
    r.*,
    (rank() over (order by r.oferta_per_capita_100k desc))::int as rank_per_capita,
    (rank() over (order by r.densidad_oferta_km2 desc))::int     as rank_density,
    round(((coalesce(r.pctl_per_capita,0) + coalesce(r.pctl_density,0)) / 2.0)::numeric, 0)::int as pctl_combined,
    case
      when round(((coalesce(r.pctl_per_capita,0) + coalesce(r.pctl_density,0)) / 2.0)::numeric, 0)::int >= 67 then 'Alto'
      when round(((coalesce(r.pctl_per_capita,0) + coalesce(r.pctl_density,0)) / 2.0)::numeric, 0)::int >= 34 then 'Medio'
      else 'Bajo'
    end as semaforo_combined
  from ranked r
)

select
  borough_name,
  active_listings,
  population,
  land_area_km2,
  oferta_per_capita_100k,
  densidad_oferta_km2,
  rank_per_capita,
  rank_density,
  pctl_combined,
  semaforo_combined
from scored

