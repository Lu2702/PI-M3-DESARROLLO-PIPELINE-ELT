-- models/staging/stg_boroughs.sql
with src as (select * from {{ source('raw_ext','nyc_boroughs_latest') }})
select
  initcap(borough)              as borough_name,
  round(population)::numeric(18,0)           as population,
  land_area_km2::numeric(12,3) as land_area_km2,
  density::numeric(12,3)       as density_km2
from src
where borough is not null
