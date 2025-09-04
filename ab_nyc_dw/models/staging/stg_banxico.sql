-- models/staging/stg_banxico.sql
with src as (select * from {{ source('raw_ext','banxico_latest') }})
select
  date::date                as rate_date,
  usd_to_mxn::numeric(12,6) as usd_to_mxn
from src
where date is not null