{% snapshot host_snapshot %}
  {{
    config(
      target_schema='snapshots',
      unique_key='host_id_nat',
      strategy='check',
      check_cols=['host_name','calculated_host_listings_count']
    )
  }}

  select distinct
    host_id_nat,
    host_name,
    calculated_host_listings_count
  from {{ ref('stg_ab_nyc') }}

{% endsnapshot %}