{% snapshot listing_snapshot %}
  {{
    config(
      target_schema='snapshots',
      unique_key='listing_id_nat',
      strategy='check',
      check_cols=['host_id_nat','listing_name','borough_name','neighbourhood_name','room_type','latitude','longitude']
    )
  }}

  select distinct
    listing_id_nat,         -- id natural del listing
    listing_name,
    host_id_nat,
    borough_name,
    neighbourhood_name,
    room_type,
    latitude,
    longitude
  from {{ ref('stg_ab_nyc') }}

{% endsnapshot %}