-- sql/raw_ext_foreign_tables.sql
-- Crea el esquema y las foreign tables que apuntan a los latest.csv en /data/raw/files
-- Notas:
-- - Se usa file_fdw para leer CSV sin copiarlos a Postgres 
-- - La opción `null ''` hace que los campos vacíos en el CSV se lean como NULL,
--   útil para columnas date/numeric con valores en blanco.

-- sql/010_raw_ext_foreign_tables.sql
-- Crea/asegura FOREIGN TABLES que apuntan a /data/raw/files/*/latest.csv

CREATE SCHEMA IF NOT EXISTS raw_ext;
CREATE EXTENSION IF NOT EXISTS file_fdw;

-- Asegura el servidor (mismo nombre que usas ya: csv_server)
DO $do$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_foreign_server WHERE srvname = 'csv_server') THEN
    CREATE SERVER csv_server FOREIGN DATA WRAPPER file_fdw;
  END IF;
END
$do$;

-- =========================
-- AB_NYC 
-- =========================
DO $do$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='raw_ext' AND table_name='ab_nyc_latest'
  ) THEN
    CREATE FOREIGN TABLE raw_ext.ab_nyc_latest (
      id                             bigint,
      name                           text,
      host_id                        bigint,
      host_name                      text,
      neighbourhood_group            text,
      neighbourhood                  text,
      latitude                       double precision,
      longitude                      double precision,
      room_type                      text,
      price                          numeric,
      minimum_nights                 int,
      number_of_reviews              int,
      last_review                    date,
      reviews_per_month              numeric,
      calculated_host_listings_count int,
      availability_365               int
    )
    SERVER csv_server
    OPTIONS (
      filename '/data/raw/files/ab_nyc/latest.csv',
      format   'csv',
      header   'true',
      null     ''
    );
  ELSE
    -- Si ya existe, asegura/actualiza opciones (no borres vistas dependientes)
    ALTER FOREIGN TABLE raw_ext.ab_nyc_latest
      OPTIONS (
        SET filename '/data/raw/files/ab_nyc/latest.csv',
        SET format   'csv',
        SET header   'true',
        SET null     ''
      );
  END IF;
END
$do$;

-- =========================
-- BANXICO 
-- =========================
DO $do$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='raw_ext' AND table_name='banxico_latest'
  ) THEN
    CREATE FOREIGN TABLE raw_ext.banxico_latest (
      date       date,
      usd_to_mxn numeric
    )
    SERVER csv_server
    OPTIONS (
      filename '/data/raw/files/banxico/latest.csv',
      format   'csv',
      header   'true',
      null     ''
    );
  ELSE
    ALTER FOREIGN TABLE raw_ext.banxico_latest
      OPTIONS (
        SET filename '/data/raw/files/banxico/latest.csv',
        SET format   'csv',
        SET header   'true',
        SET null     ''
      );
  END IF;
END
$do$;

-- =========================
-- NYC BOROUGHS 
-- =========================
DO $do$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='raw_ext' AND table_name='nyc_boroughs_latest'
  ) THEN
    CREATE FOREIGN TABLE raw_ext.nyc_boroughs_latest (
      borough       text,
      population    numeric,
      land_area_km2 numeric,
      density       numeric
    )
    SERVER csv_server
    OPTIONS (
      filename '/data/raw/files/nyc_boroughs/latest.csv',
      format   'csv',
      header   'true',
      null     ''
    );
  ELSE
    ALTER FOREIGN TABLE raw_ext.nyc_boroughs_latest
      OPTIONS (
        SET filename '/data/raw/files/nyc_boroughs/latest.csv',
        SET format   'csv',
        SET header   'true',
        SET null     ''
      );
  END IF;
END
$do$;
