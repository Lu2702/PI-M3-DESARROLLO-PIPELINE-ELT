#!/bin/sh
# sql/000_init.sh
# Crea las BDs del proyecto de forma idempotente (seguro si ya existen)

set -eu

DB_USER="${POSTGRES_USER:-postgres}"

# --- ab_nyc_dw ---
psql -v ON_ERROR_STOP=1 --username "$DB_USER" --dbname postgres \
  -tAc "SELECT 1 FROM pg_database WHERE datname='ab_nyc_dw'" | grep -q 1 || \
psql --username "$DB_USER" --dbname postgres \
  -c "CREATE DATABASE ab_nyc_dw OWNER \"$DB_USER\";"

# --- airflow ---
psql -v ON_ERROR_STOP=1 --username "$DB_USER" --dbname postgres \
  -tAc "SELECT 1 FROM pg_database WHERE datname='airflow'" | grep -q 1 || \
psql --username "$DB_USER" --dbname postgres \
  -c "CREATE DATABASE airflow OWNER \"$DB_USER\";"
