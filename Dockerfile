# Dockerfile
FROM python:3.11-slim

# Evita prompts interactivos
ENV DEBIAN_FRONTEND=noninteractive PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    git build-essential libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Instala dbt-core + adaptador Postgres 
RUN pip install "dbt-core==1.7.*" "dbt-postgres==1.7.*"

# Crea carpeta de trabajo 
WORKDIR /app/ab_nyc_dw

