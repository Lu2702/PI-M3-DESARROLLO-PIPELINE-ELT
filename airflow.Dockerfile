FROM apache/airflow:2.9.3-python3.10

# 1) instala git 
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*


USER airflow

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.10

# Airflow + extractor (con constraints)
COPY requirements_airflow.txt /req_base.txt
RUN pip install --no-cache-dir -r /req_base.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# dbt SIN constraints 
COPY requirements_dbt.txt /req_dbt.txt
RUN pip install --no-cache-dir -r /req_dbt.txt
