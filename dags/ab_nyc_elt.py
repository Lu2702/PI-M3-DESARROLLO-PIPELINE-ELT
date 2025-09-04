# dags/ab_nyc_elt.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

DEFAULT_ARGS = {"retries": 1, "retry_delay": timedelta(minutes=3), "depends_on_past": False}

with DAG(
    dag_id="ab_nyc_elt",
    start_date=datetime(2025, 9, 1),
    schedule=None,          
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ab_nyc", "local"],
) as dag:

    # 1) Extrae datos RAW con tu paquete Python
    extract_raw = BashOperator(
        task_id="extract_raw",
        bash_command=(
            "set -euo pipefail; "
            "cd /opt/airflow/repo; "
            "if [ -f .env ]; then set -a; . ./.env; set +a; fi; "
            "export PYTHONPATH=/opt/airflow/repo:${PYTHONPATH:-}; "
            "echo 'RUN: python -m src.main --date {{ ds }}'; "
            "python -m src.main --date {{ ds }}"
        ),
        env={"PYTHONPATH": "/opt/airflow/repo"},
    )

    # 2) Actualiza latest.csv dentro de ./data
    
    update_symlinks = BashOperator(
        task_id="update_symlinks",
        bash_command="set -euo pipefail; /usr/bin/env bash /opt/airflow/repo/scripts/update_latest_symlinks.sh; ",
    )

    # 3) Construcción con dbt
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "set -euo pipefail; "
            "mkdir -p /tmp/dbt_logs; "
            "export DBT_LOG_PATH=/tmp/dbt_logs; "
            "cd /opt/airflow/repo/ab_nyc_dw && "
            "dbt deps && "
            "dbt build --profiles-dir /opt/airflow/repo/ab_nyc_dw"
        ),
        env={
            "DBT_PROFILES_DIR": "/opt/airflow/repo/ab_nyc_dw",
            "DBT_LOG_PATH": "/tmp/dbt_logs",
        },
    )

    # 4) Chequeo en GOLD/SILVER 
    gold_has_rows = SQLCheckOperator(
        task_id="gold_has_rows",
        conn_id="postgres_dw",
        sql="""
            SELECT COUNT(*) >= 1
            FROM public_silver.fct_listing_snapshot
            WHERE snapshot_date_key = (
              SELECT MAX(snapshot_date_key)
              FROM public_silver.fct_listing_snapshot
            );
        """,
    )

    extract_raw >> update_symlinks >> dbt_build >> gold_has_rows



