#!/usr/bin/env bash
set -Eeuo pipefail

# 1) Migrar metadatos
airflow db migrate

# 2) Validaciones: modo estricto por defecto

ALLOW_RANDOM="${AIRFLOW_ALLOW_RANDOM_ADMIN_PASSWORD:-false}"

need() {
  local var="$1"
  if [ -z "${!var:-}" ]; then
    echo "[airflow-init] ERROR: falta la variable ${var}" >&2
    echo "[airflow-init] Sugerencia: define ${var} en airflow.env" >&2
    exit 1
  fi
}

need AIRFLOW_ADMIN_USER
need AIRFLOW_ADMIN_EMAIL
need AIRFLOW_ADMIN_FIRSTNAME
need AIRFLOW_ADMIN_LASTNAME


if [ -z "${AIRFLOW_ADMIN_PASSWORD:-}" ]; then
  if [ "$ALLOW_RANDOM" = "true" ]; then
    AIRFLOW_ADMIN_PASSWORD="$(python - <<'PY'
import secrets, string
alphabet = string.ascii_letters + string.digits + "!@#$%^&*()-_=+"
print("".join(secrets.choice(alphabet) for _ in range(24)))
PY
)"
   
    echo "[airflow-init] Se generó una contraseña aleatoria para ${AIRFLOW_ADMIN_USER}:" \
         > /opt/airflow/logs/_bootstrap_admin_password.txt
    echo "$AIRFLOW_ADMIN_PASSWORD" >> /opt/airflow/logs/_bootstrap_admin_password.txt
    chmod 600 /opt/airflow/logs/_bootstrap_admin_password.txt || true
    echo "[airflow-init] Password aleatoria guardada en /opt/airflow/logs/_bootstrap_admin_password.txt"
  else
    echo "[airflow-init] ERROR: falta AIRFLOW_ADMIN_PASSWORD (o habilita AIRFLOW_ALLOW_RANDOM_ADMIN_PASSWORD=true)" >&2
    exit 1
  fi
fi


if ! airflow users list | awk '{print $2}' | grep -qw "${AIRFLOW_ADMIN_USER}"; then
  airflow users create --role Admin \
    --username  "${AIRFLOW_ADMIN_USER}" \
    --password  "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname "${AIRFLOW_ADMIN_FIRSTNAME}" \
    --lastname  "${AIRFLOW_ADMIN_LASTNAME}" \
    --email     "${AIRFLOW_ADMIN_EMAIL}"
else
  echo "[airflow-init] El usuario '${AIRFLOW_ADMIN_USER}' ya existe. OK."
fi

echo "[airflow-init] Done."
