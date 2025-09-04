#!/usr/bin/env bash
# scripts/update_latest_symlinks.sh
# Actualiza symlinks latest.csv dentro del contenedor de Postgres (Alpine/BusyBox friendly).

set -euo pipefail

echo "[symlinks] Actualizando latest.csv para AB_NYC, Banxico y Boroughs dentro del contenedor postgres..."

pick_latest () {
  SRC_DIR="$1"
  MOUNT_DIR="$2"
  SERVICE_NAME="${3:-postgres}"

  docker compose exec -T "$SERVICE_NAME" sh -lc '
    set -e
    SRC_DIR="'"$SRC_DIR"'"
    # Listar todos los CSV y obtener su mtime con stat (formato: "<epoch> <ruta>")
    # Nota: BusyBox stat soporta -c y %Y %n
    if [ ! -d "$SRC_DIR" ]; then
      echo "['"$(basename "$SRC_DIR")"'] Directorio no existe: $SRC_DIR" >&2
      exit 1
    fi

    # Construir lista; si no hay archivos, salir con mensaje claro
    FILES="$(find "$SRC_DIR" -type f -name "*.csv" 2>/dev/null || true)"
    if [ -z "$FILES" ]; then
      echo "['"$(basename "$SRC_DIR")"'] No se encontró ningún CSV en $SRC_DIR" >&2
      exit 1
    fi

    # Imprimir "<mtime> <path>" para cada archivo y quedarnos con el más reciente
    LAST_FILE="$(
      printf "%s\n" "$FILES" | while IFS= read -r f; do
        # Si un archivo desaparece entre find y stat, ignorarlo
        [ -f "$f" ] || continue
        stat -c "%Y %n" "$f" 2>/dev/null || true
      done | sort -nr | head -n1 | cut -d" " -f2-
    )"

    if [ -z "$LAST_FILE" ]; then
      echo "['"$(basename "$SRC_DIR")"'] No se pudo determinar el último CSV" >&2
      exit 1
    fi

    # Crear/actualizar symlink latest.csv junto a los archivos
    LN_DIR="$(dirname "$SRC_DIR")/$(basename "$SRC_DIR")"
    ln -sf "$LAST_FILE" "$LN_DIR/latest.csv"
    echo "['"$(basename "$SRC_DIR")"'] latest.csv -> $LAST_FILE"
  '
}

# AB_NYC
pick_latest "/data/raw/files/ab_nyc" "/data/raw/files" "postgres"

# BANXICO
pick_latest "/data/raw/files/banxico" "/data/raw/files" "postgres"

# BOROUGHS
pick_latest "/data/raw/files/nyc_boroughs" "/data/raw/files" "postgres"

echo "[symlinks] Listo."

