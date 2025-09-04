"""
Propósito:
    Copiar el CSV original AB_NYC hacia la capa RAW **solo si cambió** por contenido,
    manteniendo trazabilidad diaria en un manifest JSONL.

Convención:
    raw/files/<fuente>/YYYY/MM/DD/<fuente>_<timestamp>.csv

Requisitos:
    - .env: LOCAL_CSV_PATH, LOCAL_CSV_SOURCE_NAME, RAW_DIR, LOG_LEVEL
    - utils.logger.get_logger
    - utils.paths.raw_files_dir
    - utils.verify: md5sum, find_last_record_by_md5, register_file, register_reference
"""

import os
import shutil
from datetime import datetime
import pandas as pd

from src.utils.logger import get_logger
from src.utils.config import LOCAL_CSV_PATH, LOCAL_CSV_SOURCE_NAME
from src.utils.paths import raw_files_dir
from src.utils.verify import (
    md5sum,
    find_last_record_by_md5,
    register_file,
    register_reference,
)

logger = get_logger(__name__)


def _copy_to_raw(src_path: str, ts_utc: datetime) -> str:
    """
    Copia el archivo a RAW con convención de fecha + timestamp UTC.
    Devuelve la ruta final escrita.
    """
    out_dir = raw_files_dir(LOCAL_CSV_SOURCE_NAME, ts_utc)  # raw/files/<fuente>/YYYY/MM/DD/
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(
        out_dir,
        f"{LOCAL_CSV_SOURCE_NAME}_{ts_utc.strftime('%Y%m%dT%H%M%SZ')}.csv"
    )
    shutil.copy2(src_path, out_path)
    return out_path


def run() -> tuple[str, pd.DataFrame]:
    """
    Ejecuta el flujo "CSV → RAW" sin duplicar si el contenido no cambió.
    Devuelve (out_path, df) para DQ posteriores.
    """
    logger.info("=== EXTRACT CSV LOCAL (no dup if same MD5) ===")

    # 1) Validar existencia del archivo fuente
    if not os.path.exists(LOCAL_CSV_PATH):
        raise FileNotFoundError(f"No encuentro el CSV original: {LOCAL_CSV_PATH}")

    # 2) Leer CSV fuente (sin transformar) — para DQ posteriores
    df = pd.read_csv(LOCAL_CSV_PATH)

    # 3) Calcular MD5 del fuente y decidir si copiamos o referenciamos
    current_md5 = md5sum(LOCAL_CSV_PATH)
    last = find_last_record_by_md5(current_md5)

    logger.info(f"[DBG] md5_fuente={current_md5} | last_rec_path={last['path'] if last else 'None'}")

    if last:
        # Sin cambios → NO copiar; registrar referencia diaria y devolver path previo
        out_path = last["path"]
        register_reference(source=LOCAL_CSV_SOURCE_NAME, path=out_path, md5=current_md5)
        logger.info(f"[CSV] Sin cambios (MD5 igual). Reutilizando path: {out_path}")
        logger.info(f"[CSV] Filas (del CSV fuente): {len(df)}")
        return out_path, df

    # 4) Con cambios → copiar a RAW y registrar archivo en manifest
    now_utc = datetime.utcnow()
    out_path = _copy_to_raw(LOCAL_CSV_PATH, now_utc)
    register_file(path=out_path, source=LOCAL_CSV_SOURCE_NAME, md5=current_md5)
    logger.info(f"[CSV] Copiado a RAW → {out_path} | filas={len(df)}")
    return out_path, df


if __name__ == "__main__":
    run()
