"""
Archivo: src/main.py
Descripción:
    Punto de entrada del pipeline de extracción hacia la capa RAW.

      El extractor decide: si el MD5 ya existe → NO copia y registra referencia;
      si es nuevo → copia y registra archivo.
    - Mantenemos _post_write() para Banxico (sí genera archivo diario).

Ejecución:
    python -m src.main
"""

from __future__ import annotations

# Carga .env por side-effect (load_dotenv vive en config.py)
import src.utils.config as config  

import os
import json
import sys
from datetime import datetime

from src.utils.logger import get_logger
from src.extract.extract_csv import run as run_csv
from src.extract.extract_banxico import run as run_banxico, BanxicoError
from src.extract.web_scraping_nyc import run_scraper_nyc_boroughs

# Helpers de verificación / manifest
from src.utils.verify import file_exists_and_size, md5sum, is_duplicate, register_file

# === IMPORTS quality ===
from src.utils.quality import (
    validate_ab_nyc,
    validate_banxico_raw,
    validate_nyc_boroughs,
)

logger = get_logger(__name__)


def _write_status(stage: str, error: str) -> str:
    """
    Crea un artefacto JSON de estatus cuando un step hace soft-fail.
    Guarda en: data/status/extract/<stage>/<stage>_<timestamp>.json
    """
    now = datetime.utcnow()

    out_dir = os.path.join("data", "status", "extract", stage)  # ← solo por carpeta de stage
    os.makedirs(out_dir, exist_ok=True)

    status_path = os.path.join(out_dir, f"{stage}_{now:%Y%m%dT%H%M%SZ}.json")

    payload = {
        "stage": stage,                # p.ej. "banxico" o "scraper_nyc"
        "status": "failed",            # este helper lo usamos cuando falla (soft-fail)
        "error": error,                # mensaje de error
        "ts_utc": f"{now:%Y-%m-%dT%H:%M:%SZ}",
    }

    with open(status_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return status_path


def _post_write(out_path: str, source: str, min_bytes: int = 10) -> None:
    """
    Post-escritura:
      1) existencia y tamaño mínimo
      2) hash MD5 del contenido
      3) si hash ya existe en manifest → duplicado → borra archivo NUEVO
      4) si no, registra en manifest para futuras corridas
    """
    # 1) existencia / tamaño
    if not file_exists_and_size(out_path, min_bytes=min_bytes):
        logger.error(f"[verify] size_or_exist_fail source={source} path={out_path} min={min_bytes}")
        return

    # 2) hash
    h = md5sum(out_path)

    # 3) duplicado por contenido (global por md5)
    if is_duplicate(out_path, h):
        logger.warning(f"[verify] duplicate_detected source={source} path={out_path} md5={h}")
        try:
            os.remove(out_path)
            logger.warning(f"[dedupe] removed source={source} path={out_path} md5={h}")
        except Exception as e:
            logger.error(f"[dedupe] remove_failed source={source} path={out_path} err={e}")
        return

    # 4) registrar en manifest
    register_file(out_path, source=source, md5=h)
    logger.info(f"[verify] registered source={source} path={out_path} md5={h}")


def main() -> None:
    """
    Orquesta la ejecución de los extractores hacia RAW + validaciones DQ.
    """
    dq_strict = int(getattr(config, "DQ_STRICT", 0))  # 0 = solo reporta, 1 = aborta si falla DQ

    # 1) CSV local (AB_NYC)
    logger.info("=== PIPELINE: CSV → RAW ===")
    csv_out, csv_df = run_csv()
    logger.info(f"CSV OK | filas={len(csv_df)} | path={csv_out}")
    # IMPORTANTE : NO usamos _post_write() para AB_NYC.
    # El extractor ya decidió copiar o registrar referencia en el manifest.

    # --- DQ CSV (AB_NYC)
    ok_csv, rep_csv_path, _ = validate_ab_nyc(csv_df)
    logger.info(f"[DQ] ab_nyc {'OK' if ok_csv else 'FAIL'} | report={rep_csv_path}")
    if not ok_csv and dq_strict == 1:
        logger.error("[DQ] estricto activado: abortando por DQ en ab_nyc (CSV).")
        sys.exit(1)

    logger.info("=== FIN CSV → RAW ===")

    # 2) API Banxico
    logger.info("=== PIPELINE: BANXICO → RAW ===")
    try:
        bnx_out, bnx_df = run_banxico()
        logger.info(f"Banxico OK | filas={len(bnx_df)} | path={bnx_out}")
        banxico_source = getattr(config, "BANXICO_SOURCE_NAME", "banxico")
        _post_write(bnx_out, source=banxico_source, min_bytes=5)

        # --- DQ BANXICO
        ok_bnx, rep_bnx_path, _ = validate_banxico_raw(bnx_df)
        logger.info(f"[DQ] banxico {'OK' if ok_bnx else 'FAIL'} | report={rep_bnx_path}")
        if not ok_bnx and dq_strict == 1:
            logger.error("[DQ] estricto activado: abortando por DQ en Banxico.")
            sys.exit(1)

    except BanxicoError as e:
        logger.error(f"BANXICO ERROR: {e}")
        if getattr(config, "STRICT_MODE", 0) == 1:
            sys.exit(1)
        else:
            status_path = _write_status(stage="banxico", error=str(e))
            logger.warning(f"Banxico marcado FAILED (soft). Status: {status_path}")
    logger.info("=== FIN BANXICO → RAW ===")

    # 3) Scraper Wikipedia (NYC boroughs)
    if getattr(config, "RUN_SCRAPER_NYC", "0") == "1":
        logger.info("=== PIPELINE: SCRAPER NYC (Wikipedia) → RAW ===")
        try:
            nyc_out, nyc_df = run_scraper_nyc_boroughs()  # usa config internamente
            logger.info(
                f"Scraper NYC OK | filas={len(nyc_df)} | cols={list(nyc_df.columns)} | path={nyc_out}"
            )
            _post_write(nyc_out, source=getattr(config, "SCRAPER_NYC_SOURCE_NAME", "nyc_boroughs"), min_bytes=10)

            # --- DQ NYC BOROUGHS
            ok_nyc, rep_nyc_path, _ = validate_nyc_boroughs(nyc_df)
            logger.info(f"[DQ] nyc_boroughs {'OK' if ok_nyc else 'FAIL'} | report={rep_nyc_path}")
            if not ok_nyc and dq_strict == 1:
                logger.error("[DQ] estricto activado: abortando por DQ en NYC boroughs.")
                sys.exit(1)

        except Exception as e:
            logger.error(f"SCRAPER_NYC ERROR: {e}")
            if getattr(config, "STRICT_MODE", 0) == 1:
                sys.exit(1)
            else:
                status_path = _write_status(stage="scraper_nyc", error=str(e))
                logger.warning(f"Scraper NYC marcado FAILED (soft). Status: {status_path}")
        logger.info("=== FIN SCRAPER NYC → RAW ===")
    else:
        logger.info("SCRAPER NYC desactivado (RUN_SCRAPER_NYC != '1').")

    logger.info("=== PIPELINE RAW: end ===")


if __name__ == "__main__":
    main()
