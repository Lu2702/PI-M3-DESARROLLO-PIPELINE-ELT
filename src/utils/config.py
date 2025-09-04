"""
Módulo: src/utils/config.py
Propósito:
    - Cargar automáticamente las variables del archivo .env al entorno del proceso.
    - Exponer valores de configuración con defaults seguros para todo el proyecto.
    - Centralizar decisiones de configuración (rutas, niveles de log, flags de ejecución).

Notas importantes:
    * Este módulo ejecuta load_dotenv() al importarse; por eso, en src/main.py
      basta con hacer:  `import src.utils.config as config`  para que .env
      quede cargado antes de correr extractores.
"""

from __future__ import annotations

import os
from dotenv import load_dotenv

# Carga de variables de entorno desde .env (efecto secundario del import)
load_dotenv()

# ----------------------------
# Helpers de lectura de entorno
# ----------------------------
def env(key: str, default: str = "") -> str:
    """
    Devuelve la variable de entorno `key` como string.
    Si no existe, retorna `default`. El valor se devuelve limpio (strip()).
    """
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v


def env_int(key: str, default: int = 0) -> int:
    """
    Devuelve la variable de entorno `key` convertida a int.
    Tolera valores no numéricos devolviendo `default`.
    """
    raw = os.getenv(key, str(default))
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return default


# ----------------------------
# Variables de configuración del proyecto
# ----------------------------

# Nivel de logging (DEBUG/INFO/WARNING/ERROR/CRITICAL)
LOG_LEVEL: str = env("LOG_LEVEL", "INFO")

# Base de la capa RAW 
RAW_DIR: str = env("RAW_DIR", "./data/raw")

# CSV local de ejemplo (inputs → RAW)
LOCAL_CSV_PATH: str        = env("LOCAL_CSV_PATH", "./inputs/AB_NYC.csv")
LOCAL_CSV_SOURCE_NAME: str = env("LOCAL_CSV_SOURCE_NAME", "ab_nyc")

# Banxico
BANXICO_SERIES_ID: str = env("BANXICO_SERIES_ID", "SF43718")
BANXICO_TOKEN: str = env("BANXICO_TOKEN", "")

# Política de fallo global: 0 = soft-fail (continúa), 1 = fail-fast (termina proceso con error)
STRICT_MODE: int = env_int("STRICT_MODE", 0)

# ----------------------------
# Scraper de boroughs (Wikipedia)
# ----------------------------
# Flag para encender/apagar el scraper sin tocar código
RUN_SCRAPER_NYC: str = env("RUN_SCRAPER_NYC", "0")  # "1" = ejecutar, "0" = saltar

# Nombre lógico de la fuente (se usa en las rutas de RAW)
SCRAPER_NYC_SOURCE_NAME: str = env("SCRAPER_NYC_SOURCE_NAME", "nyc_boroughs")

# URL objetivo (Wikipedia)
SCRAPER_NYC_URL: str = env(
    "SCRAPER_NYC_URL",
    "https://en.wikipedia.org/wiki/Boroughs_of_New_York_City"
)

# Parámetros HTTP (para requests.get del scraping)
HTTP_USER_AGENT: str = env("HTTP_USER_AGENT", "Integrador-ETL/1.0 (+educativo)")
HTTP_TIMEOUT: int    = env_int("HTTP_TIMEOUT", 30)


if __name__ == "__main__":
    print("LOG_LEVEL       =", LOG_LEVEL)
    print("RAW_DIR         =", RAW_DIR)
    print("LOCAL_CSV_PATH  =", LOCAL_CSV_PATH)
    print("LOCAL_CSV_NAME  =", LOCAL_CSV_SOURCE_NAME)
    print("BANXICO_SERIES  =", BANXICO_SERIES_ID)
    print("STRICT_MODE     =", STRICT_MODE)
    print("RUN_SCRAPER_NYC =", RUN_SCRAPER_NYC)
    print("SCRAPER_NAME    =", SCRAPER_NYC_SOURCE_NAME)
    print("SCRAPER_URL     =", SCRAPER_NYC_URL)
    print("HTTP_USER_AGENT =", HTTP_USER_AGENT)
    print("HTTP_TIMEOUT    =", HTTP_TIMEOUT)