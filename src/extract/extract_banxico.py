"""
Módulo: src/extract/extract_banxico.py

Propósito
---------
Extractor del tipo de cambio desde la API SIE de Banxico.

Flujo:
  1) Construye la URL (path) para una serie SIE (p.ej. FIX: SF43718).
     - Si se especifica rango: /datos/YYYY-MM-DD/YYYY-MM-DD
     - Si no hay fechas:       /datos/oportuno (último dato disponible)
  2) Llama la API con requests, pidiendo JSON (se envía "format=json" y "token" en params).
  3) Normaliza la respuesta a un DataFrame con columnas:
       - fecha: datetime64[ns]
       - valor: float
  4) Guarda un CSV en RAW con la convención:
       raw/files/banxico/YYYY/MM/DD/banxico_<serie>_<timestamp>.csv
  5) Registra logs en consola y en archivo (via utils/logger.py)

Uso (desde el orquestador main):
    from src.extract.extract_banxico import run
    out_path, df = run()  # usa valores de .env
"""

from __future__ import annotations

import os
from datetime import datetime, date
from typing import Optional, Tuple

import requests
import pandas as pd

from src.utils.logger import get_logger
from src.utils.paths import raw_files_dir

logger = get_logger(__name__)


# Excepción específica del módulo
class BanxicoError(Exception):
    """Errores propios del extractor de Banxico (config, HTTP, parseo, etc.)."""
    pass



# URL builder (solo path, sin querystring)

def build_url(
    series_id: str,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> str:
    """
    Construye la URL base (path) del endpoint SIE de Banxico para una serie.

    - Con rango de fechas: /series/<serie>/datos/YYYY-MM-DD/YYYY-MM-DD
    - Sin fechas:          /series/<serie>/datos/oportuno

    Nota: el token y el formato se envían aparte con `params` al hacer requests.get().

    Parámetros
    ----------
    series_id : str
        Id de la serie SIE (p. ej., 'SF43718' para FIX USD→MXN).
    date_from, date_to : date | None
        Rango de fechas. Deben venir ambas o ninguna.

    Retorna
    -------
    str
        URL (path completo) listo para consumir con requests junto con `params`.
    """
    base = "https://www.banxico.org.mx/SieAPIRest/service/v1/series"

    # Evita URLs inválidas si llega solo una de las dos fechas
    if (date_from and not date_to) or (date_to and not date_from):
        raise BanxicoError("Debes proporcionar ambas fechas o ninguna.")

    if date_from and date_to:
        return f"{base}/{series_id}/datos/{date_from:%Y-%m-%d}/{date_to:%Y-%m-%d}"

    # Sin fechas → último dato disponible (oportuno)
    return f"{base}/{series_id}/datos/oportuno"



# Extractor principal (run)

def run(
    series_id: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> Tuple[str, pd.DataFrame]:
    """
    Llama la API SIE de Banxico, normaliza y persiste en RAW.

    Parámetros
    ----------
    series_id : str | None
        Serie a consultar. Si no se pasa, se usa BANXICO_SERIES_ID de .env (default SF43718).
    date_from, date_to : date | None
        Opcional: rango de fechas. Si no se especifica, usa "oportuno".

    Retorna
    -------
    (out_path, df) : (str, pandas.DataFrame)
        - out_path : ruta del CSV guardado en RAW
        - df       : DataFrame con ['fecha', 'valor'], ordenado por fecha ascendente
    """
    logger.info("=== EXTRACT API BANXICO ===")

    # 1) Token (limpio) y serie desde .env (o parámetro)
    raw_token = os.getenv("BANXICO_TOKEN", "")
    token = raw_token.strip().strip("\"'")  # quita espacios/comillas que rompen la URL
    if not token:
        raise BanxicoError("Falta BANXICO_TOKEN en .env")

    sid = series_id or os.getenv("BANXICO_SERIES_ID", "SF43718")

    # 2) Construcción de URL (solo path). Query se envía en params
    url = build_url(sid, date_from=date_from, date_to=date_to)
    params  = {"token": token, "mediaType": "json"}
    headers = {"Accept": "application/json"}

    # (debug seguro) ver URL final que usará requests, enmascarando token
    try:
        prep = requests.Request("GET", url, params=params, headers=headers).prepare()
        safe = prep.url.replace(token, token[:4] + "..." + token[-4:])
        logger.debug(f"URL final: {safe}")
    except Exception:
        pass

    # 3) Llamada HTTP con manejo de error
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=20)
    except requests.RequestException as e:
        raise BanxicoError(f"Error de red al llamar Banxico: {e}") from e

    if resp.status_code != 200:
        snippet = (resp.text or "")[:300]
        raise BanxicoError(f"HTTP {resp.status_code} {resp.reason}. Respuesta: {snippet!r}")

    # 4) Parseo y normalización a DataFrame
    try:
        payload = resp.json()
        # Estructura SIE: {"bmx":{"series":[{"idSerie":"...","datos":[{"fecha":"dd/mm/aaaa","dato":"xx.xx"}, ...]}]}}
        datos = payload["bmx"]["series"][0]["datos"]
        df = pd.DataFrame(datos)
    except Exception as e:
        logger.error("Respuesta inesperada al parsear JSON de Banxico", exc_info=True)
        raise BanxicoError(f"Formato inesperado en la respuesta: {e}") from e

    if df.empty:
        raise BanxicoError("La respuesta de Banxico vino vacía")

    # Tipado/limpieza
    df["fecha"] = pd.to_datetime(df["fecha"], format="%d/%m/%Y", errors="coerce")
    df["valor"] = pd.to_numeric(df["dato"].str.replace(",", ""), errors="coerce")
    df = df.drop(columns=["dato"]).sort_values("fecha").reset_index(drop=True)

    # 5) Persistencia en RAW
    out_dir = raw_files_dir(source="banxico", dt=datetime.utcnow())
    os.makedirs(out_dir, exist_ok=True)

    out_name = f"banxico_{sid}_{datetime.utcnow():%Y%m%dT%H%M%SZ}.csv"
    out_path = os.path.join(out_dir, out_name)
    df.to_csv(out_path, index=False, encoding="utf-8")

    logger.info(f"Banxico {sid}: {len(df)} filas → {out_path}")
    logger.info("=== FIN EXTRACT API BANXICO ===")

    return out_path, df