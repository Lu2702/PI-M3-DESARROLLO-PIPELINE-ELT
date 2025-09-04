from __future__ import annotations
import os, re
from datetime import datetime
from typing import Optional, Tuple, List

import requests
import pandas as pd
from bs4 import BeautifulSoup

import src.utils.config as config
from src.utils.logger import get_logger
from src.utils.paths import raw_files_dir

logger = get_logger(__name__)

EXPECTED_BOROUGHS = {"Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"}

# ---------------------- Helpers de limpieza ---------------------- #
def _to_number(s: str) -> Optional[float]:
    """Convierte celdas tipo '1,694,251[3]' o '59.13 km2' -> float."""
    if s is None:
        return None
    s = re.sub(r"\[[^\]]*\]", "", s)        # quita [1], [a], etc.
    s = s.replace(",", "")                   # quita separador miles
    m = re.search(r"\(([\d\.]+)\s*km2\)", s) # usa valor entre paréntesis en km2 si existe
    if m:
        try:
            return float(m.group(1))
        except Exception:
            pass
    s = re.sub(r"[^\d\.\-]", "", s)         # deja solo dígitos, '.', '-'
    try:
        return float(s) if s else None
    except Exception:
        return None

def _clean_borough(name: str) -> str:
    """Normaliza 'The Bronx' -> 'Bronx'."""
    return (name or "").strip().replace("The Bronx", "Bronx")

# ---------------------- Detección de headers ---------------------- #
def _collect_header_rows(table: BeautifulSoup) -> List[Tuple[int, list[str]]]:
    """Todas las filas que son solo <th> (encabezados puros)."""
    rows: List[Tuple[int, list[str]]] = []
    for i, tr in enumerate(table.find_all("tr")):
        ths = tr.find_all("th")
        tds = tr.find_all("td")
        if ths and not tds:  # fila de encabezado puro
            rows.append((i, [th.get_text(strip=True) for th in ths]))
    return rows

def _table_matches(header_rows: List[Tuple[int, list[str]]]) -> bool:
    """
    Acepta tabla si en el CONJUNTO de filas de encabezado aparece:
      - 'borough'
      - y ('population' o 'census')
    """
    all_text = " ".join(" ".join(h).lower() for _, h in header_rows)
    return ("borough" in all_text) and (("population" in all_text) or ("census" in all_text))

def _find_table_by_leaf_headers(soup: BeautifulSoup) -> Tuple[BeautifulSoup, int, list[str]]:
    """
    Entre todas las 'wikitable', elige la que cumpla _table_matches().
    Usa la ÚLTIMA fila de <th> como encabezados 'hoja' (alinean con los <td>).
    Devuelve (tabla, idx_fila_header, headers_leaf).
    """
    for t in soup.select("table.wikitable"):
        header_rows = _collect_header_rows(t)
        if not header_rows:
            continue
        if _table_matches(header_rows):
            idx_leaf, headers_leaf = header_rows[-1]
            return t, idx_leaf, headers_leaf
    raise RuntimeError("No encontré una 'wikitable' con Borough y Population/Census en los encabezados.")

# ---------------------- Runner ---------------------- #
def run_scraper_nyc_boroughs(
    source_name: str | None = None,
    url: str | None = None
) -> Tuple[str, pd.DataFrame]:
    """
    Descarga Wikipedia (boroughs NYC), detecta la tabla correcta (headers multinivel),
    extrae/limpia y guarda CSV en RAW. Devuelve (ruta_salida, DataFrame).
    """
    source_name = source_name or config.SCRAPER_NYC_SOURCE_NAME
    url         = url         or config.SCRAPER_NYC_URL

    logger.info(f"[{source_name}] GET {url}")
    resp = requests.get(
        url,
        headers={"User-Agent": config.HTTP_USER_AGENT},
        timeout=config.HTTP_TIMEOUT,
    )
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Ubica tabla y toma SOLO la última fila de encabezados (<th>)
    table, header_idx, headers_leaf = _find_table_by_leaf_headers(soup)
    logger.info(f"[{source_name}] Encabezados (leaf): {headers_leaf}")

    # Recorre filas de datos a partir de la fila siguiente al header
    data_rows: list[list[str]] = []
    trs = table.find_all("tr")
    for tr in trs[header_idx + 1 :]:
        if not tr.find_all("td"):
            continue  # salta filas que no son de datos
        cells = tr.find_all(["td", "th"])
        texts = [c.get_text(strip=True) for c in cells][: len(headers_leaf)]
        if texts:
            data_rows.append(texts)

    if not data_rows:
        raise RuntimeError("No se encontraron filas de datos bajo el encabezado detectado.")

    df_raw = pd.DataFrame(data_rows, columns=headers_leaf)
    logger.info(f"[{source_name}] Filas crudas: {len(df_raw)}")

    # Normaliza nombres de columnas (SIN GDP)
    rename: dict[str, str] = {}
    for c in df_raw.columns:
        lc = c.lower()
        if "borough" in lc:
            rename[c] = "borough"
        elif ("population" in lc) or ("census" in lc):
            rename[c] = "population"
        elif (("land" in lc or "area" in lc or "square" in lc) and "km" in lc):
            # 'land area (km2)', 'area (km2)', 'square km'
            rename[c] = "land_area_km2"
        elif ("density" in lc and "km" in lc) or ("people/sq. km" in lc) or ("per km" in lc):
            rename[c] = "density_km2"

    df = df_raw.rename(columns=rename)

    # Conserva solo columnas útiles
    keep = [c for c in ["borough", "population", "land_area_km2", "density_km2"] if c in df.columns]
    df = df[keep].copy()

    if "borough" not in df.columns:
        raise RuntimeError("No se encontró 'borough' tras normalizar.")

    # Limpieza de valores
    df["borough"] = df["borough"].map(_clean_borough)
    for col in ["population", "land_area_km2", "density_km2"]:
        if col in df.columns:
            df[col] = df[col].apply(_to_number)

    # Filtrado y validación suave
    df = df[df["borough"].notna()].drop_duplicates().reset_index(drop=True)
    df = df[df["borough"].isin(EXPECTED_BOROUGHS)].reset_index(drop=True)

    found = set(df["borough"].tolist())
    if found != EXPECTED_BOROUGHS:
        logger.warning(f"[{source_name}] Boroughs encontrados={sorted(found)}; esperado={sorted(EXPECTED_BOROUGHS)}")

    # Guardado en RAW
    now = datetime.utcnow()
    out_dir = raw_files_dir(source_name, now)  # data/raw/files/<fuente>/YYYY/MM/DD
    os.makedirs(out_dir, exist_ok=True)
    filename = f"{source_name}_{now.strftime('%Y%m%dT%H%M%SZ')}.csv"
    out_path = os.path.join(out_dir, filename)

    df.to_csv(out_path, index=False, encoding="utf-8", lineterminator="\n")
    logger.info(f"[{source_name}] Guardado en: {out_path} (rows={len(df)})")

    return out_path, df