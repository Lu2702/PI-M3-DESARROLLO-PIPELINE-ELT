# src/utils/quality.py
"""
Validación de calidad de los datos
- Define "reglas" por columna (tipo, nulos, rangos, unicidad).
- Aplica esas reglas a un DataFrame.
- Genera un REPORTE JSON en data/status/quality/.
- Devuelve (ok, ruta_del_reporte, dict_con_el_reporte).
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional, Sequence, Dict, List, Tuple
import os, json, re
from datetime import datetime
import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

#Modelado de reglas por columna 
@dataclass
class ColumnRule:
    name: str #Nombre exacto esperado 
    dtype: str # tipado objetivo
    required: bool = True #si la columna debe existir 
    allow_nulls: bool = False # Si se permirten NaN/ None
    unique: bool = False #Si todos los valores deben ser unicos 
    min_value: Optional[float] = None #Rango numerico 
    max_value: Optional[float] = None #Rango numerico 
    allowed_values: Optional[Sequence[Any]] = None #Lista vacia para especificar ej categorias validas 
    regex: Optional[str] = None # Patron para validar formato de strings 
    min_len: Optional[int] = None #Rango de longitud para strings
    max_len: Optional[int] = None #Rango de longitud para strings  

#Esquema del data set completo
@dataclass
class DatasetSchema:
    source: str #Etiqueta del origen 
    rules: List[ColumnRule] #Modelo de reglas por columna
    required_rows_min: int = 1 #Umbral de data set vacio 
    exact_row_count: Optional[int] = None # Si se espera una cantidad exacta de filas 

#============== HELPERS ============
# forzar cada columna a su tipo antes de validar reglas.
def _coerce_series(s:pd.Series, dtype: str) -> pd.Series:
    if dtype == "int":
        return pd.to_numeric(s, errors= "coerce").astype("Int64")
    if dtype == "float":
        return pd.to_numeric(s, errors="coerce")
    if dtype == "date":
        return pd.to_datetime(s, errors="coerce", utc=False).dt.date
    return s.astype("string").str.strip()

#Separar “nulos que ya estaban” de “nulos creados por tipado malo”.
def _coerce_types(df: pd.DataFrame, schema: DatasetSchema)-> Dict[str, int]:
    conv_errors: Dict[str, int] = {}
    for r in schema.rules:
        if r.name not in df.columns:
            continue
        before = int(df[r.name].isna().sum())
        df[r.name] = _coerce_series(df[r.name], r.dtype)
        after = int(df[r.name].isna().sum())
        conv_errors[r.name] = max(0, after - before)
    return conv_errors

#Genera ruta para reporte JSON 
def _report_path(source: str) -> str:
    now = datetime.utcnow()
    out_dir = os.path.join(
        "data", "status", "dq", source, f"{now:%Y}", f"{now:%m}", f"{now:%d}"
    )
    os.makedirs(out_dir, exist_ok=True)
    filename = f"dq_{source}_{now:%Y%m%dT%H%M%SZ}.json"
    return os.path.join(out_dir, filename)

# ========= Motor generico ==========
def validate_df (df: pd.DataFrame, schema: DatasetSchema) -> Tuple[bool, str, dict]:
    report: Dict[str, Any] ={
        "source" :schema.source,
        "ts_utc" : datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "row_count" : int(len(df)),
        "issues": [],
        "by_column" :{}, 
    }
#numero de filas exactas o minimo establecido 
    if schema.exact_row_count is not None and len(df) != schema.exact_row_count:
        report["issues"].append(f"row_count != {schema.exact_row_count} (got {len(df)})")
    if len(df) < schema.required_rows_min:
        report["issues"].append(f"row_count < {schema.required_rows_min} (got {len(df)})")
        
# Columnas obligatorias
    required_cols = [r.name for r in schema.rules if r.required]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        report["issues"].append(f"missing_columns: {missing}")

#Convierte tipos y registra los errores de coerción (por columna).
    conv_errors = _coerce_types(df, schema)

    for r in schema.rules:
        col = {"present": r.name in df.columns}
        if not col["present"]:
            col["issues"] = ["missing"]
            report["by_column"][r.name] = col
            continue
#Cuenta los nulos y compara con allow_nulls
    s = df[r.name]
    issues: List[str] = []
    nulls = int(s.isna().sum())
    col["nulls"] = nulls

    if not r.allow_nulls and nulls > 0:
        issues.append(f"nulls_not_allowed ({nulls})")
# Registra si el tipado forzado generó nulos (valores inválidos)
    ce = conv_errors.get(r.name, 0)
    if ce > 0:
            issues.append(f"type_coercion_failed ({ce}) expected={r.dtype}")
#Unicidad: duplicated(keep=False) marca todas las filas duplicadas.
    if r.unique:
        dup = int(s.duplicated(keep=False).sum())
        if dup > 0:
            issues.append(f"unique_violations ({dup})")

#Rangos: solo compara valores no nulos.
        if r.dtype in ("int", "float"):
            if r.min_value is not None:
                bad = int((s.dropna() < r.min_value).sum())
                if bad > 0: issues.append(f"min_value_violation (<{r.min_value}) ({bad})")
            if r.max_value is not None:
                bad = int((s.dropna() > r.max_value).sum())
                if bad > 0: issues.append(f"max_value_violation (>{r.max_value}) ({bad})")
#valores fuera del conjunto permitido
        if r.allowed_values is not None:
            bad = int((~s.dropna().isin(r.allowed_values)).sum())
            if bad > 0: issues.append(f"allowed_values_violation ({bad})")
# validación de Strings: longitudes y regex
        if r.dtype == "str":
            if r.min_len is not None:
                bad = int((s.dropna().astype(str).str.len() < r.min_len).sum())
                if bad > 0: issues.append(f"min_len_violation (<{r.min_len}) ({bad})")
            if r.max_len is not None:
                bad = int((s.dropna().astype(str).str.len() > r.max_len).sum())
                if bad > 0: issues.append(f"max_len_violation (>{r.max_len}) ({bad})")
            if r.regex is not None:
                pat = re.compile(r.regex)
                bad = int((~s.dropna().astype(str).str.match(pat)).sum())
                if bad > 0: issues.append(f"regex_violation ({bad})")
# Adjunta las incidencias por columna 
        if issues:
            col["issues"] = issues
        report["by_column"][r.name] = col
# Semáforo final (ok): no debe haber issues a nivel dataset ni por columna.
    ok = len(report["issues"]) == 0 and all(
        "issues" not in report["by_column"].get(r.name, {}) for r in schema.rules
    )
#Guarda el reporte JSON
    path = _report_path(schema.source)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    logger.info(f"[dq] {schema.source} {'OK' if ok else 'FAIL'} | report={path}")
    return ok, path, report

# ======== Esquemas para CSV capa RAW ==========
EXPECTED_BOROUGHS = {"Manhattan","Brooklyn","Queens","Bronx","Staten Island"}

#web scraping
def schema_nyc_boroughs() -> DatasetSchema:
    return DatasetSchema(
        source="nyc_boroughs",
        required_rows_min=5,
        exact_row_count=5,
        rules=[
            ColumnRule("borough", "str", required=True, allow_nulls=False,
                       unique=True, allowed_values=EXPECTED_BOROUGHS),
            ColumnRule("population", "int", required=True, allow_nulls=False, min_value=1),
            ColumnRule("land_area_km2", "float", required=True, allow_nulls=False, min_value=0),
            ColumnRule("density_km2", "float", required=True, allow_nulls=False, min_value=0),
        ],
    )

#API banxico
def schema_banxico_raw() -> DatasetSchema:
    return DatasetSchema(
        source="banxico",
        required_rows_min=1,
        rules=[
            ColumnRule("fecha", "date", required=True, allow_nulls=False, unique=True),
            ColumnRule("valor", "float", required=True, allow_nulls=False, min_value=0),
        ],
    )

#Archivo plano CSV

def schema_ab_nyc() -> DatasetSchema:
    ROOM_TYPES = {"Entire home/apt", "Private room", "Shared room", "Hotel room"}

    return DatasetSchema(
        source="ab_nyc",
        required_rows_min=1,
        rules=[
            ColumnRule("id", "int", required=True, allow_nulls=False, unique=True, min_value=1),

            ColumnRule("neighbourhood_group", "str", required=True, allow_nulls=False,
                       allowed_values=EXPECTED_BOROUGHS),

            ColumnRule("neighbourhood", "str", required=True, allow_nulls=False),

            ColumnRule("name", "str", required=True, allow_nulls=False, min_len=1, max_len=120),

            ColumnRule("host_id", "int", required=True, allow_nulls=False, min_value=1),  

            ColumnRule("host_name", "str", required=True, allow_nulls=True, min_len=1, max_len=80),

            ColumnRule("room_type", "str", required=True, allow_nulls=False,
                       allowed_values=ROOM_TYPES),
            # Precio no negativo
            ColumnRule("price", "float", required=True, allow_nulls=False, min_value=0),
            # Mínimo de noches: suele ser >= 1
            ColumnRule("minimum_nights", "int", required=True, allow_nulls=False, min_value=1),
            # Reseñas acumuladas: entero >= 0
            ColumnRule("number_of_reviews", "int", required=True, allow_nulls=False, min_value=0),
            # Días disponibles en el año: 0..365
            ColumnRule("availability_365", "int", required=True, allow_nulls=False, min_value=0, max_value=365),

            ColumnRule("last_review", "date", required=False, allow_nulls=True),

            ColumnRule("reviews_per_month", "float", required=False, allow_nulls=True, min_value=0),

            ColumnRule("calculated_host_listings_count", "int", required=True, allow_nulls=False, min_value=0),
        ],
    )
def validate_nyc_boroughs(df: pd.DataFrame):
    return validate_df(df.copy(), schema_nyc_boroughs())

def validate_banxico_raw(df: pd.DataFrame):
    return validate_df(df.copy(), schema_banxico_raw())

def validate_ab_nyc(df: pd.DataFrame):
    return validate_df(df.copy(), schema_ab_nyc())
