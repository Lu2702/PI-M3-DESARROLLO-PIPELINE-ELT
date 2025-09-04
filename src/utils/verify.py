# src/utils/verify.py
from __future__ import annotations
import hashlib, json, os
from datetime import datetime
from typing import Iterable

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Carpeta raíz donde se guardarán los manifests por source:
#   data/status/verify/<source>/manifest_raw.jsonl
VERIFY_ROOT = os.path.join("data", "status", "verify")



# Helpers de ruta

def _manifest_path_for(source: str) -> str:
    """
    Devuelve la ruta del manifest para un source:
      data/status/verify/<source>/manifest_raw.jsonl
    Crea la carpeta si no existe.
    """
    out_dir = os.path.join(VERIFY_ROOT, source)
    os.makedirs(out_dir, exist_ok=True)
    return os.path.join(out_dir, "manifest_raw.jsonl")


def _iter_all_manifests() -> Iterable[str]:
    """
    Itera sobre todos los manifests bajo data/status/verify/** (uno por source).
    """
    if os.path.exists(VERIFY_ROOT):
        for root, _, files in os.walk(VERIFY_ROOT):
            for f in files:
                if f == "manifest_raw.jsonl":
                    yield os.path.join(root, f)



# Verificaciones básicas

def file_exists_and_size(path: str, min_bytes: int = 1) -> bool:
    if not os.path.exists(path):
        logger.error(f"[verify] no existe: {path}")
        return False
    size = os.path.getsize(path)
    if size < min_bytes:
        logger.error(f"[verify] tamaño insuficiente: {path} ({size} bytes)")
        return False
    logger.info(f"[verify] ok: {path} ({size} bytes)")
    return True


def md5sum(path: str, chunk: int = 1024 * 1024) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()



# Dedupe + registro

def is_duplicate(path: str, md5: str, registry_path: str | None = None) -> bool:
    """
    Devuelve True si ya existe un registro con el mismo MD5.
    - Si registry_path se pasa y existe, solo busca allí.
    - Si no, busca en TODOS los manifests bajo data/status/verify/**.
    """
    if registry_path and os.path.exists(registry_path):
        manifests = [registry_path]
    else:
        manifests = list(_iter_all_manifests())

    for mf in manifests:
        try:
            with open(mf, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if rec.get("md5") == md5:
                        logger.warning(
                            f"[verify] duplicate_detected source={rec.get('source')} "
                            f"path={path} == {rec.get('path')} md5={md5}"
                        )
                        return True
        except FileNotFoundError:
            continue
    return False


def register_file(path: str, source: str, md5: str, registry_path: str | None = None) -> None:
    """
    Registra el archivo en el manifest por source:
      data/status/verify/<source>/manifest_raw.jsonl
    - Si registry_path se pasa, escribe allí (modo compatibilidad).
    """
    manifest_path = registry_path or _manifest_path_for(source)
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)

    rec = {
        "ts_utc": f"{datetime.utcnow():%Y-%m-%dT%H:%M:%SZ}",
        "source": source,
        "path": path,
        "md5": md5,
    }

    with open(manifest_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    logger.info(
        f"[verify] registered source={source} path={path} md5={md5} manifest={manifest_path}"
    )
# === Obtener el último registro por MD5 (para reutilizar path) ===
def find_last_record_by_md5(md5: str) -> dict | None:
    """
    Devuelve el último registro (por ts_utc) en todos los manifests que tenga ese md5.
    """
    last = None
    last_ts = ""
    for mf in _iter_all_manifests():
        try:
            with open(mf, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if rec.get("md5") == md5:
                        ts = rec.get("ts_utc", "")
                        if ts >= last_ts:
                            last = rec
                            last_ts = ts
        except FileNotFoundError:
            continue
    return last

# === Registrar una “referencia diaria” (sin copiar) ===
def register_reference(source: str, path: str, md5: str) -> None:
    """
    Escribe una línea en el manifest marcando que en esta corrida se
    usó la misma versión (sin nueva copia).
    """
    manifest_path = _manifest_path_for(source)
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    rec = {
        "ts_utc": f"{datetime.utcnow():%Y-%m-%dT%H:%M:%SZ}",
        "source": source,
        "path": path,
        "md5": md5,
        "reference": True
    }
    with open(manifest_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    logger.info(f"[verify] reference_registered source={source} path={path} md5={md5} manifest={manifest_path}")