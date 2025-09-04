# src/utils/logger.py
# -----------------------------------------------------------
# Este módulo crea y configura un "logger" reutilizable:
# - Imprime mensajes a CONSOLA y también a un ARCHIVO rotado.
# - El nivel de detalle se controla con la variable de entorno LOG_LEVEL.
# - Es seguro ante valores inválidos y evita duplicar handlers.
# -----------------------------------------------------------

import logging # Módulo estándar de logging en Python.
import os  # Para leer variables de entorno y manejar rutas.
from logging.handlers import RotatingFileHandler # Handler que rota el archivo de logs automáticamente.

# Diccionario que traduce el texto del .env (DEBUG/INFO/...) al valor numérico interno de logging.
# También aceptamos "WARN" como sinónimo de "WARNING".
LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO" : logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

def resolve_level(value:str) -> int:
    if not value:
        return logging.INFO
    v = value.strip().upper() # Normalizamos espacios y a MAYÚSCULAS.
    # Si viene como número en texto (p. ej., "10"), intentamos convertirlo.
    if v.isdigit():
        num = int(v)
        # Solo permitimos los niveles estándar. Si no coincide, usa INFO.
        return num if num in (10, 20, 30, 40, 50) else logging.INFO 
    # Si viene como texto (DEBUG/INFO/WARNING/ERROR/CRITICAL/WARN).
    return LEVELS.get(v, logging.INFO)

def get_logger (name : str) -> logging.Logger:
    """Crea y devuelve un logger configurado:
    - Nivel de log controlado por LOG_LEVEL (o INFO por defecto)
    - Handler de consola (StreamHandler)
    - Handler de archivo con rotación (RotatingFileHandler) en ./logs/extractor.log.
    - Evita duplicar Handlers si ya se configuro antes 
    """
    #Resolvemos el nivel a partir de la variable de entorno LOG_LEVEL (si no existe, INFO)

    level = resolve_level(os.getenv("LOG_LEVEL", "INFO"))

    # Obtenemos (o creamos) el logger con este nombre
    # Recomendación pasar __name__ desde el módulo que lo usa para diferenciar orígenes.

    logger = logging.getLogger(name)
    

    # Si el logger ya tiene handlers configurados (porque ya se llamó antes), lo devolvemos tal cual.
    # Esto evita que se agreguen múltiples handlers y se dupliquen los mensajes.

    if logger.handlers:
        return logger
    
    logger.setLevel(level) 
    
    # Definimos el formato de salida: timestamp | nivel | nombre_del_logger | mensaje
    form = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

     # Handler de CONSOLA
    ch = logging.StreamHandler()  # Imprime en la consola.
    ch.setLevel(level)            # Mismo umbral de nivel que el logger.
    ch.setFormatter(form)          # Aplicamos el formato definido.
    logger.addHandler(ch)         # Conectamos el handler al logger.   

    # Handler de ARCHIVO con rotación automática
    # Guardamos los logs en ./logs/extractor.log
    log_path = os.path.join(os.getcwd(), "logs", "extractor.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)  # Crea la carpeta ./logs si no existe.

    # RotatingFileHandler:
    # - maxBytes: tamaño máximo del archivo (5 MB).
    # - backupCount: cuántos archivos de respaldo mantener (5).
    # - encoding='utf-8': para soportar acentos/símbolos correctamente

    fh = RotatingFileHandler(
        log_path,
        maxBytes=5_000_000,   # 5 MB
        backupCount=5,        # extractor.log.1, extractor.log.2, ...
        encoding="utf-8"
    )
    fh.setLevel(level)        # Mismo umbral de nivel que el logger.
    fh.setFormatter(form)      # Mismo formato que consola.
    logger.addHandler(fh)     # Conectamos el handler al logger.

    # Evita que los mensajes suban al "root logger" y se impriman dos veces
    # si otro paquete configuró el root. Mantiene los logs limpios.
    logger.propagate = False

    # (Opcional) Comprobación: si LOG_LEVEL estaba mal escrito, ya hicimos fallback a INFO.
    # Dejamos un aviso en el log para que sepas que el valor no era válido.
    raw = os.getenv("LOG_LEVEL")
    if raw and resolve_level(raw) == logging.INFO and raw.strip().upper() not in LEVELS and not raw.strip().isdigit():
        logger.warning(f"LOG_LEVEL inválido: {raw!r}. Usando INFO por defecto.")

    # Devolvemos el logger listo para usar en cualquier módulo.
    return logger