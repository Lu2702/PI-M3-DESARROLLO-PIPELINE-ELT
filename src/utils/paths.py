import os
from datetime import datetime
from .config import RAW_DIR # Carpeta base de la capa RAW, definida en el archivo .env y leída en config.py

def raw_files_dir(source: str, dt: datetime) -> str:
    """
    Construye la ruta donde se guardarán los datos en la capa RAW,
    siguiendo una convención estándar: 
    data/raw/files/<fuente>/<YYYY>/<MM>/<DD>

    Parámetros:
    ----------
    source : str
        Nombre corto de la fuente de datos
        Esto sirve para separar datos de distintas fuentes en carpetas diferentes. 
    dt : datetime
        Fecha y hora de referencia. Generalmente usamos datetime.utcnow()
        para que cada ejecución del pipeline quede organizada por día.

    Retorna:
    --------
    str
        Ruta completa hacia la carpeta de salida en la capa RAW.
        Ejemplo: "data/raw/files/ab_nyc/2025/08/24"
    """
    return os.path.join(RAW_DIR, "files", source, dt.strftime("%Y/%m/%d"))