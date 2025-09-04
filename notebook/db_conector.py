import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

# 1) Cargar variables desde .env
load_dotenv(override=True) 

DB_HOST = os.getenv("PGHOST")
DB_PORT = os.getenv("PGPORT")
DB_NAME = os.getenv("PGDATABASE")
DB_USER = os.getenv("PGUSER")
DB_PASSWORD = os.getenv("PGPASSWORD")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# 2) Crear motor y sesión

engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True, client_encoding="utf8")

Base = declarative_base()
DBSession = sessionmaker(bind=engine, future=True)


# 3) Helpers

def get_db_engine():
    return engine

def get_db_session():
    return DBSession()

def get_db_connection():
    return engine.connect()

def query_to_df(sql: str, params: dict = None) -> pd.DataFrame:
    """Ejecuta una query SQL y la devuelve como DataFrame de pandas"""
    raw = engine.raw_connection()
    try:
        df = pd.read_sql_query(sql, raw, params=params)
    finally:
        raw.close()
    return df

