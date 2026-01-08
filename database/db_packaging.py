
import psycopg
from psycopg.rows import dict_row
from config import settings
from psycopg import sql
import pandas as pd

TABLE_NAME = "packaging"
SCHEMA_NAME = settings.DATABASE_SCHEMA

def get_all_packaging():
    """
    Retorna el último id_version y load_timestamp desde la tabla de productos.
    """
    with psycopg.connect(settings.DATABASE_CONN_STR, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    SELECT id_packaging, packaging_code
                    FROM {}.{}
                """).format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(TABLE_NAME)
                )
            )
            return cur.fetchall()

