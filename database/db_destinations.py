import psycopg
from psycopg.rows import dict_row
from config import settings
from psycopg import sql
import pandas as pd

TABLE_NAME = "destinations"
SCHEMA_NAME = settings.DATABASE_SCHEMA

def get_all_destinations_and_country():
    """
    Retorna una lista de diccionarios con todos los destinations y countries.
    """
    with psycopg.connect(settings.DATABASE_CONN_STR, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    SELECT id_destination, destination_name, country
                    FROM {}.{}
                """).format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(TABLE_NAME)
                )
            )
            return cur.fetchall()

