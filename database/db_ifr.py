import io
import csv
import psycopg
from psycopg.rows import dict_row
from config import settings
from psycopg import sql
import pandas as pd

TABLE_NAME = "ifr"
SCHEMA_NAME = settings.DATABASE_SCHEMA

def copy_dataframe_to_table_ifr(df: pd.DataFrame):
    """
    Inserta los datos de un DataFrame en una tabla PostgreSQL
    usando la instrucción COPY (método eficiente para cargas masivas).
    """
    df = df.copy()
    df = df.where(pd.notnull(df), None)  # reemplaza NaN por NULL

    # Convierte el DataFrame a un CSV temporal en memoria
    buffer = io.StringIO()
    df.to_csv(
        buffer,
        index=False,
        header=False,
        lineterminator="\n",
        quoting=csv.QUOTE_MINIMAL,
        escapechar="\\",
    )
    buffer.seek(0)

    with psycopg.connect(settings.DATABASE_CONN_STR) as conn:
        with conn.cursor() as cur:
            copy_sql = sql.SQL("""
                COPY {} ({})
                FROM STDIN
                WITH (FORMAT CSV)
            """).format(
                sql.Identifier(TABLE_NAME),
                sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
            )

            # Escribe los datos por chunks para optimizar memoria
            with cur.copy(copy_sql) as copy:
                while True:
                    chunk = buffer.read(1024 * 1024)
                    if not chunk:
                        break
                    copy.write(chunk)
        conn.commit()
