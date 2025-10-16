import csv
import io
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from config import settings
import pandas as pd
from database.db_product import get_latest_version_info

TABLE_NAME = "product_summaries"
SCHEMA_NAME = settings.DATABASE_SCHEMA

def init_summary_table():
    """
    Crea el esquema y la tabla 'product_summaries' si no existen.
    Almacena sumatorias por campo, versión, producto y período.
    """
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        id_version BIGINT NOT NULL,
        field TEXT NOT NULL,
        total DOUBLE PRECISION,
        product TEXT,
        load_timestamp TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with psycopg.connect(settings.DATABASE_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(SCHEMA_NAME))
            )
            cur.execute(ddl)
        conn.commit()



def insert_summaries_bulk(df: pd.DataFrame, columnas: list[str], table_name: str):
    """
    Calcula sumatorias por producto y campo, y las inserta en bloque usando COPY.
    Recupera id_version desde la tabla de productos.
    """
    try:
        print("INGRESA")
        # Validación
        if 'product' not in df.columns:
            raise ValueError("No products in DataFrame.")

        # Calcular sumatorias
        df_summaries = calculate_sum_for_product(df, columnas)
        print('SUMATORIA')
        # Obtener la última versión insertada
        version_info = get_latest_version_info(table_name)
        print("VERSION")
        print(version_info)
        if not version_info:
            raise ValueError("Version not found")
        id_version = version_info["id_version"]
        load_timestamp = version_info["load_timestamp"]

        # Agregar columnas contextuales
        df_summaries = df_summaries.copy()
        df_summaries["id_version"] = id_version
        df_summaries["load_timestamp"] = load_timestamp
        print("COLUMNAS AGREGADAS")
        # Reordenar columnas
        df_summaries = df_summaries[["id_version", "field", "total", "product", "load_timestamp"]]

        # Convertir a CSV en memoria
        buffer = io.StringIO()
        df_summaries.to_csv(
            buffer,
            index=False,
            header=False,
            lineterminator="\n",
            quoting=csv.QUOTE_MINIMAL,
            escapechar="\\",
        )
        buffer.seek(0)
        print("BUFFER:", buffer)
        # Ejecutar COPY
        with psycopg.connect(settings.DATABASE_CONN_STR) as conn:
            with conn.cursor() as cur:
                copy_sql = sql.SQL("""
                    COPY {}.{} (id_version, field, total, product, load_timestamp)
                    FROM STDIN WITH (FORMAT CSV)
                """).format(sql.Identifier(SCHEMA_NAME), sql.Identifier(TABLE_NAME))

                with cur.copy(copy_sql) as copy:
                    while True:
                        chunk = buffer.read(1024 * 1024)
                        if not chunk:
                            break
                        copy.write(chunk)
            conn.commit()
    except Exception as e:
        print(e)

def calculate_sum_for_product(df: pd.DataFrame, columnas: list[str]) -> pd.DataFrame:
    try:
        if 'product' not in df.columns:
            raise ValueError("No product column in DataFrame.")

        # Agrupar por producto y sumar las columnas especificadas
        resumen = df.groupby('product')[columnas].sum().reset_index()

        # Reestructurar a formato largo para compatibilidad con inserciones
        resumen_largo = resumen.melt(id_vars='product', var_name='field', value_name='total')
        return resumen_largo
    except Exception as e:
        print(e)