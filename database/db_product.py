import io
import csv
import psycopg
from psycopg.rows import dict_row
from config import settings
from psycopg import sql
import pandas as pd

TABLE_NAME = "sqm.products"


def map_dtype_to_postgres(dtype) -> str:
    """
    Mapea un tipo de dato de pandas a su tipo equivalente en PostgreSQL.
    """
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "TEXT"


def init_products_table(df: pd.DataFrame, table_name: str):
    """
    Crea una tabla en PostgreSQL basada en la estructura del DataFrame.
    Si la tabla ya existe, no la recrea.
    """
    df = rename_duplicate_columns(df)
    column_defs = []

    for col, dtype in zip(df.columns, df.dtypes):
        pg_type = map_dtype_to_postgres(dtype)
        column_defs.append(
            sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(pg_type))
        )

    ddl = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            {}
        );
    """).format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(column_defs)
    )

    with psycopg.connect(settings.DATABASE_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


def copy_dataframe_to_table(df: pd.DataFrame, table_name: str):
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
                sql.Identifier(table_name),
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


def count_rows(table_name: str) -> int:
    """
    Retorna el número total de filas existentes en una tabla PostgreSQL.
    """
    with psycopg.connect(settings.DATABASE_CONN_STR) as conn:
        with conn.cursor() as cur:
            count_sql = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name))
            cur.execute(count_sql)
            total = cur.fetchone()[0]
            return total


def rename_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra columnas duplicadas en un DataFrame agregando sufijos numéricos (_1, _2, ...).
    """
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        duplicates = cols[cols == dup].index.tolist()
        for i, idx in enumerate(duplicates):
            if i == 0:
                continue  # el primero se mantiene igual
            cols[idx] = f"{dup}_{i}"
    df.columns = cols
    return df
