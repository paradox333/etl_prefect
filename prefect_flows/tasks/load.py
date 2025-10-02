from prefect import task, get_run_logger
import psycopg
from psycopg import sql
import io
import pandas as pd
import csv

from config import settings

def map_dtype_to_postgres(dtype) -> str:
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

@task
def load_data(df: pd.DataFrame, table_name: str):
    """
    Crea la tabla si no existe y carga el DataFrame via COPY FROM STDIN (CSV).
    Requiere psycopg v3.x.
    """
    logger = get_run_logger()
    logger.info(f"Insertando {len(df)} filas en la tabla {table_name!r}")

    if df.empty:
        logger.warning("El DataFrame está vacío. No se hará COPY.")
        return

    # Normaliza NaN a vacío para CSV (evita 'nan' literales)
    df = df.copy()
    df = df.where(pd.notnull(df), None)

    try:
        with psycopg.connect(settings.DATABASE_CONN_STR) as conn:
            with conn.cursor() as cur:
                # 1) Crear tabla si no existe (usando Identifier para evitar problemas de quoting)
                column_defs = []
                for col, dtype in zip(df.columns, df.dtypes):
                    pg_type = map_dtype_to_postgres(dtype)
                    column_defs.append(
                        sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(pg_type))
                    )

                create_table_sql = sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {} (
                        {}
                    );
                """).format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(column_defs)
                )

                cur.execute(create_table_sql)
                logger.info("Tabla verificada/creada.")

                # 2) Preparar CSV en memoria (texto)
                buffer = io.StringIO()
                # Ajustes de CSV robustos ante comas, nuevas líneas y comillas en datos
                df.to_csv(
                    buffer,
                    index=False,
                    header=False,
                    lineterminator="\n",
                    quoting=csv.QUOTE_MINIMAL,
                    escapechar="\\",
                )
                buffer.seek(0)

                # 3) COPY FROM STDIN (con quoting correcto de tabla y columnas)
                copy_sql = sql.SQL("""
                    COPY {} ({})
                    FROM STDIN
                    WITH (FORMAT CSV)
                """).format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
                )

                # Opción A: usar el context manager y copy.write(...)
                with cur.copy(copy_sql) as copy:
                    # Para datasets grandes, mejor escribir en chunks:
                    while True:
                        chunk = buffer.read(1024 * 1024)  # 1 MB
                        if not chunk:
                            break
                        copy.write(chunk)

                # Alternativa B (equivalente y más corta):
                # cur.copy(copy_sql, source=buffer)

            conn.commit()
            logger.info("COPY ejecutado y commit realizado.")

            # Validación opcional: contar filas
            with conn.cursor() as cur:
                count_sql = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name))
                cur.execute(count_sql)
                total = cur.fetchone()[0]
                logger.info(f"Total de filas en {table_name!r} tras la carga: {total}")

    except Exception as e:
        # Prefect registrará el stack trace
        logger.error(f"Error cargando datos en {table_name!r}: {e}")
        raise