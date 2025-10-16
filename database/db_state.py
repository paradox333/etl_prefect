import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from datetime import datetime
from config import settings

TABLE_NAME = "state"
SCHEMA_NAME = settings.DATABASE_SCHEMA  # Esquema definido en la configuración


def init_state_table():
    """
    Crea el esquema y la tabla 'state' si no existen.
    La tabla almacena el estado de cada archivo procesado (estatus, intentos, fechas, etc.).
    """
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
        file_path TEXT PRIMARY KEY,
        etag TEXT NOT NULL,
        last_modified TIMESTAMP NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        retries INTEGER NOT NULL DEFAULT 0,
        last_checked TIMESTAMP NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with psycopg.connect(settings.DATABASE_CONN_STR) as conn:
        with conn.cursor() as cur:
            # Crea el esquema si no existe
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(SCHEMA_NAME))
            )
            # Crea la tabla
            cur.execute(ddl)
        conn.commit()


def get_state_record(file_path: str) -> dict | None:
    """
    Obtiene un registro de estado por su file_path.
    Retorna un diccionario o None si no existe.
    """
    with psycopg.connect(settings.DATABASE_CONN_STR, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT * FROM {}.{} WHERE file_path = %s").format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(TABLE_NAME)
                ),
                (file_path,)
            )
            return cur.fetchone()


def create_state_record(record: dict):
    """
    Inserta un nuevo registro en la tabla 'state' con la información del archivo.
    """
    with psycopg.connect(settings.DATABASE_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    INSERT INTO {}.{} (file_path, etag, last_modified, status, retries, last_checked)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """).format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(TABLE_NAME)
                ),
                (
                    record["file_path"],
                    record["etag"],
                    record["last_modified"],
                    record["status"],
                    record["retries"],
                    record["last_checked"]
                )
            )
        conn.commit()


def update_state_record(record: dict):
    """
    Actualiza los campos principales de un registro existente según su file_path.
    """
    with psycopg.connect(settings.DATABASE_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    UPDATE {}.{}
                    SET etag = %s,
                        last_modified = %s,
                        status = %s,
                        retries = %s,
                        last_checked = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE file_path = %s
                """).format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(TABLE_NAME)
                ),
                (
                    record["etag"],
                    record["last_modified"],
                    record["status"],
                    record["retries"],
                    record["last_checked"],
                    record["file_path"]
                )
            )
        conn.commit()


def has_pending_state() -> bool:
    """
    Retorna True si existe al menos un registro pendiente (status='pending' y retries<3).
    """
    with psycopg.connect(settings.DATABASE_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    SELECT EXISTS (
                        SELECT 1 FROM {}.{} WHERE status != 'ready' AND retries < 3
                    );
                """).format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(TABLE_NAME)
                )
            )
            exists, = cur.fetchone()
            return bool(exists)


def get_pending_files() -> list[str]:
    """
    Retorna una lista con los file_path de registros pendientes (status != 'pending' y retries < 3).
    """
    with psycopg.connect(settings.DATABASE_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    SELECT file_path
                    FROM {}.{}
                    WHERE status != 'ready' AND retries < 3;
                """).format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(TABLE_NAME)
                )
            )
            return [row[0] for row in cur.fetchall()]


def update_status(file_path: str, new_status: str) -> None:
    """
    Actualiza el estado (status) de un registro específico por su file_path.
    """
    with psycopg.connect(settings.DATABASE_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    UPDATE {}.{}
                    SET status = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE file_path = %s;
                """).format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(TABLE_NAME)
                ),
                (new_status, file_path)
            )
            conn.commit()


def increment_retries(file_path: str) -> None:
    """
    Incrementa el número de reintentos (retries) para un archivo determinado.
    """
    with psycopg.connect(settings.DATABASE_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    UPDATE {}.{}
                    SET retries = retries + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE file_path = %s;
                """).format(
                    sql.Identifier(SCHEMA_NAME),
                    sql.Identifier(TABLE_NAME)
                ),
                (file_path,)
            )
            conn.commit()
