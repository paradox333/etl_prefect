# watcher_flow.py
from prefect import flow, get_run_logger
from prefect.deployments import run_deployment
from config import settings
import psycopg
from psycopg import sql

def asegurar_schema_y_tabla():
    """
    Crea schema y tabla si no existen. Inserta un registro inicial con status='READY'.
    """
    logger = get_run_logger()

    try:
        with psycopg.connect(settings.DATABASE_CONN_STR) as conn:
            with conn.cursor() as cur:
                # 1) Crear schema si no existe
                cur.execute(
                    sql.SQL("""
                        CREATE SCHEMA IF NOT EXISTS {};
                    """).format(sql.Identifier(settings.DATABASE_SCHEMA))
                )
                logger.info(f"Schema {settings.DATABASE_SCHEMA!r} verificado/creado.")

                # 2) Crear tabla si no existe
                cur.execute(
                    sql.SQL("""
                        CREATE TABLE IF NOT EXISTS {}.{} (
                            id SERIAL PRIMARY KEY,
                            status TEXT NOT NULL,
                            created_at TIMESTAMP DEFAULT NOW()
                        );
                    """).format(
                        sql.Identifier(settings.DATABASE_SCHEMA),
                        sql.Identifier("state")
                    )
                )
                logger.info(f"Tabla {settings.DATABASE_SCHEMA}.{"state"!r} verificada/creada.")

                # 3) Insertar registro inicial si no existe
                cur.execute(
                    sql.SQL("""
                        INSERT INTO {}.{} (status)
                        SELECT 'READY'
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {}.{} WHERE status = 'READY'
                        );
                    """).format(
                        sql.Identifier(settings.DATABASE_SCHEMA),
                        sql.Identifier("state"),
                        sql.Identifier(settings.DATABASE_SCHEMA),
                        sql.Identifier("state")
                    )
                )
                logger.info("Registro inicial 'READY' asegurado.")

            conn.commit()

    except Exception as e:
        logger.error(f"[watcher] Error asegurando schema y tabla: {e}")


def check_estado() -> bool:
    """
    Devuelve True si hay al menos un registro en estado con status='READY'.
    """
    try:
        with psycopg.connect(settings.DATABASE_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("""
                        SELECT EXISTS (
                            SELECT 1 FROM {}.{}
                            WHERE status = 'READY' LIMIT 1
                        );
                    """).format(
                        sql.Identifier(settings.DATABASE_SCHEMA),
                        sql.Identifier("state")
                    )
                )
                exists, = cur.fetchone()
                return bool(exists)
    except psycopg.OperationalError as e:
        print(f"[watcher] No se pudo conectar a la DB: {e}")
        return False


@flow
def watcher_flow():
    logger = get_run_logger()

    asegurar_schema_y_tabla()

    if check_estado():
        logger.info("✅ Condición encontrada. Disparando ETL…")
        run_deployment(name="etl-flow/etl_api_trigger")
    else:
        logger.info("⏳ Nada que hacer. Estado no READY.")