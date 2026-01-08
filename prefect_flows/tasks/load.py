from config.settings import COLUMNS_SUMMARIE
from database.db_ifr import copy_dataframe_to_table_ifr
from database.db_product_summarie import init_summary_table, insert_summaries_bulk
from prefect import task, get_run_logger
import pandas as pd

from database.db_program import copy_dataframe_to_table, count_rows, init_products_table
from database.db_state import increment_retries, update_status

@task
def load_data_program(df: pd.DataFrame, table_name: str, file_name: str):
    """
    Carga los datos de un DataFrame en una tabla de la base de datos.
    Si el proceso falla o el DataFrame está vacío, incrementa los reintentos
    asociados al archivo.
    """
    logger = get_run_logger()
    logger.info(f"Insert {len(df)} rows into table {table_name!r}")

    # Validar si el DataFrame está vacío antes de intentar cargar
    if df.empty:
        logger.warning("Empty DataFrame")
        increment_retries(file_name)
        return

    try:
        # Asegurar que la tabla de destino exista con la estructura adecuada
        init_products_table(df, table_name)

        # Insertar los datos en la tabla program
        copy_dataframe_to_table(df, table_name)

        # Actualizar el estado del archivo a "loading" en la base de datos
        update_status(file_name, 'loading')


        
        logger.info(f"Loading program Success")

    except Exception as e:
        # En caso de error, registrar y marcar el intento fallido
        increment_retries(file_name)
        logger.error(f"Error loading data in {table_name!r}: {e}")
        raise


@task
def load_data_ifr(df: pd.DataFrame, file_name: str):
    """
    Carga los datos de un DataFrame en una tabla de la base de datos.
    Si el proceso falla o el DataFrame está vacío, incrementa los reintentos
    asociados al archivo.
    """
    logger = get_run_logger()
    logger.info(f"Insert {len(df)} rows")

    # Validar si el DataFrame está vacío antes de intentar cargar
    if df.empty:
        logger.warning("Empty DataFrame")
        increment_retries(file_name)
        return

    try:

        #Inserta los datos en la tabla ifr
        copy_dataframe_to_table_ifr(df)
   
        # Actualizar el estado del archivo a "loading" en la base de datos
        update_status(file_name, 'loading')

        logger.info(f"Loading IFR Success")

    except Exception as e:
        # En caso de error, registrar y marcar el intento fallido
        increment_retries(file_name)
        logger.error(f"Error loading data IFR")
        raise
