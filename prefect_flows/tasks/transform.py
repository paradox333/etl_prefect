import pandas as pd
from prefect import get_run_logger, task
from io import BytesIO
import unicodedata
import re
from database.db_state import increment_retries, update_status

@task
def transform_data(data: bytes, file_name: str) -> pd.DataFrame:
    """
    Transforma los datos crudos extraídos de un archivo Excel.
    Limpia columnas, normaliza nombres y elimina filas incompletas.
    """
    logger = get_run_logger()
    logger.info(f"Transforming data from {file_name!r}")
    
    try:
        # Leer el contenido binario como archivo Excel
        df = pd.read_excel(BytesIO(data), sheet_name="Program", engine="openpyxl")

        # Eliminar filas con más de 'max_nans' valores faltantes
        max_nans = 10
        df = df.dropna(thresh=max_nans)
        
        # --- Normalización de nombres de columnas ---
        cols = df.columns.to_list()

        # Convertir a minúsculas y reemplazar espacios por guiones bajos
        cols = [col.lower().replace(' ', '_') for col in cols]
        
        # Eliminar acentos y tildes
        cols = [unicodedata.normalize('NFD', col).encode('ascii', 'ignore').decode("utf-8") for col in cols]
        
        # Reemplazar caracteres no alfanuméricos por guiones bajos
        cols = [re.sub(r'[^a-z0-9_]', '_', col) for col in cols]

        # Limpiar guiones bajos duplicados o en extremos
        cols = [re.sub(r'__+', '_', col).strip('_') for col in cols]
        
        # Asignar los nuevos nombres normalizados
        df.columns = cols
        
        # Actualizar estado del archivo tras la transformación exitosa
        update_status(file_name, 'transforming')
        logger.info("Data transformed successfully")
    
    except Exception as e:
        # Registrar error e incrementar número de reintentos
        increment_retries(file_name)
        logger.error(f"Error transforming data from {file_name!r}: {e}")

    return df
