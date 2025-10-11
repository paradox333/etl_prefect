from prefect import flow, get_run_logger
from config import settings
from database.db_state import get_pending_files
from prefect_flows.tasks.extract import extract_data
from prefect_flows.tasks.transform import transform_data
from prefect_flows.tasks.load import load_data
from database.db_state import increment_retries, update_status

@flow
def etl_flow(
    bucket: str = settings.BUCKET_NAME,
    ):
    """Flujo ETL principal: extrae, transforma y carga los archivos pendientes desde MinIO."""
    logger = get_run_logger()
    logger.info("ETL Initialization")

    # Obtiene la lista de archivos con estado 'pending' desde la base de datos
    files = get_pending_files()

    # Procesa cada archivo de forma secuencial
    for file in files:
        logger.info(f"Start processing for file {file}")
        try:
            # Usa el nombre del archivo como nombre de tabla destino
            table_name = file.split()[0].strip()
            logger.info(f"Table: {table_name}")

            # 1. Extrae los datos desde MinIO
            raw_data = extract_data(bucket, file)

            # 2. Transforma los datos en un DataFrame limpio
            df = transform_data(raw_data, file)

            # 3. Carga el DataFrame a la base de datos
            load_data(df, table_name, file)

            # Actualiza el estado del archivo como procesado
            update_status(file, 'ready')
            logger.info("Process ended successfully")

        except Exception as e:
            # Si ocurre un error, incrementa el contador de reintentos y registra el fallo
            increment_retries(file)
            logger.error(e)


if __name__ == "__main__":
    # Permite ejecutar el flujo manualmente desde línea de comandos
    etl_flow()
