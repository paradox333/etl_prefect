from prefect import get_run_logger, task
from database.db_state import increment_retries, update_status
from prefect_flows.utils.minio_client import get_minio_client

@task
def extract_data(bucket_name: str, file_name: str) -> bytes:
    """
    Descarga un archivo desde MinIO y actualiza su estado en la base de datos.
    Si ocurre un error, incrementa el contador de reintentos.
    """
    logger = get_run_logger()
    logger.info(f"Extracting data from {file_name!r}")

    try:
        # Conexión al cliente MinIO y lectura del archivo remoto
        client = get_minio_client()
        response = client.get_object(bucket_name, file_name)
        data = response.read()

        # Liberar los recursos del stream de respuesta
        response.close()
        response.release_conn()

        # Actualizar estado del archivo en la base de datos
        update_status(file_name, "extracting")
        logger.info("Data extracted successfully")

    except Exception as e:
        # Si ocurre un error, aumentar los reintentos
        increment_retries(file_name)
        logger.error(f"Error extracting {file_name!r}: {e}")

    return data

@task
def extract_data_ifr(bucket_name: str, file_name: str) -> bytes:
    """
    Descarga un archivo desde MinIO y actualiza su estado en la base de datos.
    Si ocurre un error, incrementa el contador de reintentos.
    """
    logger = get_run_logger()
    logger.info(f"Extracting data from {file_name!r}")

    try:
        # Conexión al cliente MinIO y lectura del archivo remoto
        client = get_minio_client()
        response = client.get_object(bucket_name, file_name)
        data = response.read()

        # Liberar los recursos del stream de respuesta
        response.close()
        response.release_conn()

        # Actualizar estado del archivo en la base de datos
        update_status(file_name, "extracting")
        logger.info("Data extracted successfully")

    except Exception as e:
        # Si ocurre un error, aumentar los reintentos
        increment_retries(file_name)
        logger.error(f"Error extracting {file_name!r}: {e}")

    return data