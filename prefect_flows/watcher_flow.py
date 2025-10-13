from prefect import flow, get_run_logger
from prefect.deployments import run_deployment
from database.db_state import has_pending_state

# Flujo que revisa periódicamente si hay archivos pendientes y dispara el ETL
@flow
def watcher_flow():
    logger = get_run_logger()

    # Verifica si existen archivos con estado 'pending' en la base de datos
    if has_pending_state():
        logger.info("Condition found. Starting ETL")
        # Lanza el despliegue del flujo ETL asociado
        run_deployment(name="etl-flow/etl_api_trigger")
    else:
        logger.info("No file to process")

# Permite ejecutar el flujo directamente desde la línea de comandos
if __name__ == "__main__":
    watcher_flow()
