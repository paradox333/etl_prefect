from prefect import flow, get_run_logger
from config import settings
from database.db_state import get_pending_files, increment_retries, update_status
from prefect_flows.tasks.extract import extract_data
# Importamos las nuevas tareas separadas
from prefect_flows.tasks.transform import parse_excel_sheet, clean_dataframe, transform_ifr_excel
from prefect_flows.tasks.load import load_data_program, load_data_ifr

@flow
def etl_flow(bucket: str = settings.BUCKET_NAME):
    """Flujo ETL principal: Procesa Program e IFR desde el mismo archivo."""
    logger = get_run_logger()
    logger.info("ETL Initialization")

    files = get_pending_files()

    for file in files:
        logger.info(f"Start processing for file {file}")
        try:
            # 1. Extraer los datos desde MinIO 
            # Esto devuelve los bytes del archivo excel completo
            raw_bytes = extract_data(bucket, file)

            # --- RAMA 1: PROGRAM ---
            logger.info("--- Processing Branch: Program ---")
            # a) Parsear hoja Program
            df_program_raw = parse_excel_sheet(raw_bytes, sheet_name="Program")
            # b) Limpiar (reutilizando lógica)
            df_program_clean = clean_dataframe(df_program_raw, context_name="Program")
            # c) Cargar a tabla 'program' (o nombre derivado del archivo)
            # Asumimos que load_data maneja la creación de tabla
            load_data_program(df_program_clean, "program", file)


            # --- RAMA 2: IFR ---
            logger.info("--- Processing Branch: IFR ---")
            # a) transforma la data de la hora ifr
            df_ifr_transfrom = transform_ifr_excel(raw_bytes)
            # b) Cargar a tabla 'ifr'
            load_data_ifr(df_ifr_transfrom, file)


            # Si ambas ramas tuvieron éxito, actualizamos estado
            update_status(file, 'ready')
            logger.info(f"File {file} processed successfully (Program + IFR)")

        except Exception as e:
            # Si falla CUALQUIERA de las dos ramas, marcamos error en el archivo
            increment_retries(file)
            logger.error(f"Failed processing file {file}: {e}")

if __name__ == "__main__":
    etl_flow()