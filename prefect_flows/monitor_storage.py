from prefect import flow, task
from datetime import datetime, timezone
from prefect.cache_policies import NO_CACHE
from database.db_state import (
    init_state_table,
    create_state_record,
    get_state_record,
    update_state_record
)
from prefect_flows.utils.minio_client import MinioStorageObserver

# Inicializa la tabla de estado si aún no existe
@task
def initialize_state_table():
    init_state_table()

# Detecta archivos nuevos o modificados en MinIO comparando el etag con el registro en base de datos
@task(cache_policy=NO_CACHE)
def detect_changes(observer: MinioStorageObserver):
    changed_files = []
    for file in observer.list_files():
        metadata = observer.get_file_metadata(file)
        state = get_state_record(file)
        # Si no existe registro o el etag ha cambiado, se marca como modificado
        if not state or state["etag"] != metadata["etag"]:
            changed_files.append({
                "file_path": file,
                "etag": metadata["etag"],
                "last_modified": metadata["last_modified"],
                "status": "pending",
                "retries": 0,
                "last_checked": datetime.now(timezone.utc)
            })
    return changed_files

# Crea o actualiza los registros en la tabla de estado según los cambios detectados
@task
def update_state(changed_files: list[dict]):
    for record in changed_files:
        if get_state_record(record["file_path"]):
            update_state_record(record)
        else:
            create_state_record(record)

# Flujo principal: observa el almacenamiento, detecta cambios y actualiza el estado de los archivos
@flow
def monitor_storage():
    initialize_state_table()
    observer = MinioStorageObserver()
    changes = detect_changes(observer)
    update_state(changes)

# Permite ejecutar el flujo directamente desde la línea de comandos
if __name__ == "__main__":
    monitor_storage()
