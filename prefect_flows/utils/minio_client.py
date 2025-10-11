from minio import Minio
from config import settings
from prefect_flows.utils.sotrage_observer import StorageObserver

# Crea y retorna una instancia del cliente MinIO configurado según las credenciales del sistema
def get_minio_client():
    return Minio(
        settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=False
    )
    

from minio import Minio
from minio.error import S3Error
from config import settings
from datetime import datetime

# Implementación de un observador de almacenamiento para MinIO
class MinioStorageObserver(StorageObserver):
    def __init__(self):
        # Inicializa el cliente y define el bucket por defecto
        self.client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=False
        )
        self.bucket = settings.BUCKET_NAME

    def list_files(self, prefix: str = "") -> list[str]:
        """Retorna una lista con los nombres de archivos en el bucket, opcionalmente filtrados por prefijo."""
        return [obj.object_name for obj in self.client.list_objects(self.bucket, prefix=prefix, recursive=True)]

    def get_file_metadata(self, file_path: str) -> dict:
        """Obtiene metadatos básicos (fecha, tamaño, etag) de un archivo almacenado en MinIO."""
        stat = self.client.stat_object(self.bucket, file_path)
        return {
            "last_modified": stat.last_modified,
            "size": stat.size,
            "etag": stat.etag
        }