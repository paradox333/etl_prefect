from abc import ABC, abstractmethod

# Clase base abstracta para definir una interfaz común de observadores de almacenamiento.
class StorageObserver(ABC):

    @abstractmethod
    def list_files(self, prefix: str = "") -> list[str]:
        """Debe retornar una lista de archivos almacenados, opcionalmente filtrados por un prefijo."""
        pass

    @abstractmethod
    def get_file_metadata(self, file_path: str) -> dict:
        """Debe retornar los metadatos de un archivo específico (como tamaño, fecha, hash, etc.)."""
        pass
