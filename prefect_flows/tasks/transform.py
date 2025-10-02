import pandas as pd
from prefect import task
from io import BytesIO
import unicodedata
import re # Necesario para expresiones regulares

@task
def transform_data(data: bytes) -> pd.DataFrame:
    df = pd.read_excel(BytesIO(data), sheet_name="Program", engine="openpyxl")

    # Elimina filas con más de x NaNs
    max_nans = 10
    df = df.dropna(thresh=max_nans)
    
    # 1. Convertir nombres a una lista
    cols = df.columns.to_list()
    
    # 2. Convertir a minúsculas y reemplazar espacios por guiones bajos
    cols = [col.lower().replace(' ', '_') for col in cols]
    
    # 3. Eliminar acentos usando unicodedata (sigue siendo el método más robusto)
    cols = [unicodedata.normalize('NFD', col).encode('ascii', 'ignore').decode("utf-8") for col in cols]
    
    # 4. Limpiar caracteres especiales con Regex
    # Explicación del regex:
    # [^a-z0-9_] Busca cualquier carácter que NO sea (^) letra minúscula, número o guion bajo.
    # Los reemplaza con un guion bajo (_).
    cols = [re.sub(r'[^a-z0-9_]', '_', col) for col in cols]

    # 5. Eliminar guiones bajos duplicados o iniciales/finales
    cols = [re.sub(r'__+', '_', col).strip('_') for col in cols]
    
    # Asignar los nuevos nombres al DataFrame
    df.columns = cols
    
    # -----------------------------------------------------
    
    # Opcional: conversión de tipos, ahora usando los nombres limpios
    # df['ok_date'] = pd.to_datetime(df['ok_date'], format='%d-%b', errors='coerce')
    # df['mt'] = pd.to_numeric(df['mt'], errors='coerce')

    return df