import pandas as pd
from prefect import get_run_logger, task
from io import BytesIO
import unicodedata
import re

from database.db_destinations import get_all_destinations_and_country
from database.db_packaging import get_all_packaging
from database.db_product import get_all_products

@task(name="Parse Excel Sheet")
def parse_excel_sheet(data: bytes, sheet_name: str, header_row: int = 0) -> pd.DataFrame:
    """
    Convierte los bytes del archivo en un DataFrame seleccionando una hoja específica.
    """
    logger = get_run_logger()
    logger.info(f"Parsing sheet '{sheet_name}'...")
    
    try:
        df = pd.read_excel(
            BytesIO(data), 
            sheet_name=sheet_name, 
            header=header_row, 
            engine="openpyxl"
        )
        return df
    except Exception as e:
        logger.error(f"Error parsing sheet {sheet_name}: {e}")
        raise e

@task(name="Clean DataFrame")
def clean_dataframe(df: pd.DataFrame, context_name: str) -> pd.DataFrame:
    """
    Aplica la limpieza estándar (ñ, acentos, normalización) a un DataFrame ya cargado.
    'context_name' sirve solo para logs (ej: 'Program' o 'IFR').
    """
    logger = get_run_logger()
    logger.info(f"Cleaning data for {context_name}...")
    
    try:
        # Eliminar filas con más de 'max_nans' valores faltantes
        max_nans = 10
        df = df.dropna(thresh=max_nans)
        
        # --- Normalización de nombres de columnas ---
        cols = df.columns.to_list()

        # 1. Convertir a minúsculas y reemplazar espacios
        cols = [str(col).lower().replace(' ', '_') for col in cols]

        # 2. Reemplazo ESPECÍFICO para "año" -> "anio"
        cols = [col.replace('año', 'anio') for col in cols]
        
        # 3. Eliminar acentos y tildes
        cols = [unicodedata.normalize('NFD', col).encode('ascii', 'ignore').decode("utf-8") for col in cols]
        
        # 4. Reemplazar caracteres no alfanuméricos
        cols = [re.sub(r'[^a-z0-9_]', '_', col) for col in cols]

        # 5. Limpiar guiones bajos extra
        cols = [re.sub(r'__+', '_', col).strip('_') for col in cols]
        
        df.columns = cols
        
        logger.info(f"Data cleaned successfully for {context_name}")
        return df
    
    except Exception as e:
        logger.error(f"Error cleaning dataframe for {context_name}: {e}")
        raise e

def classify_row(val_a, val_f):
    """
    Aplica TU lógica de validación
    """
    # Limpieza segura de inputs (evita errores con NaN)
    is_a_nan = pd.isna(val_a)
    texto = str(val_a).strip() if not is_a_nan else ""
    
    is_f_nan = pd.isna(val_f)
    texto_col_f = str(val_f).strip().lower() if not is_f_nan else ""

    # --- 1. VALIDACIÓN DE MÉTRICAS (Literales Exactos) ---
    # Mapeamos el texto del Excel al nombre de columna en la BD
    metrics_map = {
        'Arrivals + Sailed': 'arrivals_sailed',
        'Planned (w/booking)': 'planned_wbooking',
        'To be booked': 'to_be_booked',
        'Sales': 'sales',
        'Adjustments': 'adjustments',
        'Final Inv.': 'final_inv'
    }
    
    if texto in metrics_map:
        return 'metric', metrics_map[texto]

    # --- 2. VALIDACIÓN MOS (Tu lógica ajustada) ---
    # Si texto (Col C) es NaN y Col F es 'mos'
    if is_a_nan: 
        if texto_col_f == 'mos':
            return 'metric', 'mos'
        return None, None # Es NaN pero no es MOS

    # --- 3. VALIDACIÓN DE HEADER (Tus 3 Reglas de Split) ---
    
    # REGLA 1: Separar por "-" debe dar longitud 1 o 2
    partes_guion = texto.split(' - ')
    if len(partes_guion) not in [1, 2]:
        return None, None
    
    # REGLA 2: Primer elemento split por espacios -> longitud 3
    # Ejemplo: "1.1.1.1 Wil (CRY/CL"
    primer_elemento = partes_guion[0].strip()
    partes_espacio = primer_elemento.split()
    
    if len(partes_espacio) != 3:
        return None, None

    # REGLA 3: Tercer elemento split por "/" -> longitud 2
    # Ejemplo: "(CRY/CL"
    tercer_elemento = partes_espacio[2]
    partes_slash = tercer_elemento.split('/')
    
    if len(partes_slash) != 2:
        return None, None

    # --- SI PASA LAS REGLAS, EXTRAEMOS LA DATA ---
    try:
        # Usamos las mismas partes que ya validamos
        # partes_espacio = ['1.1.1.1', 'Wil', '(CRY/CL']
        codigo = partes_espacio[0]
        filial = partes_espacio[1]
        
        # partes_slash = ['(CRY', 'CL']
        producto = partes_slash[0].replace('(', '')
        
        # Reconstrucción del envase
        envase_inicio = partes_slash[1] # "CL"
        envase_fin = partes_guion[1] if len(partes_guion) == 2 else "" # "50L)"
        
        raw_envase = f"{envase_inicio} {envase_fin}" if envase_fin else envase_inicio
        envase = raw_envase.replace(')', '').strip('-')

        return 'header', {
            "filial": filial,
            "producto": producto,
            "envase": envase
        }
    except Exception:
        return None, None



@task(name="Transform IFR Excel")
def transform_ifr_excel(file_content: bytes) -> pd.DataFrame:
    logger = get_run_logger()

    # --- 1. OBTENCIÓN DE DATOS PARAMÉTRICOS (DESDE BD) ---
    logger.info("Obteniendo maestros de base de datos...")
    packaging_list = get_all_packaging()
    destinations_list = get_all_destinations_and_country()
    products_list = get_all_products()
    
    # --- 2. CREACIÓN DE DICCIONARIOS DE BÚSQUEDA (HASH MAPS) ---
    # Convertimos las listas en diccionarios para búsqueda rápida y normalizada (minusculas)
    
    # Mapa: 'cry9000.00' -> id_product
    map_products = {
        p['product_name'].strip().lower(): p['id_product'] 
        for p in products_list if p['product_name']
    }
    
    # Mapa: 'cl-50l' -> id_packaging
    map_packaging = {
        p['packaging_code'].strip().lower(): p['id_packaging'] 
        for p in packaging_list if p['packaging_code']
    }
    
    # Mapa: 'wilmington' -> {'id': id_destination, 'country': 'USA'}
    map_destinations = {
        d['destination_name'].strip().lower(): {'id': d['id_destination'], 'country': d['country']} 
        for d in destinations_list if d['destination_name']
    }

    # --- 3. LECTURA DEL EXCEL ---
    # C=0, D=1, E=2, F=3, G=4 ...
    df = pd.read_excel(BytesIO(file_content), sheet_name="IFR", header=None, engine="openpyxl", usecols="C:AE")

    # Eliminar filas completamente vacías
    df = df.dropna(how='all').reset_index(drop=True)

    # --- 4. MAPA DE COLUMNAS (Fechas) ---
    COLUMN_MAP = {
        5: "01-2025",  
        6: "02-2025",  
        7: "03-2025",  
        8: "04-2025",  
        9: "05-2025",
        10: "06-2025",
        11: "07-2025",
        12: "08-2025",
        13: "09-2025",
        14: "10-2025",
        15: "11-2025",
        16: "12-2025",
        17: "01-2026",
        18: "02-2026",
        19: "03-2026",
        20: "04-2026",
        21: "05-2026",
        22: "06-2026",
        23: "07-2026",
        24: "08-2026",
        25: "09-2026",
        26: "10-2026",
        27: "11-2026",
        28: "12-2026",
    }

    processed_rows = []
    current_ids = None # Aquí guardaremos los IDs en lugar del texto

    # --- 5. ITERACIÓN ---
    # row[0]=Index, row[1]=Col C, ... row[4]=Col F ...
    for row in df.itertuples(index=True):
        
        val_c = row[1] # Valor Columna C
        
        # Obtenemos valor de Col F (Index 4 si C es 0, D=1, E=2, F=3... espera)
        # Nota: itertuples incluye el índice en posición 0.
        # row[1] es la primera columna de datos (Col C).
        # row[4] sería la cuarta columna de datos (Col F).
        try:
            val_f = row[4] 
        except IndexError:
            val_f = None

        # --- CLASIFICACIÓN (Usa tu función existente classify_row) ---
        if val_c == '3.4.1 Shanghai (MIC9000.00/CL-500)':
            logger.warning(f"IFR 3.4.1 Shanghai (MIC9000.00/CL-500) Saltado")
            continue
        row_type, data = classify_row(val_c, val_f)

        if row_type == 'header':
            # Data trae: {'filial': 'Wilmington', 'producto': 'CRY...', 'envase': 'CL...', 'pais': '...'}
            
            # 1. Normalizar textos del Excel
            txt_dest = str(data['filial']).strip().lower()
            txt_prod = str(data['producto']).strip().lower()
            txt_pack = str(data['envase']).strip().lower()

            # 1.1 En caso de que el producto no contenga .00 al final, se le agrega
            if not txt_prod.endswith('.00'):
                if len(txt_prod.split()) == 1:
                    txt_prod = txt_prod+'.00'
            
            # 2. Buscar IDs en los mapas
            dest_info = map_destinations.get(txt_dest)
            id_prod = map_products.get(txt_prod)
            id_pack = map_packaging.get(txt_pack)

            # 3. Lógica de Destino/País
            if dest_info:
                id_dest = dest_info['id']
                country_real = dest_info['country']
            else:
                id_dest = None
                # Si no cruza, usamos el país que derivamos del código en classify_row
                country_real = data['pais'] 
                logger.warning(f"Destino no encontrado en BD: '{txt_dest}'")

            if not id_prod:
                logger.warning(f"Producto no encontrado en BD: '{txt_prod}', Filial: {txt_dest}")
            if not id_pack:
                logger.warning(f"Envase no encontrado en BD: '{txt_pack}', Filial: {txt_dest}")

            # 4. Actualizar metadatos actuales con IDs
            current_ids = {
                "id_destination": id_dest,
                "country": country_real,
                "id_product": id_prod,
                "id_packaging": id_pack
            }
            continue 

        elif row_type == 'metric':
            if current_ids is None:
                continue
            
            metric_key = data
            is_mos = (metric_key == 'mos')

            # --- EXTRACCIÓN HORIZONTAL ---
            for col_idx, periodo in COLUMN_MAP.items():
                try:
                    # +1 para compensar el Index de la tupla
                    raw_val = row[col_idx + 1]
                except IndexError:
                    raw_val = 0

                # Limpieza de valores
                val = None
                if is_mos:
                    if pd.notnull(raw_val):
                        try:
                            # Intentamos convertir a float y redondear a 2 decimales
                            float_val = float(raw_val)
                            val = str(round(float_val, 2))
                        except:
                            # Si falla (es texto), cortamos a 16 chars por seguridad
                            val = str(raw_val)[:16]
                    else:
                        val = None
                else:
                    try:
                        val = float(raw_val) if pd.notnull(raw_val) else 0.0
                    except:
                        val = 0.0

                # Periodo Equivalente
                try:
                    mes, anio = periodo.split('-')
                    per_eq = int(f"{anio}{mes}")
                except:
                    per_eq = 0

                processed_rows.append({
                    "id_destination": current_ids["id_destination"], # ID
                    "country": current_ids["country"],
                    "id_product": current_ids["id_product"],         # ID
                    "id_packaging": current_ids["id_packaging"],     # ID
                    "periodo": periodo,
                    "periodoequivalente": col_idx - 4, #se resta 4 para que coincida con la numeracion de meses del 1 al 24 (dos años)
                    "metric_type": metric_key,
                    "value": val
                })

    # --- 6. PIVOT FINAL ---
    if not processed_rows:
        return pd.DataFrame()

    df_flat = pd.DataFrame(processed_rows)
    
    # Pivotamos usando los nuevos campos de ID
    df_pivoted = df_flat.pivot_table(
        index=["id_destination", "country", "id_product", "id_packaging", "periodo", "periodoequivalente"], 
        columns="metric_type", 
        values="value",
        aggfunc='first'
    ).reset_index()

    df_pivoted.columns.name = None
    
    # Rellenar columnas faltantes
    required_cols = ["arrivals_sailed", "planned_wbooking", "to_be_booked", "sales", "adjustments", "final_inv", "mos"]
    for c in required_cols:
        if c not in df_pivoted.columns:
            df_pivoted[c] = None

    cols_to_int = ["id_destination", "id_product", "id_packaging"]
    for col in cols_to_int:
        if col in df_pivoted.columns:
            df_pivoted[col] = df_pivoted[col].astype("Int64")
    
    df_pivoted = df_pivoted.rename(columns={
        "id_destination": "filial",
        "id_product": "producto",
        "id_packaging": "envase",
        "country": "pais"
    })

    return df_pivoted