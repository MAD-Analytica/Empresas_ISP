from datetime import datetime

# PostData CRC - ID del recurso de internet fijo
RESOURCE_ID = "34bbf5b5-0537-4bf0-8836-3f51d1a24162"
API_BASE_URL = "https://www.postdata.gov.co/api/action/datastore"

# Archivos
RAW_FILENAME = "accesos_internet_fijo.csv"
RAW_DATA_DIR = "data/raw"
PROCESSED_DATA_DIR = "data/processed"
FINAL_DATA_DIR = "data/processed/finals"

# Carpeta destino (ajustar a tu ruta local)
LOAD_DATA_DIR = "G:/Mi unidad/tu-carpeta-destino"

# APIs de enriquecimiento
RUES_API_URL = "https://www.datos.gov.co/resource/c82u-588k.json"
WHOIS_IPIP_URL = "https://whois.ipip.net/search"

# Nombres de archivos de salida
OUTPUT_RESUMEN_FILENAME = f"accesos-emp-mpio-trim-{datetime.today().strftime('%Y%m%d')}.csv"
OUTPUT_EMPRESA_TRIM_FILENAME = f"accesos-emp-trim-{datetime.today().strftime('%Y%m%d')}.csv"
OUTPUT_BASE_FILENAME = f"base_accesos_emp-trim-{datetime.today().strftime('%Y%m%d')}.csv"
OUTPUT_EMPRESAS_FILENAME = f"empresas_enriquecidas-{datetime.today().strftime('%Y%m%d %H%M%S')}.csv"
OUTPUT_EMPRESAS_TABLA_FILENAME = f"tabla-empresas-{datetime.today().strftime('%Y%m%d %H%M%S')}.csv"
OUTPUT_LEADS_FILENAME = f"tabla-leads-{datetime.today().strftime('%Y%m%d %H%M%S')}.csv"
