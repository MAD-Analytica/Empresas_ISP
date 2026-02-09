"""
Script para extraer datos de accesos de internet desde la API de postdata.gov.co
Usa la DKAN Datastore API
"""
import requests
import pandas as pd
import json
from pathlib import Path
import sys
from datetime import datetime
import os

# Agregar el directorio padre al path para importar config
sys.path.append(str(Path(__file__).parent.parent))
import config


def build_params(limit=None, offset=0):
    """
    Construye los parámetros para la API DKAN Datastore
    
    Args:
        limit: Límite de registros por petición
        offset: Offset para paginación
    
    Returns:
        dict: Parámetros para la petición GET
    """
    params = {
        "resource_id": config.RESOURCE_ID,
        "limit": limit if limit else 100,
        "offset": offset
    }
    
    return params


def extract_data_from_api(resource_id):
    """
    Extrae datos de la API DKAN de postdata.gov.co
    
    Args:
        resource_id: ID del recurso en postdata.gov.co
    
    Returns:
        pd.DataFrame: DataFrame con los datos extraídos
    """
    if not resource_id:
        raise ValueError("RESOURCE_ID no está configurado en config.py")
    
    url = f"{config.API_BASE_URL}/search.json"
    
    print(f"Extrayendo datos desde la API...")
    print(f"URL: {url}")
    
    all_data = []
    offset = 0
    limit = 100
    
    while True:
        params = build_params(
            limit=limit,
            offset=offset
        )
        
        print(f"Solicitando registros {offset} - {offset + limit}...")
        
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            # DKAN estructura: {'success': True, 'result': {'records': [...], 'total': N}}
            if not data.get('success', False):
                print(f"Error en respuesta de API: {data.get('error', 'Unknown error')}")
                break
            
            result = data.get('result', {})
            records = result.get('records', [])
            
            if not records:
                print(f"No hay más datos. Total registros obtenidos: {len(all_data)}")
                break
            
            all_data.extend(records)
            
            print(f"   Obtenidos {len(records)} registros (Total acumulado: {len(all_data)})")
            
            # Verificar si hay más datos
            total_records = result.get('total', len(all_data))
            if len(all_data) >= total_records:
                print(f"Extracción completa. Total registros: {len(all_data)}")
                break
            
            # Si obtuvimos menos registros que el límite, ya no hay más datos
            if len(records) < limit:
                print(f"Extracción completa. Total registros: {len(all_data)}")
                break
            
            offset += limit
            
        except requests.exceptions.Timeout:
            print(f"Timeout en la petición. Reintentando...")
            continue
            
        except requests.exceptions.RequestException as e:
            print(f"Error en la petición: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Status code: {e.response.status_code}")
                print(f"Respuesta: {e.response.text[:500]}")
            raise
        
        except json.JSONDecodeError as e:
            print(f"Error al decodificar JSON: {e}")
            print(f"Respuesta: {response.text[:500]}")
            raise
    
    if not all_data:
        print("No se obtuvieron datos")
        return pd.DataFrame()
    
    # Convertir a DataFrame
    df = pd.DataFrame(all_data)
    
    print(f"\nDataFrame creado con {len(df)} filas y {len(df.columns)} columnas")
    
    return df


def save_raw_data(df, filename=None):
    """
    Guarda los datos raw en CSV
    
    Args:
        df: DataFrame a guardar
        filename: Nombre del archivo (opcional)
    """
    if df.empty:
        print("No hay datos para guardar")
        return
    
    filename = filename or config.RAW_FILENAME
    filepath = os.path.join(config.RAW_DATA_DIR, filename)
    
    print(f"\nGuardando datos en: {filepath}")
    df.to_csv(filepath, index=False, sep=';')
    
    print(f"Datos guardados exitosamente")


def run():
    """
    Función principal para ejecutar la extracción
    """
    print("=" * 60)
    print("EXTRACCIÓN DE DATOS - ACCESOS INTERNET FIJO")
    print("=" * 60)
    
    try:
        # Extraer datos
        df = extract_data_from_api(
            resource_id=config.RESOURCE_ID
        )
        
        if not df.empty:
            print("\n" + "=" * 60)
            print("RESUMEN DE DATOS EXTRAÍDOS")
            print("=" * 60)
            print(f"Columnas: {list(df.columns)}")
            print(f"\nPrimeras filas:")
            print(df.head())
            print(f"\nInfo del DataFrame:")
            print(df.info())
            
            # Guardar
            save_raw_data(df)
        
        print("\n" + "=" * 60)
        print("EXTRACCIÓN COMPLETADA")
        print("=" * 60)
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"ERROR EN LA EXTRACCIÓN: {e}")
        print("=" * 60)
        raise


if __name__ == "__main__":
    run()
