"""
Script para transformar datos raw y generar bases procesadas
"""
import pandas as pd
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
import config


# Mapeos de clasificación
MAPEO_TECNOLOGIAS = {
    'Fiber to the home (FTTH)': 'Fibra avanzada',
    'Otras tecnologías de fibra (antes FTTx)': 'Fibra avanzada', 
    'Fiber to the building o fiber to the basement (FTTB)': 'Fibra avanzada',
    'Fiber to the premises': 'Fibra avanzada',
    'Fiber to the cabinet (FTTC)': 'Fibra avanzada',
    'Fiber to the node (FTTN)': 'Fibra avanzada',
    'Fiber to the antenna (FTTA)': 'Fibra avanzada',
    'Cable': 'Cable-HFC',
    'Hybrid Fiber Coaxial (HFC)': 'Cable-HFC',
    'xDSL': 'xDSL',
    'Otras tecnologías inalámbricas': 'Inalámbricas',
    'WiFi': 'Inalámbricas',
    'WiMAX': 'Inalámbricas',
    'Satelital': 'Satelital'
}

MAPEO_SEGMENTOS = {
    'Corporativo': 'corporativo',
    'Corporativo  (accesos adicionales)': 'corporativo',
    'Uso propio interno del operador': 'NA',
    'Residencial - Estrato 1': 'residencial',
    'Residencial - Estrato 2': 'residencial',
    'Residencial - Estrato 3': 'residencial',
    'Residencial - Estrato 4': 'residencial',
    'Residencial - Estrato 5': 'residencial',
    'Residencial - Estrato 6': 'residencial',
    'Sin estratificar': 'residencial'
}

MAPEO_ESTRATOS = {
    'Residencial - Estrato 1': 'bajo',
    'Residencial - Estrato 2': 'bajo',
    'Residencial - Estrato 3': 'medio',
    'Residencial - Estrato 4': 'medio',
    'Residencial - Estrato 5': 'alto',
    'Residencial - Estrato 6': 'alto',
    'Sin estratificar': 'sin_estrato',
    'Corporativo': 'no_aplica',
    'Corporativo  (accesos adicionales)': 'no_aplica',
    'Uso propio interno del operador': 'no_aplica'
}


def load_raw_data():
    """Carga datos raw desde CSV"""
    filepath = config.RAW_DATA_DIR / config.RAW_FILENAME
    
    print(f"Cargando datos de: {filepath}")
    
    df = pd.read_csv(
        filepath,
        sep=';',
        na_values=['', 'NA', 'null'],
        keep_default_na=True,
        on_bad_lines='warn'
    )
    
    print(f"Cargados {len(df)} registros")
    return df


def apply_transformations(df):
    """Aplica mapeos y limpieza de datos"""
    print("\nAplicando transformaciones...")
    
    # Crear grupos
    df['grupo_tecnologia'] = df['tecnologia'].map(MAPEO_TECNOLOGIAS)
    df['grupo_segmento'] = df['segmento'].map(MAPEO_SEGMENTOS)
    df['grupo_estrato'] = df['segmento'].map(MAPEO_ESTRATOS)
    
    # Convertir tipos
    df['id_empresa'] = df['id_empresa'].astype(str)
    df['velocidad_efectiva_downstream'] = df['velocidad_efectiva_downstream'].str.replace(',', '.').astype(float)
    df['velocidad_efectiva_upstream'] = df['velocidad_efectiva_upstream'].str.replace(',', '.').astype(float)
    
    print("Transformaciones aplicadas")
    return df



def generate_base_detallada(df):
    """Genera base detallada por empresa/trimestre/municipio con grupos"""
    print("\nGenerando base detallada...")
    
    df_grouped = df.groupby([
        'id_empresa', 'empresa', 'anno', 'trimestre', 
        'id_municipio', 'municipio', 'id_departamento', 'departamento',
        'grupo_segmento', 'grupo_estrato', 'grupo_tecnologia'
    ]).agg(
        accesos=('accesos', 'sum'),
        velocidad_bajada=('velocidad_efectiva_downstream', lambda x: x.mean(numeric_only=True)),
        velocidad_subida=('velocidad_efectiva_upstream', lambda x: x.mean(numeric_only=True))
    ).reset_index()
    
    filepath = config.PROCESSED_DATA_DIR / config.OUTPUT_BASE_FILENAME
    df_grouped.to_csv(filepath, index=False)
    print(f"Base detallada guardada: {filepath} ({len(df_grouped)} registros)")
    
    return df_grouped


def generate_resumen(df_grouped):
    """Genera resumen agregado por empresa/municipio/trimestre"""
    print("\nGenerando resumen empresa/municipio/trimestre...")
    
    # Excluir accesos no relevantes
    mask_relevantes = df_grouped['grupo_segmento'] != 'NA'
    
    df_resumen = df_grouped.loc[mask_relevantes].groupby([
        'id_empresa', 'empresa', 'anno', 'trimestre',
        'id_municipio', 'municipio', 'id_departamento', 'departamento'
    ]).agg(
        num_accesos=('accesos', 'sum'),
        velocidad_subida=('velocidad_subida', 'mean'),
        velocidad_bajada=('velocidad_bajada', 'mean')
    ).reset_index()
    
    # Ordenar
    df_resumen = df_resumen.sort_values(
        by=['id_empresa', 'empresa', 'id_municipio', 'municipio', 
            'id_departamento', 'departamento', 'anno', 'trimestre']
    )
    
    # Calcular variaciones
    df_resumen['variacion_accesos'] = df_resumen.groupby(
        ['id_empresa', 'id_municipio']
    )['num_accesos'].diff()
    
    df_resumen['tasa_variacion'] = (
        df_resumen['variacion_accesos'] / 
        df_resumen.groupby(['id_empresa', 'id_municipio'])['num_accesos'].shift(1)
    )
    
    filepath = config.PROCESSED_DATA_DIR / config.OUTPUT_RESUMEN_FILENAME
    df_resumen.to_csv(filepath, index=False)
    print(f"Guardado: {filepath} ({len(df_resumen)} registros)")
    
    return df_resumen


def generate_empresa_trimestre(df_grouped):
    """Genera base agregada por empresa/trimestre (sin municipio)"""
    print("\nGenerando base empresa/trimestre...")
    
    # Excluir accesos no relevantes
    mask_relevantes = df_grouped['grupo_segmento'] != 'NA'
    
    df_emp_trim = df_grouped.loc[mask_relevantes].groupby([
        'id_empresa', 'empresa', 'anno', 'trimestre'
    ]).agg(
        num_accesos=('accesos', 'sum'),
        velocidad_subida=('velocidad_subida', 'mean'),
        velocidad_bajada=('velocidad_bajada', 'mean')
    ).reset_index()
    
    # Ordenar
    df_emp_trim = df_emp_trim.sort_values(by=['id_empresa', 'empresa', 'anno', 'trimestre'])
    
    # Calcular variaciones
    df_emp_trim['variacion_accesos'] = df_emp_trim.groupby('id_empresa')['num_accesos'].diff()
    df_emp_trim['tasa_variacion'] = (
        df_emp_trim['variacion_accesos'] / 
        df_emp_trim.groupby('id_empresa')['num_accesos'].shift(1)
    )
    
    filepath = config.PROCESSED_DATA_DIR / config.OUTPUT_EMPRESA_TRIM_FILENAME
    df_emp_trim.to_csv(filepath, index=False)
    print(f"Guardado: {filepath} ({len(df_emp_trim)} registros)")
    
    return df_emp_trim


def run():
    """Ejecuta pipeline completo de transformación"""
    print("=" * 60)
    print("TRANSFORMACIÓN DE DATOS")
    print("=" * 60)
    
    try:
        df = load_raw_data()
        df = apply_transformations(df)
        
        df_base = generate_base_detallada(df)
        df_resumen = generate_resumen(df_base)
        df_emp_trim = generate_empresa_trimestre(df_base)
        
        print("\n" + "=" * 60)
        print("TRANSFORMACIÓN COMPLETADA")
        print("=" * 60)
        print(f"\nArchivos generados en: {config.PROCESSED_DATA_DIR}")
        print(f"  - {config.OUTPUT_BASE_FILENAME}")
        print(f"  - {config.OUTPUT_RESUMEN_FILENAME}")
        print(f"  - {config.OUTPUT_EMPRESA_TRIM_FILENAME}")
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"ERROR EN LA TRANSFORMACIÓN: {e}")
        print("=" * 60)
        raise


if __name__ == "__main__":
    run()

