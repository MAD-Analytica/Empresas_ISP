import pandas as pd
import os
import time

# Define el filepath - ajusta esto a la ubicación de tu archivo
filepath = 'data_ISPs/ACCESOS_INTERNET_FIJO_3_15.csv'

df = pd.read_csv(
    filepath,
    sep=';',
    na_values=['', 'NA', 'null'],
    keep_default_na=True,
    on_bad_lines='warn'
)

print(df.head())
print(df.info())
print(df.describe())
print(df.isnull().sum())
print(df.duplicated().sum())

#Agrupar datos por empresa, municipio, año y trimestre
#Qué tecnologías son relevantes?
df['TECNOLOGIA'].value_counts()
#Qué tipos de clientes hay?
df['SEGMENTO'].value_counts()

# Creo grupos de tecnologías
mapeo_tecnologias = {
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

# Crear variables: grupo_segmento: residencial(todos los estratos y sin estratificar), corporativo. Estrato: bajo (1,2), medio (3,4), alto (5,6).
mapeo_segmentos = {
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

mapeo_estratos = {
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

# Crear nueva columna con los grupos de tecnologías
df['grupo_tecnologia'] = df['TECNOLOGIA'].map(mapeo_tecnologias)
#Creo columna grupo_segmento y grupo_estrato
df['grupo_segmento'] = df['SEGMENTO'].map(mapeo_segmentos)
df['grupo_estrato'] = df['SEGMENTO'].map(mapeo_estratos)

# Mostrar las distribuciones de los nuevos grupos
print("\nDistribución por grupo de segmento:")
print(df['grupo_segmento'].value_counts())
print("\nDistribución por grupo de estrato:")
print(df['grupo_estrato'].value_counts())
# Mostrar el conteo de accesos por grupo de tecnología
print("\nDistribución de accesos por grupo de tecnología:")
print(df['grupo_tecnologia'].value_counts())

# Asegurar que ID_EMPRESA sea tratado como string
df['ID_EMPRESA'] = df['ID_EMPRESA'].astype(str)
# Convertir las comas a puntos y luego a numérico
df['VELOCIDAD_EFECTIVA_DOWNSTREAM'] = df['VELOCIDAD_EFECTIVA_DOWNSTREAM'].str.replace(',', '.').astype(float)
df['VELOCIDAD_EFECTIVA_UPSTREAM'] = df['VELOCIDAD_EFECTIVA_UPSTREAM'].str.replace(',', '.').astype(float)

# Agrupar datos por empresa, año y trimestre, grupo_segmento, grupo_estrato, grupo_tecnologia
df_grouped = df.groupby(['ID_EMPRESA', 'EMPRESA', 'ANNO', 'TRIMESTRE', 
                         'ID_MUNICIPIO', 'MUNICIPIO', 'ID_DEPARTAMENTO', 'DEPARTAMENTO',
                        'grupo_segmento', 'grupo_estrato', 'grupo_tecnologia']).agg(
                            accesos=('ACCESOS', 'sum'),
                            velocidad_bajada=('VELOCIDAD_EFECTIVA_DOWNSTREAM', lambda x: x.mean(numeric_only=True)),
                            velocidad_subida=('VELOCIDAD_EFECTIVA_UPSTREAM', lambda x: x.mean(numeric_only=True))).reset_index()


#Guardo esta base
df_grouped.to_csv('data_ISPs/base_accesos_empresasxQxmpio.csv', index=False)

#Excluyamos accesos no relevantes
mask_no_relevantes = df_grouped['grupo_segmento']!='NA'
df_empresa_mpio_trim = df_grouped.loc[
    mask_no_relevantes].groupby(['ID_EMPRESA', 'EMPRESA',
                                 'ANNO', 'TRIMESTRE',
                                 'ID_MUNICIPIO','MUNICIPIO',
                                 'ID_DEPARTAMENTO','DEPARTAMENTO']).agg(
                        num_accesos=('accesos', 'sum'),
                        velocidad_subida=('velocidad_subida', 'mean'),
                        velocidad_bajada=('velocidad_bajada', 'mean')).reset_index()

#Ordeno por empresa, municipio, año y trimestre
df_empresa_mpio_trim = df_empresa_mpio_trim.sort_values(
    by=['ID_EMPRESA', 'EMPRESA', 
        'ID_MUNICIPIO', 'MUNICIPIO', 
        'ID_DEPARTAMENTO', 'DEPARTAMENTO',
        'ANNO', 'TRIMESTRE'])
#Calculo variación de accesos y tasa de variación
df_empresa_mpio_trim['variacion_accesos'] = df_empresa_mpio_trim.groupby(
    ['ID_EMPRESA', 'ID_MUNICIPIO'])['num_accesos'].diff()
# Agregar tasa de variación (%)
df_empresa_mpio_trim['tasa_variacion'] = (
    df_empresa_mpio_trim['variacion_accesos'] / 
    df_empresa_mpio_trim.groupby(['ID_EMPRESA', 'ID_MUNICIPIO'])['num_accesos'].shift(1)
)

df_empresa_mpio_trim.loc[df_empresa_mpio_trim['ID_EMPRESA']=='901448700', 
                         ['ANNO','TRIMESTRE','EMPRESA','num_accesos',
                          'variacion_accesos','tasa_variacion']]

df_empresa_mpio_trim.loc[df_empresa_mpio_trim['EMPRESA'].str.contains('ISPA'), 
                         ['ANNO','TRIMESTRE','EMPRESA','MUNICIPIO','num_accesos',
                          'variacion_accesos','tasa_variacion']]

#GUardo resumen
df_empresa_mpio_trim.to_csv('data_ISPs/resumen_accesos_empresasxQxmpio.csv', index=False)