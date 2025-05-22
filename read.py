import pandas as pd
import os
import time

# Define el filepath - ajusta esto a la ubicación de tu archivo
filepath = '/Users/juliandiazparra/Library/CloudStorage/GoogleDrive-juliandp@madanalytica.com/.shortcut-targets-by-id/1-Q0tRpY6LLjvh4cZeZWt2DLY2Scu6Kml/Buenos Negocios Studio/Data/ACCESOS_INTERNET_FIJO_3_15.csv'

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

# Agrupar datos por empresa, año y trimestre
df_grouped = df.groupby(['ID_EMPRESA', 'ANNO', 'TRIMESTRE'])['ACCESOS'].sum().reset_index()

# Obtener las 20 empresas con más accesos totales
top_20_empresas = df_grouped.groupby('ID_EMPRESA')['ACCESOS'].sum().nlargest(20).index

# Filtrar solo las 20 empresas principales
df_top20 = df_grouped[df_grouped['ID_EMPRESA'].isin(top_20_empresas)]

# Crear un período temporal combinando año y trimestre
df_top20['Periodo'] = df_top20['ANNO'].astype(str) + '-T' + df_top20['TRIMESTRE'].astype(str)
df_top20.head()
# Crear la gráfica
import matplotlib.pyplot as plt

plt.figure(figsize=(15, 8))

# Graficar una línea para cada empresa
for empresa in top_20_empresas:
    data = df_top20[df_top20['ID_EMPRESA'] == empresa]
    plt.plot(data['Periodo'], data['ACCESOS'], label=empresa, marker='o')

plt.title('Evolución de Accesos por Empresa (Top 20)')
plt.xlabel('Período (Año-Trimestre)')
plt.ylabel('Número de Accesos')
plt.xticks(rotation=45)
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True)
plt.tight_layout()
plt.show()


# Obtener el total de accesos por empresa
accesos_por_empresa = df_grouped.groupby('ID_EMPRESA')['ACCESOS'].sum()

# Filtrar empresas entre 1000 y 20000 accesos
empresas_filtradas = accesos_por_empresa[(accesos_por_empresa >= 1000) & (accesos_por_empresa <= 20000)].index

# Filtrar el dataframe para estas empresas
df_filtrado = df_grouped[df_grouped['ID_EMPRESA'].isin(empresas_filtradas)]

# Crear período temporal
df_filtrado['Periodo'] = df_filtrado['ANNO'].astype(str) + '-T' + df_filtrado['TRIMESTRE'].astype(str)

# Crear la gráfica
plt.figure(figsize=(15, 8))

# Graficar una línea para cada empresa
for empresa in empresas_filtradas:
    data = df_filtrado[df_filtrado['ID_EMPRESA'] == empresa]
    plt.plot(data['Periodo'], data['ACCESOS'], label=empresa, marker='o')

plt.title('Evolución de Accesos por Empresa (Entre 1000 y 20000 accesos totales)')
plt.xlabel('Período (Año-Trimestre)')
plt.ylabel('Número de Accesos')
plt.xticks(rotation=45)
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True)
plt.tight_layout()
plt.show()

# Agrupar por empresa, municipio, año y trimestre
df_empresa_mpio_trim = df.groupby(['ID_EMPRESA', 'ID_MUNICIPIO', 'ANNO', 'TRIMESTRE'])['ACCESOS'].sum().reset_index()

# Ordenar para calcular correctamente la variación
df_empresa_mpio_trim = df_empresa_mpio_trim.sort_values(['ID_EMPRESA', 'ID_MUNICIPIO', 'ANNO', 'TRIMESTRE'])

# Calcular la variación de accesos
df_empresa_mpio_trim['variacion_accesos'] = df_empresa_mpio_trim.groupby(['ID_EMPRESA', 'ID_MUNICIPIO'])['ACCESOS'].diff()

#Calculo tasa de variación
df_empresa_mpio_trim['tasa_variacion'] = df_empresa_mpio_trim['variacion_accesos'] / df_empresa_mpio_trim['ACCESOS']

# Obtener el percentil 90 de variación de accesos
percentil_90 = df_empresa_mpio_trim['tasa_variacion'].quantile(0.9)

# Filtrar datos por encima del percentil 90
# Obtener el percentil 75 de variación de accesos
percentil_75 = df_empresa_mpio_trim['tasa_variacion'].quantile(0.75)

# Filtrar datos por encima del percentil 75
df_top_variacion = df_empresa_mpio_trim[df_empresa_mpio_trim['tasa_variacion'] >= percentil_75]

# Crear período temporal
df_top_variacion['Periodo'] = df_top_variacion['ANNO'].astype(str) + '-T' + df_top_variacion['TRIMESTRE'].astype(str)

#Tasa de variación promedio por empresa
df_empresa_mpio_trim['tasa_variacion_promedio'] = df_empresa_mpio_trim.groupby(['ID_EMPRESA', 'ID_MUNICIPIO'])['tasa_variacion'].transform('mean')
# Crear la gráfica
plt.figure(figsize=(15, 8))

# Graficar solo los puntos que superan el percentil 75
plt.scatter(df_top_variacion['Periodo'], 
           df_top_variacion['tasa_variacion'],
           c=df_top_variacion['ID_EMPRESA'].astype('category').cat.codes,
           label=df_top_variacion['ID_EMPRESA'])

plt.title('Puntos con Variaciones en el Top 25% por Empresa')
plt.xlabel('Período (Año-Trimestre)')
plt.ylabel('Variación de Accesos')
plt.xticks(rotation=45)
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True)
plt.tight_layout()
plt.show()

print(df_top_variacion.merge(df_empresa_mpio_trim, on=['ID_EMPRESA', 'ID_MUNICIPIO', 'ANNO', 'TRIMESTRE'], how='left'))