import pandas as pd
import os

# Define el filepath - ajusta esto a la ubicación de tu archivo
filepath = 'data_ISPs/resumen_accesos_empresasxQxmpio.csv'

df_estudio = pd.read_csv(
    filepath,
    na_values=['', 'NA', 'null'],
    keep_default_na=True,
    on_bad_lines='warn'
)

print(df_estudio.head())
print(df_estudio.info())
print(df_estudio.describe())
print(df_estudio.isnull().sum())
print(df_estudio.duplicated().sum())

df_estudio['ID_EMPRESA']=df_estudio['ID_EMPRESA'].astype(str)

df_estudio.loc[df_estudio['ID_EMPRESA']=='901448700', 
                         ['ANNO','TRIMESTRE','EMPRESA','MUNICIPIO','DEPARTAMENTO','num_accesos',
                          'variacion_accesos','tasa_variacion']]

df_estudio.loc[df_estudio['EMPRESA'].str.contains('Conectamos Soluciones', case=False), 
                         ['ANNO','TRIMESTRE','EMPRESA','MUNICIPIO','DEPARTAMENTO','num_accesos',
                          'variacion_accesos','tasa_variacion']]

# Filtro el último trimestre de 2024 y empresas con más de 1000 usuarios
mask_ultimo_trim = (df_estudio['ANNO']==2024) & (df_estudio['TRIMESTRE']==4)
mask_usuarios = df_estudio['num_accesos'] >= 25
# Calculo el promedio de la tasa de variación para ese trimestre
print("\nPromedio nacional de tasa de variación en Q4 2024 (empresas >1000 usuarios):")
print(df_estudio.loc[mask_ultimo_trim & mask_usuarios, 'tasa_variacion'].mean())

# Calculo el percentil 95 de la tasa de variación
percentil_95 = df_estudio.loc[mask_ultimo_trim & mask_usuarios, 'tasa_variacion'].quantile(0.95)

print("\nEmpresas-municipio con crecimiento en el top 5% (mayor a {:.2%}):".format(percentil_95))
print(df_estudio.loc[
    (mask_ultimo_trim & mask_usuarios) & 
    (df_estudio['tasa_variacion'] > percentil_95),
    ['EMPRESA', 'MUNICIPIO', 'DEPARTAMENTO', 'num_accesos', 'variacion_accesos','tasa_variacion']
].sort_values('tasa_variacion', ascending=False))


df_empresas_q4_2024 = df_estudio.loc[mask_ultimo_trim & mask_usuarios].groupby(
    ['ID_EMPRESA','EMPRESA']).agg(num_accesos=('num_accesos','sum'),
                                  variacion_accesos=('variacion_accesos','sum')).reset_index()
df_empresas_q4_2024['tasa_variacion'] = df_empresas_q4_2024['variacion_accesos'] / (df_empresas_q4_2024['num_accesos']-df_empresas_q4_2024['variacion_accesos'])
df_empresas_q4_2024.sort_values('tasa_variacion', ascending=False, inplace=True)
print("Top 10 empresas mayor tasa de crecimiento en accesos Q4-2024")
print(df_empresas_q4_2024.head(10))

df_empresas_q4_2024.sort_values('variacion_accesos', ascending=False, inplace=True)
print("Top 10 empresas mayor crecimiento en accesos Q4-2024")
print(df_empresas_q4_2024.head(10))

#*************************************************
#Quiero calcular los 5 municipios más competidos: 
#1. El líder tiene más del 60% de accesos del municipio
#2. La densidad es mayor a 1.5: más de 1.5 ISPs (con más de 25 accesos) por cada 1000 accesos
#3. El municipio tiene más de 1000 accesos

# Filtramos para el último trimestre y empresas con más de 25 accesos
df_municipios = df_estudio.loc[mask_ultimo_trim & mask_usuarios]

# Calculamos métricas por municipio
df_municipios_metrics = df_municipios.groupby(['DEPARTAMENTO','MUNICIPIO']).agg(
    total_accesos=('num_accesos', 'sum'),
    num_isps=('ID_EMPRESA', 'nunique'),
    accesos_lider=('num_accesos', 'max')  # Calculamos directamente el máximo
).reset_index()

# Calculamos la densidad de ISPs (por cada 1000 accesos)
df_municipios_metrics['densidad_isps'] = df_municipios_metrics['num_isps'] / (df_municipios_metrics['total_accesos']/1000)
#Calculamos el porcentaje del líder en cada municipio
df_municipios_metrics['porcentaje_lider'] = (df_municipios_metrics['accesos_lider'] / df_municipios_metrics['total_accesos']) * 100

municipios_competidos = df_municipios_metrics.loc[(df_municipios_metrics['densidad_isps']>1.5) 
                                                  & (df_municipios_metrics['porcentaje_lider']>60)]

# Ordenamos por total de accesos
municipios_competidos = municipios_competidos.sort_values(
    ['total_accesos','densidad_isps','porcentaje_lider'], ascending=[False,False,False])

print("\nTop 10 municipios más competidos:")
print(municipios_competidos.head(10)[['DEPARTAMENTO','MUNICIPIO', 
                                     'total_accesos','densidad_isps', 
                                     'porcentaje_lider']])


#**************************************************
                        #Graficos#
#**************************************************

import matplotlib.pyplot as plt
import seaborn as sns
sns.set_theme()

# Crear la figura con un tamaño específico
plt.figure(figsize=(15, 8))
# Crear el gráfico de barras
ax = sns.barplot(data=df_empresas_q4_2024.head(10), 
            x='EMPRESA', 
            y='variacion_accesos',
            color='skyblue')
# Rotar las etiquetas del eje x para mejor legibilidad
plt.xticks(rotation=45, ha='right')
# Añadir títulos y etiquetas
plt.title('Top 10 Empresas por Variación de Accesos en Q4 2024', 
          pad=20, 
          fontsize=14)
plt.xlabel('Empresa', fontsize=12)
plt.ylabel('Variación de Accesos', fontsize=12)

# Agregar grid
plt.grid(True, linestyle='--', alpha=0.7)

# Agregar etiquetas de valor en cada barra
for i in ax.containers:
    ax.bar_label(i, fmt='%.0f', padding=3)

# Ajustar el layout para que no se corten las etiquetas
plt.tight_layout()

# Mostrar el gráfico
plt.show()
#**************************************************

# Crear un gráfico de líneas para la tasa de variación por tecnología
plt.figure(figsize=(15, 8))

#Grafico de tasa de variación por tecnología
filepath = 'data_ISPs/base_accesos_empresasxQxmpio.csv'
df_22_24=pd.read_csv(filepath, na_values=['', 'NA', 'null'], keep_default_na=True, on_bad_lines='warn')

# Obtener datos agrupados por trimestre y tecnología    
df_tech = df_22_24.groupby(['ANNO', 'TRIMESTRE', 'grupo_tecnologia']).agg(
    num_accesos=('accesos', 'sum')
).reset_index()

#Calculo variación de accesos
df_tech.sort_values(by=['grupo_tecnologia','ANNO','TRIMESTRE'], inplace=True)
df_tech['variacion_accesos'] = df_tech.groupby(['grupo_tecnologia'])['num_accesos'].diff()
# Calcular tasa de variación por grupo
df_tech['tasa_variacion'] = df_tech['variacion_accesos'] / (df_tech['num_accesos'] - df_tech['variacion_accesos'])

# Crear etiquetas de periodo
df_tech['periodo'] = df_tech['ANNO'].astype(str) + '-Q' + df_tech['TRIMESTRE'].astype(str)

# Crear el gráfico de líneas
for i, tech in enumerate(df_tech['grupo_tecnologia'].unique()):
    data = df_tech[df_tech['grupo_tecnologia'] == tech]
    plt.plot(data['periodo'], data['tasa_variacion'], 
             marker='o', linewidth=2, label=tech)

# Personalizar el gráfico
plt.title('Tasa de Variación Trimestral por Tecnología', fontsize=14, pad=20)
plt.xlabel('Período', fontsize=12)
plt.ylabel('Tasa de Variación (%)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(title='Tecnología', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.xticks(rotation=45, ha='right')

# Ajustar layout
plt.tight_layout()

# Mostrar el gráfico
plt.show()
#**************************************************

# Municipios con mayor crecimiento en accesos

# Agrupo municipio, anno y trimestre
municipios_crecimiento = df_estudio.groupby(['ANNO','TRIMESTRE','MUNICIPIO']).agg(
    variacion_accesos=('variacion_accesos', 'sum'), 
    accesos=('num_accesos', 'sum')
).reset_index()

municipios_crecimiento['tasa_variacion'] = municipios_crecimiento[
    'variacion_accesos'] / (municipios_crecimiento[
        'accesos']-municipios_crecimiento[
            'variacion_accesos'])

plt.figure(figsize=(15, 8))

# Crear etiqueta de periodo
municipios_crecimiento['periodo'] = municipios_crecimiento['ANNO'].astype(str) + '-Q' + municipios_crecimiento['TRIMESTRE'].astype(str)

# Calcular el promedio de tasa de variación de los últimos 4 trimestres por municipio
ultimos_4q = municipios_crecimiento.sort_values(['ANNO', 'TRIMESTRE']).groupby('MUNICIPIO').tail(4)
promedio_crecimiento = ultimos_4q.groupby('MUNICIPIO')['tasa_variacion'].mean()

# Obtener los 10 municipios con mayor promedio de crecimiento
top_10_municipios = promedio_crecimiento.nlargest(10).index

# Filtrar datos para los top 10 municipios
datos_grafico = municipios_crecimiento[(municipios_crecimiento['MUNICIPIO'].isin(top_10_municipios)) 
                                       & (municipios_crecimiento['ANNO']==2024)]

# Crear el gráfico de líneas
plt.figure(figsize=(15, 8))
for municipio in top_10_municipios:
    data = datos_grafico[datos_grafico['MUNICIPIO'] == municipio]
    plt.plot(data['periodo'], data['tasa_variacion'], marker='o', linewidth=2, label=municipio)

# Personalizar el gráfico
plt.title('Evolución de la Tasa de Variación - Top 10 Municipios con Mayor Crecimiento Promedio', fontsize=14, pad=20)
plt.xlabel('Período', fontsize=12)
plt.ylabel('Tasa de Variación (%)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(title='Municipio', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.xticks(rotation=45, ha='right')

# Ajustar layout
plt.tight_layout()

# Mostrar el gráfico
plt.show()
