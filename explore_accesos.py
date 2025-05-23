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

# Filtro el último trimestre de 2024 y empresas con más de 100 usuarios
mask_ultimo_trim = (df_estudio['ANNO']==2024) & (df_estudio['TRIMESTRE']==4)
mask_usuarios = df_estudio['num_accesos'] >= 1000
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

import matplotlib.pyplot as plt
import seaborn as sns
# Configurar el estilo
plt.style.use('seaborn')
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
