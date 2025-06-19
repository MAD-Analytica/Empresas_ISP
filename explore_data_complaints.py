import pandas as pd
import os

filepath='data_ISPs/FT4_2_INT_FIJO_13.csv'

try:
    df = pd.read_csv(filepath, sep=';')
except Exception as e:
    print(f"Error en la línea: {str(e)}")

print(f"Primeras 5 filas: {df.head()}")
print(f"Información del DataFrame: {df.info()}")
print(f"Descripción estadística: {df.describe()}")
print(f"Suma de valores nulos por columna: {df.isnull().sum()}")
print(f"Porcentaje de valores nulos por columna: {df.isnull().sum()/len(df)}")
print(f"Porcentaje de valores nulos por columna: {df.isnull().sum()/len(df)}")

print(df['ID_EMPRESA'].value_counts())

mask_ultimo_trim = (df['ANNO']==2024) & (df['TRIMESTRE']==4)


df.loc[mask_ultimo_trim,'NUMERO_QUEJAS'].sum()

import matplotlib.pyplot as plt

# Filtrar solo para el año 2024
df_2024 = df[df['ANNO'] == 2024]
df_2024['NUMERO_QUEJAS'].sum()

# Agrupar por mes y sumar el número de quejas
quejas_por_mes = df_2024.groupby('TRIMESTRE')['NUMERO_QUEJAS'].sum().reset_index()

# Ordenar por mes si no está ordenado
quejas_por_mes = quejas_por_mes.sort_values('TRIMESTRE')

# Graficar
plt.figure(figsize=(8,5))
plt.plot(quejas_por_mes['TRIMESTRE'], quejas_por_mes['NUMERO_QUEJAS'], marker='o')
plt.xlabel('Trimestre')
plt.ylabel('Total de quejas')
plt.title('Total de PQRs en ISPs por trimestre en 2024')
plt.grid(True)
plt.xticks(quejas_por_mes['TRIMESTRE'])
plt.tight_layout()
plt.show()




df.groupby(['ID_EMPRESA']).agg(num_quejas=('NUMERO_QUEJAS',sum)).sort_values(by='num_quejas',ascending=False)

df.duplicated(
    subset=['ANNO','ID_EMPRESA','TRIMESTRE','MES_DEL_TRIMESTRE',
            'ID_SERVICIO','ID_TIPOLOGIA','ID_MEDIO_ATENCION']).sum()

df.groupby(['ANNO','ID_EMPRESA','TRIMESTRE','MES_DEL_TRIMESTRE']).agg(
    num_quejas=('NUMERO_QUEJAS',sum)
).reset_index()

df['TIPOLOGIA'].value_counts()