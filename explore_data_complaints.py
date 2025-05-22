import pandas as pd
import os

folder='/Users/juliandiazparra/Desktop/Empresas_ISP'
data=os.path.join(folder,'data')
filepath=os.path.join(data,'FT4_2_INT_FIJO_13.csv')

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

df.groupby(['ID_EMPRESA']).agg(num_quejas=('NUMERO_QUEJAS',sum)).sort_values(by='num_quejas',ascending=False)

df.duplicated(
    subset=['ANNO','ID_EMPRESA','TRIMESTRE','MES_DEL_TRIMESTRE',
            'ID_SERVICIO','ID_TIPOLOGIA','ID_MEDIO_ATENCION']).sum()

df.groupby(['ANNO','ID_EMPRESA','TRIMESTRE','MES_DEL_TRIMESTRE']).agg(
    num_quejas=('NUMERO_QUEJAS',sum)
).reset_index()

df['TIPOLOGIA'].value_counts()