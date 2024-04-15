import os
import pandas as pd
import mysql.connector
import json
from datetime import datetime

# Conectar a la base de datos Sakila
conn = mysql.connector.connect(
    host="localhost", #ejemplo puedes ponerl el local host 127.0.0.1
    user="root", #el user puede ser el que gustes como daniel o luis o maps
    password="", #la contraseña debe ser la que tiene el usuario 
    database="sakila" #y el nombre de la db que relizaras la copia
)

# Consulta mysq para obtener el esquema de tablas
tables_query = "SHOW TABLES" 
tables_df = pd.read_sql_query(tables_query, conn)

# Crear una carpeta para almacenar los archivos JSON si no existe
if not os.path.exists("sakila"):
    os.makedirs("sakila")

# Iterar sobre cada tabla en la base de datos
for index, row in tables_df.iterrows():
    table_name = row[0]
    # Consulta SQL para seleccionar todos los datos de la tabla actual
    select_query = f"SELECT * FROM {table_name}"
    # Leer datos de la tabla en un DataFrame de pandas
    table_data_df = pd.read_sql_query(select_query, conn)
    # Convertir columnas de tipo Timestamp a formato serializable
    for column in table_data_df.select_dtypes(include=['datetime64']):
        table_data_df[column] = table_data_df[column].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    # Convertir conjuntos a listas
    for column in table_data_df.select_dtypes(include=['object']):
        table_data_df[column] = table_data_df[column].apply(lambda x: list(x) if isinstance(x, set) else x)
    # Convertir DataFrame a formato JSON y guardar en un archivo
    json_file_path = f"sakila/{table_name}.json"
    with open(json_file_path, 'w') as json_file:
        json.dump(table_data_df.to_dict(orient='records'), json_file, indent=4)

# Cerrar conexión a la base de datos
conn.close()
