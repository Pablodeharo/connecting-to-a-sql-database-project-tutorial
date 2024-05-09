import os
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

# load the .env file variables
load_dotenv()

# 1) Connect to the database here using the SQLAlchemy's create_engine function

# 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function

# 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function

# 4) Use pandas to print one of the tables as dataframes using read_sql function
# Función para conectarse a la base de datos
import psycopg2
import pandas as pd

def connect_to_database():
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        user='gitpod',
        password='postgres',
        database='db_name'
    )
    return conn

# Función para crear tablas e insertar datos
def setup_database():
    conn = connect_to_database()
    cursor = conn.cursor()

    # Leer y ejecutar los scripts SQL
    sql_files = ['create.sql', 'insert.sql']
    for file_name in sql_files:
        with open(f'./src/sql/{file_name}', 'r') as sql_file:
            sql_script = sql_file.read()
            cursor.execute(sql_script)

    # Guardar los cambios y cerrar la conexión
    conn.commit()
    cursor.close()
    conn.close()

# Función para imprimir una tabla como DataFrame usando Pandas
def print_table_as_dataframe(table_name):
    conn = connect_to_database()
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    print(df)
    conn.close()

# Llamar a la función para configurar la base de datos
setup_database()

# Imprimir la tabla 'books' como un DataFrame
print_table_as_dataframe('books')