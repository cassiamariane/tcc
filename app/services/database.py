from os import environ
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import pymysql

db_url = environ['DB_URL']
db_user = environ['DB_USER']
db_password = environ['DB_PASSWORD']
database_name = environ['DB_NAME']

def read_from_database(query, params=None):
    connection_string = f'mysql+pymysql://{db_user}:{db_password}@{db_url}:3306/{database_name}'
    engine = create_engine(connection_string)
    
    try:
        with engine.connect() as connection:
            result = pd.read_sql(query, con=connection, params=params)
    except Exception as e:
        print(f"Erro ao consultar dados: {e}")
        result = pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro
    finally:
        engine.dispose()

    return result

def write_to_database(table, df, exists):
    connection_string = f'mysql+pymysql://{db_user}:{db_password}@{db_url}:3306/{database_name}'
    engine = create_engine(connection_string)
    
    try:
        with engine.connect() as connection:
            df.to_sql(name=table, con=connection, if_exists=exists, index=False)
            print(f"Dados de {table} inseridos no banco")
    except Exception as e:
        print(f"Erro ao inserir dados de {table} dados: {e}")
    finally:
        engine.dispose()