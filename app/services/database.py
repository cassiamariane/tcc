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
        result = pd.DataFrame()
    finally:
        engine.dispose()

    return result

def split_dataframe(df, chunk_size):
    for start in range(0, len(df), chunk_size):
        yield df[start:start + chunk_size]

def write_to_database(table, df, exists, chunk_size=10000):
    connection_string = f'mysql+pymysql://{db_user}:{db_password}@{db_url}:3306/{database_name}'
    engine = create_engine(connection_string)

    with engine.connect() as connection:
        with connection.begin() as transaction:
            try:
                for chunk in split_dataframe(df, chunk_size):
                    chunk.to_sql(name=table, con=connection, if_exists=exists, index=False)
                transaction.commit()
            except Exception as e:
                transaction.rollback()
                print(f"Erro ao carregar dados na tabela {table}: {e}")
            finally:
                pass

    engine.dispose()