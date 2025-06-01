import os
import pandas as pd
import mysql.connector
import pymysql
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split

db_url = 'localhost'
db_user = 'root'
db_password = '123456'
database_name = 'cnpj'

def split_dataframe(df, chunk_size):
    for start in range(0, len(df), chunk_size):
        yield df[start:start + chunk_size]

def write_to_database(df, table_name, chunk_size=10000):
    connection_string = f'mysql+pymysql://{db_user}:{db_password}@{db_url}:3306/{database_name}'
    engine = create_engine(connection_string)

    with engine.connect() as connection:
        with connection.begin() as transaction:
            try:
                for chunk in split_dataframe(df, chunk_size):
                    chunk.to_sql(name=table_name, con=connection, if_exists='append', index=False)
                transaction.commit()
            except Exception as e:
                transaction.rollback()
                print(f"Erro ao carregar dados na tabela {table_name}: {e}")
            finally:
                pass

    engine.dispose()
    
def process_and_write_cnae_secundaria(df_estabelecimentos, chunk_size=10000):
    # Step 1: Extract CNAE Fiscal Secundaria
    rows = []
    for _, row in df_estabelecimentos.iterrows():
        cnpj_basico = row['cnpj_basico']
        cnpj_ordem = row['cnpj_ordem']
        cnaes_secundaria = row['cnae_fiscal_secundaria']
        
        if pd.notna(cnaes_secundaria):  # Ensure it's not NaN
            # Split by commas
            for cnae in cnaes_secundaria.split(','):
                rows.append({
                    'cnpj_basico': cnpj_basico,
                    'cnpj_ordem': cnpj_ordem,
                    'codigo': cnae.strip()
                })
    
    # Step 2: Create DataFrame for CNAE Secundaria
    df_cnae_secundaria = pd.DataFrame(rows)

    # Step 3: Insert CNAE Secundaria Data into the Database
    write_to_database(df_cnae_secundaria, 'cnae_fiscal_secundaria', chunk_size)
    
def read_from_database(query):
    connection_string = f'mysql+pymysql://{db_user}:{db_password}@{db_url}:3306/{database_name}'
    engine = create_engine(connection_string)
    
    try:
        with engine.connect() as connection:
            result = pd.read_sql(query, con=connection)
    except Exception as e:
        print(f"Erro ao consultar dados: {e}")
        result = pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro
    finally:
        engine.dispose()

    return result
    
colunas_empresas=['cnpj_basico',
                  'razao_social',
                  'natureza_juridica',
                  'qualificacao',
                  'capital_social',
                  'porte',
                  'ente_federativo_responsavel']

colunas_estabelecimentos=['cnpj_basico', 
                          'cnpj_ordem', 
                          'cnpj_dv', 
                          'identificador_matriz_filial', 
                          'nome_fantasia', 
                          'situacao_cadastral', 
                          'data_situacao_cadastral', 
                          'motivo_situacao_cadastral', 
                          'nome_cidade_exterior', 
                          'pais', 
                          'data_inicio_atividade', 
                          'cnae_fiscal_principal', 
                          'cnae_fiscal_secundaria', 
                          'tipo_de_logradouro', 
                          'logradouro', 
                          'numero', 
                          'complemento', 
                          'bairro', 
                          'cep', 
                          'uf', 
                          'municipio', 
                          'ddd_1', 
                          'telefone_1', 
                          'ddd_2', 
                          'telefone_2',
                          'ddd_fax',
                          'fax',
                          'correio_eletronico', 
                          'situacao_especial', 
                          'data_situacao_especial']

estabelecimentos_dtypes = { 4: str,
                            8: str,
                           11: str,
                           12: str,
                           15: str,
                           18: str,
                           21: str,
                           22: str,
                           23: str,
                           24: str,
                           25: str,
                           26: str,
                           27: str,
                           28: str
                        }

colunas_padrao=['codigo',
               'descricao']

def get_dataset(type):
    
    default_path = os.path.join('Dados', 'CNPJ')
    clean = []
    combined_df = []
    stratify = None
    query = None
    cnae_secundaria_df = None
    date_columns = None
    if type == 'empresas' or type == 'estabelecimentos':  
        file_paths = [os.path.join(default_path, type.capitalize(), f"{type}_{i}.csv") for i in range(10)]
        
        if type == 'empresas':
            names=colunas_empresas
            dtypes = None
            clean = ['qualificacao']
            columns_to_check = ['cnpj_basico', 'razao_social']
            stratify = 'porte'
            
        elif type == 'estabelecimentos':
            names=colunas_estabelecimentos
            dtypes = estabelecimentos_dtypes
            clean = ['ddd_fax','fax']
            columns_to_check = ['cnpj_basico', 'cnpj_ordem']
            date_columns = ['data_situacao_cadastral', 'data_inicio_atividade', 'data_situacao_especial']
            query = 'SELECT cnpj_basico FROM cnpj.empresas;'
                
    else:
        names = colunas_padrao
        dtypes = None
        file_paths = [os.path.join(default_path, type.capitalize(), f"{type}.csv")]
        columns_to_check = ['codigo']

    for file_path in file_paths:
        df = pd.read_csv(file_path, delimiter=';', encoding='latin1', header=None, names=names, dtype=dtypes)
            
        if clean:
            df = df.drop(columns=clean)
            
        df = df.dropna(subset=columns_to_check, how='any')
        
        if not query is None:
            df_cnpjs = read_from_database(query)
            lista_cnpjs = df_cnpjs['cnpj_basico'].tolist()

            df = df[df['cnpj_basico'].isin(lista_cnpjs)]
            
        combined_df.append(df)
        
    # Faz o merge de todos os dataframes
    final_df = pd.concat(combined_df, ignore_index=True)
    duplicates = final_df.duplicated().any()
    
    if(duplicates):
        final_df = final_df.drop_duplicates()
        
    if 'cnae_fiscal_secundaria' in final_df.columns:
        cnae_secundaria_df = final_df[['cnpj_basico', 'cnpj_ordem', 'cnae_fiscal_secundaria']]
        final_df = final_df.drop(columns=['cnae_fiscal_secundaria'])
        
    if not stratify is None:
        print(final_df['porte'].value_counts())
        # Separando as features (todas as colunas exceto a 4) e o target (coluna 4)
        str_df = final_df.dropna(subset=['porte'])
        x = str_df.drop(columns=['porte'])
        y = str_df['porte']
        
        # Realizando a divisão de dados com stratify pela coluna 4 (porte), reservando 90% para teste e 10% para treino(desenvolvimento)
        X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.99999, stratify=y)
        print(y_train.value_counts())
        
        #Obtendo o df de treino
        final_df = pd.concat([X_train, y_train], axis=1)

    if date_columns is not None:
        for col in date_columns:
            final_df[col] = pd.to_datetime(final_df[col], format='%Y%m%d', errors='coerce')

        
    print(final_df.head())
    print(final_df.shape)
    write_to_database(final_df, type)
    
    if not cnae_secundaria_df is None:
        process_and_write_cnae_secundaria(cnae_secundaria_df)
    
types = ['cnaes', 'naturezas_juridicas', 'municipios', 'paises','empresas', 'estabelecimentos']
    
get_dataset(types[5])