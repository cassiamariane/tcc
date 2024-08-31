import os
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

def split_dataframe(df, chunk_size):
    for start in range(0, len(df), chunk_size):
        yield df[start:start + chunk_size]

def write_to_database(df, table_name, chunk_size=10000):
    db_url = os.environ.get('DB_URL')
    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')
    database_name = os.environ.get('DB_NAME')
    connection_string = f'mysql+mysqlconnector://{db_user}:{db_password}@{db_url}:3306/{database_name}'
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

estabelecimentos_dtypes = {8: str,
                           18: str,
                           21: str,
                           22: str,
                           24: str,
                           26: str,
                           28: str
                        }

colunas_padrao=['codigo',
               'descricao']

def get_dataset(type):
    
    default_path = 'Dados\\CNPJ\\'
    clean = []
    combined_df = []
    
    if type == 'empresas' or type == 'estabelecimentos':  
        file_paths = [default_path + type.capitalize() + '\\' + type + '_' + str(i) + '.csv' for i in range(10)]
        
        if type == 'empresas':
            names=colunas_empresas
            dtypes = None
            clean = ['qualificacao']
            columns_to_check = ['cnpj_basico', 'razao_social']
            
        elif type == 'estabelecimentos':
            names=colunas_estabelecimentos
            dtypes = estabelecimentos_dtypes
            clean = ['ddd_fax','fax']
            columns_to_check = ['cnpj_basico', 'cnpj_ordem']
                
    else:
        names = colunas_padrao
        dtypes = None
        file_paths = [default_path + type.capitalize() + '\\' + type + '.csv']
        columns_to_check = ['codigo']

    for file_path in file_paths:
        df = pd.read_csv(file_path, delimiter=';', encoding='latin1', header=None, names=names, dtype=dtypes)
            
        if clean:
            df = df.drop(columns=clean)
            
        df = df.dropna(subset=columns_to_check, how='any')
            
        combined_df.append(df)
        
        #df.loc[~df['porte'].isin(['00', '01', '03', '05'])] = df['porte'].mode().iloc[0]
        #df.loc[~df['identificador_matriz_filial'].isin(['1', '2'])] = df['identificador_matriz_filial'].mode().iloc[0]
        #df.loc[~df['situacao_cadastral'].isin(['01', '2', '3', '4', '08'])] = df['situacao_cadastral'].mode().iloc[0]
        #print("possiveis fora do permitido")
        #print(df.loc[~df['natureza_juridica'].isin([0,3271, 3280,3298,3301,3999,4014,4090,4120,5010,5029,1015,1023,1031,1040,1058,1066,1074,1082,1104,1112,1120,1139,1147,1155,1163,1171,1180,1198,1210,1228,1236,1244,1252,1260,1279,1287,1295,1309,1317,1325,1333,1341,2011,2038,2046,2054,2062,2070,2089,2097,2100,2127,2135,2143,2151,2160,2178,2194,2216,2224,2232,2240,2259,2267,2275,2283,2291,2305,2313,2321,2330,2348,3034,3069,3077,3085,3107,3115,3131,3204,3212,3220,3239,3247,3255,3263,5037,3328,8885])])
        #print('fim')
        
    # Faz o merge de todos os dataframes
    final_df = pd.concat(combined_df, ignore_index=True)
    duplicates = final_df.duplicated().any()
    
    if(duplicates):
        final_df = final_df.drop_duplicates()
        
    print(final_df.head())
    write_to_database(final_df, type)
    
types = ['cnaes', 'naturezas_juridicas', 'municipios', 'paises','empresas', 'estabelecimentos']
    
get_dataset(types[4])