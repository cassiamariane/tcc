import os
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
import dask.dataframe as dd
from dask.distributed import Client

db_url = os.environ.get('DB_URL')
db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')
database_name = os.environ.get('DB_NAME')

def split_dataframe(df, chunk_size):
    for start in range(0, len(df), chunk_size):
        yield df[start:start + chunk_size]

def write_to_database(df, table_name, chunk_size=10000):
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
    
def read_from_database(query):
    connection_string = f'mysql+mysqlconnector://{db_user}:{db_password}@{db_url}:3306/{database_name}'
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
                           24: str,
                           25: str,
                           26: str,
                           27: str,
                           28: str
                        }

colunas_padrao=['codigo',
               'descricao']

def get_dataset(type):
    
    default_path = '..\\Dados\\CNPJ\\'
    clean = []
    combined_df = []
    
    if type == 'empresas' or type == 'estabelecimentos':  
        file_paths = [default_path + type.capitalize() + '\\' + type + '_' + str(i) + '.csv' for i in range(10)]
        
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
            query = 'SELECT cnpj_basico FROM cnpj.empresas;'
                
    else:
        names = colunas_padrao
        dtypes = None
        file_paths = [default_path + type.capitalize() + '\\' + type + '.csv']
        columns_to_check = ['codigo']
    

    if __name__ == '__main__':
        # Inicializa o cliente Dask
        client = Client()
        
        # Imprime o link do dashboard
        print("Dashboard Dask:", client.dashboard_link)
        
        if query:
            df = read_from_database(query)
            lista_cnpjs = df['cnpj_basico'].tolist()
    
        for file_path in file_paths:
            print("Lendo o arquivo:", file_path)
            df = dd.read_csv(file_path, delimiter=';', encoding='latin1', header=None, names=names, dtype=dtypes)
            
            if clean:
                df = df.drop(columns=clean)
            
            df = df.dropna(subset=columns_to_check, how='any')
            
            df = df[df['cnpj_basico'].isin(lista_cnpjs)]
            
            combined_df.append(df)

        # Concatena todos os DataFrames Dask
        combined_ddf = dd.concat(combined_df, ignore_index=True)

        print("Removendo duplicatas...")
        cleaned_ddf = combined_ddf.drop_duplicates()

        print("Iniciando processamento em chunks...")
        chunk_size = 10_000  # Ajuste o tamanho do bloco conforme necessário
        chunks = cleaned_ddf.to_delayed()

        # Lista para armazenar resultados processados
        results = []

        for i, chunk in enumerate(chunks):
            print(f"Processando chunk {i+1}/{len(chunks)}")
            result = chunk.compute()
            results.append(result)

        # Combina os resultados finais
        final_df = pd.concat(results, ignore_index=True)
        
        print("Salvando como CSV")
        
        final_df.to_csv('/final_output.csv', index=False, sep=';', encoding='latin1')

        print("Arquivo CSV salvo com sucesso!")
            
        #if stratify:
        #    print(final_df['porte'].value_counts())
            # Separando as features (todas as colunas exceto a 4) e o target (coluna 4)
        #    str_df = final_df.dropna(subset=['porte'])
        #    x = str_df.drop(columns=['porte'])
        #    y = str_df['porte']

            
            # Realizando a divisão de dados com stratify pela coluna 4 (porte), reservando 90% para teste e 10% para treino(desenvolvimento)
        #    X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.99999, stratify=y)
        #    print(y_train.value_counts())
            #Obtendo o df de treino
        #    final_df = pd.concat([X_train, y_train], axis=1)
            
        print(final_df.head())
        print(final_df.shape)
        write_to_database(final_df, type)
    
types = ['cnaes', 'naturezas_juridicas', 'municipios', 'paises','empresas', 'estabelecimentos']
    
get_dataset(types[5])