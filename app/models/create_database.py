from collections import defaultdict
import glob
import os
import pandas as pd
import mysql.connector
import pymysql
from sqlalchemy import create_engine
import json
from services import utils
    
def process_and_write_cnae_secundaria(df_estabelecimentos, chunk_size=10000):

    rows = []
    for _, row in df_estabelecimentos.iterrows():
        cnpj_basico = row['cnpj_basico']
        cnpj_ordem = row['cnpj_ordem']
        cnaes_secundaria = row['codigo']
        
        if pd.notna(cnaes_secundaria):
            
            for cnae in cnaes_secundaria.split(','):
                rows.append({
                    'cnpj_basico': cnpj_basico,
                    'cnpj_ordem': cnpj_ordem,
                    'codigo': cnae.strip()
                })
    
    df_cnae_secundaria = pd.DataFrame(rows)

    utils.write_to_database('cnae_fiscal_secundaria', df_cnae_secundaria, 'append',chunk_size)
    
    
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

def related_inserts(cnpjs_filtrados):
    file_paths = [f'Dados2025/CNPJ/Empresas/empresas_{i}.csv' for i in range(10)]
    combined_df = []

    for file_path in file_paths:
        df = pd.read_csv(file_path, delimiter=';', encoding='latin1', header=None, names=colunas_empresas, dtype=str)
        df = df.drop(columns=['qualificacao'], errors='ignore')
        df = df.dropna(subset=['cnpj_basico', 'razao_social'])
        df['cnpj_basico'] = df['cnpj_basico'].astype(str).str.zfill(8)
        cnpjs_filtrados = [str(c).zfill(8) for c in cnpjs_filtrados]
        df = df[df['cnpj_basico'].isin(cnpjs_filtrados)]
        combined_df.append(df)

    final_df = pd.concat(combined_df, ignore_index=True).drop_duplicates()
    utils.write_to_database('empresas', final_df, 'append')

def sampling_process():
    import json
    import glob
    from collections import defaultdict
    
    with open('setores.json', 'r', encoding='utf-8') as f:
        SECTOR_MAP = json.load(f)

    CNAE_TO_SECTOR = {
        str(cnae).zfill(7): sector
        for sector, cnaes in SECTOR_MAP.items()
        for cnae in cnaes
    }

    files = glob.glob('Dados2025/CNPJ/Estabelecimentos/*.csv')

    sampling_by_sector = defaultdict(list)
    cnpjs_list = set()
    cnaes_secundaria = []

    MAX_ROWS = 1000
    SAMPLING_BY_FILE = 200

    for arq in files:
        for chunk in pd.read_csv(arq, delimiter=';', encoding='latin1', header=None,
                                  names=colunas_estabelecimentos, dtype=estabelecimentos_dtypes,
                                  chunksize=100_000):
            chunk['cnae_fiscal_principal'] = chunk['cnae_fiscal_principal'].astype(str).str.zfill(7)
            chunk['sector'] = chunk['cnae_fiscal_principal'].map(CNAE_TO_SECTOR)
            chunk = chunk[chunk['sector'].notna()]

            missing_sectors = {
                sector: MAX_ROWS - len(sampling_by_sector[sector])
                for sector in chunk['sector'].unique()
                if len(sampling_by_sector[sector]) < MAX_ROWS
            }

            for sector, missing in missing_sectors.items():
                subset = chunk[chunk['sector'] == sector]
                if not subset.empty:
                    n = min(missing, SAMPLING_BY_FILE, len(subset))
                    sample = subset.sample(n=n, random_state=42)

                    cnpjs_list.update(sample['cnpj_basico'].unique())

                    if 'cnae_fiscal_secundaria' in sample.columns:
                        cnaes = sample[['cnpj_basico', 'cnpj_ordem', 'cnae_fiscal_secundaria']].dropna()
                        cnaes_secundaria.append(cnaes)

                    sample = sample.drop(columns=['cnae_fiscal_secundaria'], errors='ignore')
                    sampling_by_sector[sector].append(sample)

    final_df = pd.concat([pd.concat(samples) for samples in sampling_by_sector.values()], ignore_index=True)

    for col in ['data_situacao_cadastral', 'data_inicio_atividade', 'data_situacao_especial']:
        final_df[col] = pd.to_datetime(final_df[col], format='%Y%m%d', errors='coerce')

    final_df = final_df.drop(columns=['sector', 'ddd_fax', 'fax'], errors='ignore')
    final_df['cnae_fiscal_principal'] = pd.to_numeric(final_df['cnae_fiscal_principal'], errors='coerce').astype('Int64')
    final_df['ddd_1'] = pd.to_numeric(final_df['ddd_1'], errors='coerce').astype('Int64')
    final_df['telefone_1'] = pd.to_numeric(final_df['telefone_1'], errors='coerce').astype('Int64')
    final_df['ddd_2'] = pd.to_numeric(final_df['ddd_2'], errors='coerce').astype('Int64')
    final_df['telefone_2'] = pd.to_numeric(final_df['telefone_2'], errors='coerce').astype('Int64')
    final_df['pais'] = pd.to_numeric(final_df['pais'], errors='coerce').astype('Int64')
    countrys = utils.read_from_database("SELECT codigo FROM paises")['codigo'].astype('Int64').tolist()
    valid_cnaes = utils.read_from_database("SELECT codigo FROM cnaes")['codigo'].astype('Int64').tolist()
    citys = utils.read_from_database("SELECT codigo FROM municipios")['codigo'].astype('Int64').tolist()

    final_df = final_df[final_df['pais'].isin(countrys)]
    final_df = final_df[final_df['cnae_fiscal_principal'].isin(valid_cnaes)]
    final_df = final_df[final_df['municipio'].isin(citys)]
    
    related_inserts(list(cnpjs_list))

    utils.write_to_database('estabelecimentos', final_df.drop_duplicates(), 'append')

    if cnaes_secundaria:
        cnae_secundaria_df = pd.concat(cnaes_secundaria, ignore_index=True).drop_duplicates()

        valid_cnpjs = final_df[['cnpj_basico', 'cnpj_ordem']].drop_duplicates()
        cnae_secundaria_df = cnae_secundaria_df.merge(
            valid_cnpjs,
            on=['cnpj_basico', 'cnpj_ordem'],
            how='inner'
        )

        cnae_secundaria_df = cnae_secundaria_df.rename(columns={'cnae_fiscal_secundaria': 'codigo'})

        process_and_write_cnae_secundaria(cnae_secundaria_df)

    print("Processamento de estabelecimentos concluído com sucesso!")


def get_dataset(type):
    
    default_path = os.path.join('Dados2025', 'CNPJ')
    clean = []
    combined_df = []
    query = None
    date_columns = None
    if type == 'estabelecimentos':  
        sampling_process()
        return
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
            df_cnpjs = utils.read_from_database(query)
            lista_cnpjs = df_cnpjs['cnpj_basico'].tolist()

            df = df[df['cnpj_basico'].isin(lista_cnpjs)]
            
        combined_df.append(df)
        
    final_df = pd.concat(combined_df, ignore_index=True)
    duplicates = final_df.duplicated().any()
    
    if(duplicates):
        final_df = final_df.drop_duplicates()
    
    if date_columns is not None:
        for col in date_columns:
            final_df[col] = pd.to_datetime(final_df[col], format='%Y%m%d', errors='coerce')

    print(final_df.head())
    print(final_df.shape)
    utils.write_to_database(type, final_df, 'append')
    
types = ['cnaes', 'naturezas_juridicas', 'municipios', 'paises','estabelecimentos']
    
get_dataset(types[4])