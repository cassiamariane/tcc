from collections import defaultdict
import glob
import os
import pandas as pd
import mysql.connector
import pymysql
from sqlalchemy import create_engine
import json
from app.services import database
base_dir = os.path.dirname(os.path.abspath(__file__))
    
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

    database.write_to_database('cnae_fiscal_secundaria', df_cnae_secundaria, 'append',chunk_size)
    
    
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
    empresas_dir = os.path.join(base_dir, 'Dados2025', 'CNPJ', 'Empresas')
    file_paths = glob.glob(os.path.join(empresas_dir, 'empresas_*.csv'))
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
    database.write_to_database('empresas', final_df, 'append')

def sampling_process():
    sector_path = os.path.join(base_dir, 'setores.json')
    with open(sector_path, 'r', encoding='utf-8') as f:
        sector_map = json.load(f)

    cnae_to_sector = {
        str(cnae).zfill(7): sector
        for sector, cnaes in sector_map.items()
        for cnae in cnaes
    }

    estab_path = os.path.join(base_dir, 'Dados2025', 'CNPJ', 'Estabelecimentos')
    files = glob.glob(os.path.join(estab_path, 'estabelecimentos_*.csv'))

    valid_cnaes = set(database.read_from_database("SELECT codigo FROM cnaes")['codigo'].astype('Int64'))
    valid_countries = set(database.read_from_database("SELECT codigo FROM paises")['codigo'].astype('Int64'))
    valid_cities = set(database.read_from_database("SELECT codigo FROM municipios")['codigo'].astype('Int64'))

    brazil_ufs = {
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO',
        'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI',
        'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
    }

    group_counts = defaultdict(int)
    total_records = 0
    samples_by_group = defaultdict(list)
    sample_counts = defaultdict(int)
    cnpjs = set()
    secondary_cnaes = []

    max_total_sample = 80000
    chunk_sample_limit = 200

    for file in files:
        for chunk in pd.read_csv(file, delimiter=';', encoding='latin1', header=None,
                                 names=colunas_estabelecimentos, dtype=estabelecimentos_dtypes,
                                 chunksize=100_000):
            chunk['cnae_fiscal_principal'] = chunk['cnae_fiscal_principal'].astype(str).str.zfill(7)
            chunk['sector'] = chunk['cnae_fiscal_principal'].map(cnae_to_sector)
            chunk['year'] = pd.to_datetime(chunk['data_inicio_atividade'], format='%Y%m%d', errors='coerce').dt.year
            chunk['uf'] = chunk['uf'].astype(str).str.strip()
            chunk['municipio'] = pd.to_numeric(chunk['municipio'], errors='coerce').astype('Int64')
            chunk['pais'] = None
            chunk.loc[chunk['uf'].isin(brazil_ufs), 'pais'] = 105
            chunk['pais'] = pd.to_numeric(chunk['pais'], errors='coerce').astype('Int64')
            chunk['cnae_fiscal_principal'] = pd.to_numeric(chunk['cnae_fiscal_principal'], errors='coerce').astype('Int64')

            chunk = chunk[
                chunk['sector'].notna() &
                chunk['year'].notna() &
                chunk['uf'].notna() &
                (chunk['year'] >= 1970) &
                chunk['pais'].isin(valid_countries) &
                chunk['municipio'].isin(valid_cities) &
                chunk['cnae_fiscal_principal'].isin(valid_cnaes)
            ]

            group_sizes = chunk.groupby(['uf', 'sector', 'year']).size()
            for key, count in group_sizes.items():
                group_counts[key] += count
                total_records += count

    if total_records == 0:
        print("Nenhum registro válido foi encontrado.")
        return

    proportions = {k: v / total_records for k, v in group_counts.items()}

    for file in files:
        for chunk in pd.read_csv(file, delimiter=';', encoding='latin1', header=None,
                                 names=colunas_estabelecimentos, dtype=estabelecimentos_dtypes,
                                 chunksize=100_000):
            chunk['cnae_fiscal_principal'] = chunk['cnae_fiscal_principal'].astype(str).str.zfill(7)
            chunk['sector'] = chunk['cnae_fiscal_principal'].map(cnae_to_sector)
            chunk['year'] = pd.to_datetime(chunk['data_inicio_atividade'], format='%Y%m%d', errors='coerce').dt.year
            chunk['uf'] = chunk['uf'].astype(str).str.strip()
            chunk['municipio'] = pd.to_numeric(chunk['municipio'], errors='coerce').astype('Int64')
            chunk['pais'] = None
            chunk.loc[chunk['uf'].isin(brazil_ufs), 'pais'] = 105
            chunk['pais'] = pd.to_numeric(chunk['pais'], errors='coerce').astype('Int64')
            chunk['cnae_fiscal_principal'] = pd.to_numeric(chunk['cnae_fiscal_principal'], errors='coerce').astype('Int64')

            chunk = chunk[
                chunk['sector'].notna() &
                chunk['year'].notna() &
                chunk['uf'].notna() &
                (chunk['year'] >= 1970) &
                chunk['pais'].isin(valid_countries) &
                chunk['municipio'].isin(valid_cities) &
                chunk['cnae_fiscal_principal'].isin(valid_cnaes)
            ]

            for (uf, sector, year), proportion in proportions.items():
                if sample_counts[(uf, sector, year)] >= int(max_total_sample * proportion):
                    continue

                subset = chunk[(chunk['uf'] == uf) & (chunk['sector'] == sector) & (chunk['year'] == year)]
                if subset.empty:
                    continue

                remaining = int(max_total_sample * proportion) - sample_counts[(uf, sector, year)]
                n = min(chunk_sample_limit, remaining, len(subset))
                if n > 0:
                    sample = subset.sample(n=n, random_state=42)
                    cnpjs.update(sample['cnpj_basico'].unique())

                    if 'cnae_fiscal_secundaria' in sample.columns:
                        cnaes = sample[['cnpj_basico', 'cnpj_ordem', 'cnae_fiscal_secundaria']].dropna()
                        secondary_cnaes.append(cnaes)

                    sample = sample.drop(columns=['cnae_fiscal_secundaria'], errors='ignore')
                    samples_by_group[(uf, sector, year)].append(sample)
                    sample_counts[(uf, sector, year)] += n

    final_df = pd.concat([pd.concat(samples) for samples in samples_by_group.values()], ignore_index=True)

    for col in ['data_situacao_cadastral', 'data_inicio_atividade', 'data_situacao_especial']:
        final_df[col] = pd.to_datetime(final_df[col], format='%Y%m%d', errors='coerce')

    final_df = final_df.drop(columns=['sector', 'year', 'ddd_fax', 'fax'], errors='ignore')

    final_df['ddd_1'] = pd.to_numeric(final_df['ddd_1'], errors='coerce').astype('Int64')
    final_df['telefone_1'] = pd.to_numeric(final_df['telefone_1'], errors='coerce').astype('Int64')
    final_df['ddd_2'] = pd.to_numeric(final_df['ddd_2'], errors='coerce').astype('Int64')
    final_df['telefone_2'] = pd.to_numeric(final_df['telefone_2'], errors='coerce').astype('Int64')

    related_inserts(list(cnpjs))
    database.write_to_database('estabelecimentos', final_df.drop_duplicates(), 'append')

    if secondary_cnaes:
        cnae_df = pd.concat(secondary_cnaes, ignore_index=True).drop_duplicates()
        valid_cnpjs = final_df[['cnpj_basico', 'cnpj_ordem']].drop_duplicates()
        cnae_df = cnae_df.merge(valid_cnpjs, on=['cnpj_basico', 'cnpj_ordem'], how='inner')
        cnae_df = cnae_df.rename(columns={'cnae_fiscal_secundaria': 'codigo'})
        process_and_write_cnae_secundaria(cnae_df)

    print("Processamento de estabelecimentos concluído com sucesso!")


def get_dataset(type):
    default_path = os.path.join(base_dir, 'Dados2025', 'CNPJ')
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
            df_cnpjs = database.read_from_database(query)
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
    database.write_to_database(type, final_df, 'append')
    
types = ['cnaes', 'naturezas_juridicas', 'municipios', 'paises','estabelecimentos']
    
get_dataset(types[4])