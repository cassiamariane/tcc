import os
import numpy as np
import streamlit as st
import re
import yfinance as yf
import pandas as pd
import requests
from lxml import html, etree
import datetime as dt
import mysql.connector
from sqlalchemy import create_engine
import pydeck as pdk
import plotly.express as px
import json
import pymysql

db_url = 'localhost'
db_user = 'root'
db_password = '123456'
database_name = 'cnpj'

@st.cache_data
def get_most_active_tickers():
    url = 'https://br.tradingview.com/markets/stocks-brazil/market-movers-active/'

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.google.com",
        "Connection": "keep-alive"
    }
    page = requests.get(url, headers=headers)
    if page.status_code == 200:
        print("Página carregada com sucesso!")
        # Extrai tabela do HTML
        tree = html.fromstring(page.content)
        table = tree.xpath('//table[@class="table-Ngq2xrcG"]')[0]
        return pd.read_html(etree.tostring(table))[0]
    else:
        print(f"Erro ao carregar a página: {page.status_code}")
        return None

# Função para limpar a coluna de simbolos e obter os tickers (até o 3) brasileiros (adicionando .SA ao final)
def clean_symbols(df):
    lista_simbolos = df["Símbolo"].to_list()
    tickers = [re.search(r'\w+3', simbolo).group() for simbolo in lista_simbolos]
    return [ticker + ".SA" for ticker in tickers]

def get_stock_changes(data):
    return (data.iloc[0] - data.iloc[0].shift(1)) / data.iloc[0].shift(1) * 100

def structure_df(tickers, df, last_prices):
    info_list = []
    for ticker in tickers.index:
        try:
            ticker_symbol = ticker.replace('.SA', '')
            original_row = df[df['Símbolo'].str.contains(ticker_symbol)]
            name = original_row['Símbolo'].values[0].split(ticker_symbol)[-1].strip()
            vol = df['Volume'].loc[df['Símbolo'].str.contains(ticker_symbol)].values[0]
            price = df['Preço'].loc[df['Símbolo'].str.contains(ticker_symbol)].values[0]
            activity = df['Price * Vol'].loc[df['Símbolo'].str.contains(ticker_symbol)].values[0]
            percent_change = tickers[ticker]
            last_price = last_prices[ticker]
            sector = original_row['Setor'].values[0]
            info_list.append([ticker_symbol, name, vol, price, activity, percent_change, last_price, sector])
        except IndexError:
            print(f"Failed to extract info for ticker: {ticker}")
    return pd.DataFrame(info_list, columns=['Ticker', 'Nome', 'Volume', 'Preço BRL', 'Vol * Preço BRL','Variação %', 'Valor', 'Setor'])

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

def get_coordinates(city):
    url = f"https://nominatim.openstreetmap.org/search?city={city}&country=Brazil&format=json"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.google.com",
        "Connection": "keep-alive"
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        try:
            data = response.json()
            if data:
                latitude = data[0]['lat']
                longitude = data[0]['lon']
                return latitude, longitude
        except ValueError:
            print(f"Error decoding JSON for city: {city}")
    else:
        print(f"Error fetching data for city: {city}, Status code: {response.status_code}")
    
    return None, None

def plot_sector_bar_chart(df):
    grouped = df.groupby(['Setor']).size()
    st.bar_chart(grouped, height=500, use_container_width=True)
    
def formatar_valor(valor_str):
    
    if 'BRL' in valor_str:
        valor_str = valor_str.replace(' BRL', '').strip()
    if 'K' in valor_str:
        fator = 1_000
        valor_str = valor_str.replace('K', '').strip()
    if 'M' in valor_str:
        fator = 1_000_000
        valor_str = valor_str.replace('M', '').strip()
    elif 'B' in valor_str:
        fator = 1_000_000_000
        valor_str = valor_str.replace('B', '').strip()
    else:
        fator = 1 
    
    valor_num = float(valor_str.replace('.', '').replace(',', '.')) * fator
    
    return valor_num

st.set_page_config(layout="wide", page_title='Oportunidades de negócios brasileiros')

df = get_most_active_tickers()

if df is not None and not df.empty:
    
    tab_1, tab_2, tab_3, tab_4, tab_5, tab_6 = st.tabs(["Visão Geral", 
                                          "Análise Histórica", 
                                          "Análise de Reputação", 
                                          "Distribuição Geográfica", 
                                          "Tendências", 
                                          "Especialistas"])
    
    with tab_1:
        # Obter tickers
        tickers = clean_symbols(df)
        end_date = dt.datetime.today()
        start_date=dt.datetime(end_date.year-1, end_date.month, end_date.day)
        
        with st.container():
            col_1, col_2 = st.columns(2)
            with col_1:
                data_inicial = st.date_input('Selecione a Data Inicial: ', start_date)
            with col_2:
                data_final = st.date_input('Selecione a Data Final: ', end_date)

        data = yf.download(tickers, start=data_inicial, end=data_final, interval='1d', auto_adjust=False )['Adj Close']

        stock_changes = get_stock_changes(data)

        last_prices = data.iloc[-1].dropna()
        
        final_df = structure_df(stock_changes.dropna(), df, last_prices)
        
        # Criar a barra lateral para seleção do setor
        with open('setores.json', 'r', encoding='utf-8') as file:
            setores = json.load(file)
            
        options = list(setores.keys())
        #options = final_df['Setor'].drop_duplicates()
        select = st.sidebar.selectbox('Setor:', options)
        st.header("Ranking de Empresas Mais Ativas do Setor")
        text = """As empresas são classificadas pelo volume diário de negociação, indicando o quanto estão sendo transacionadas <br> 
                no mercado brasileiro."""

        st.markdown(text, unsafe_allow_html=True)

        filtered_df = final_df.loc[final_df["Setor"] == select]
        
        filtered_df.loc[:, 'Volume'] = filtered_df['Volume'].apply(formatar_valor)
        filtered_df.loc[:, 'Preço BRL'] = filtered_df['Preço BRL'].apply(formatar_valor)
        filtered_df.loc[:, 'Vol * Preço BRL'] = filtered_df['Vol * Preço BRL'].apply(formatar_valor)

        
        filtered_df = filtered_df.sort_values(by='Vol * Preço BRL', ascending=False)
        
        st.dataframe(filtered_df, use_container_width=True, hide_index=True)
        st.header("Comparativo entre os setores")
        st.markdown("""A classificação dos setores brasileiros inclui os top gainers, que são os ativos com a maior variação percentual positiva <br>
                    no preço durante um determinado período. Por outro lado, os setores mais valorizados são aqueles com o maior valor de mercado.""", unsafe_allow_html=True)
        col_1, col_2 = st.columns([1, 1])

        with col_1:
            # Ordenar os 50 maiores ganhadores por variação %
            top_gainers = final_df.groupby('Setor')['Variação %'].sum().reset_index()
            top_gainers = top_gainers.sort_values(by='Variação %', ascending=False).head(50)
            cores_top = ['red' if categoria != select else 'blue' for categoria in top_gainers["Setor"]]
          
            st.subheader('Top Gainers')
            fig = px.bar(top_gainers, 
                        x='Variação %', 
                        y='Setor', 
                        orientation='h')
            fig.update_traces(marker_color=['blue' if categoria == select else 'red' for categoria in top_gainers['Setor']])
            st.plotly_chart(fig, use_container_width=True)

        with col_2:
            # Ordenar os 50 maiores valores
            most_valued = final_df.groupby('Setor')['Valor'].sum().reset_index()
            most_valued = most_valued.sort_values(by='Valor', ascending=False).head(50)
            cores_most = ['red' if categoria != select else 'blue' for categoria in most_valued["Setor"]]
           
            # Plotar gráfico de setores para ativos mais valorizados
            st.subheader('Ativos Mais Valorizados')
            fig = px.bar(most_valued, 
                        x='Valor', 
                        y='Setor', 
                        orientation='h')
            fig.update_traces(marker_color=['blue' if categoria == select else 'red' for categoria in most_valued['Setor']])
            st.plotly_chart(fig, use_container_width=True)
            
    with tab_2:
        codigos = setores.get(select)
        codigos = ', '.join(map(str, codigos))
        query = f'''
                SELECT e.data_inicio_atividade
                FROM cnpj.estabelecimentos e
                JOIN cnpj.cnaes c 
                    ON e.cnae_fiscal_principal = c.codigo
                WHERE c.codigo IN ({codigos});
                '''
        datas = read_from_database(query)
        lista_datas = datas['data_inicio_atividade'].tolist()

        df = pd.DataFrame(lista_datas, columns=['Data'])

        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        df['Ano'] = df['Data'].dt.year
        
        date_counts = df.groupby('Ano').size().reset_index(name='Quantidade')
        
        st.header("Linha do Tempo")
        st.markdown("""A representação indica a quantidade de abertura de empresas no setor ao decorrer dos anos.""", unsafe_allow_html=True)
        
        fig = px.line(date_counts, x='Ano', y='Quantidade', markers=True)
        fig.update_traces(line_color='red', marker_color='red')
        # Definindo o limite inferior do eixo Y como 0
        fig.update_yaxes(range=[0, date_counts['Quantidade'].max() + 1])
        st.plotly_chart(fig)
        
    with tab_4:
        query = f'''
                SELECT m.descricao AS municipio
                FROM cnpj.estabelecimentos e
                JOIN cnpj.cnaes c 
                    ON e.cnae_fiscal_principal = c.codigo
                JOIN cnpj.municipios m
                    ON e.municipio = m.codigo
                WHERE c.codigo IN ({codigos});
                '''
        citys = read_from_database(query)
        lista_citys = citys['municipio'].tolist()
        coordinates = []
        
        for city in lista_citys:
            lat, lon = get_coordinates(city)
            if lat and lon:
                coordinates.append({'city': city, 'latitude': float(lat), 'longitude': float(lon)}) 

        # Criar um DataFrame com as coordenadas
        df = pd.DataFrame(coordinates)
        
        st.header("Mapa de Localizações das Empresas do Setor")
        st.markdown("""O mapa fornece uma visualização da distribuição geográfica das empresas em um setor específico, <br> destacando sua localização em todo o território brasileiro.""", unsafe_allow_html=True)

        if not df.empty:
            st.map(df, latitude='latitude', longitude='longitude')
else:
    st.write("Não foi possível carregar os dados.")