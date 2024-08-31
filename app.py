import numpy as np
import streamlit as st
import re
import yfinance as yf
import pandas as pd
import requests
from lxml import html, etree
import datetime as dt

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
            percent_change = tickers[ticker]
            last_price = last_prices[ticker]
            sector = original_row['Setor'].values[0]
            info_list.append([ticker_symbol, name, percent_change, last_price, sector])
        except IndexError:
            print(f"Failed to extract info for ticker: {ticker}")
    return pd.DataFrame(info_list, columns=['Ticker', 'Nome', 'Variação %', 'Valor', 'Setor'])

def plot_sector_bar_chart(df):
    grouped = df.groupby(['Setor']).size()
    st.bar_chart(grouped, height=500, use_container_width=True)

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

        data = yf.download(tickers, start=data_inicial, end=data_final, interval='1d')['Adj Close']

        stock_changes = get_stock_changes(data)

        last_prices = data.iloc[-1].dropna()
        
        final_df = structure_df(stock_changes.dropna(), df, last_prices)
        
        # Criar a barra lateral para seleção do setor
        options = final_df['Setor'].drop_duplicates()
        select = st.sidebar.selectbox('Setor:', options)

        filtered_df = final_df.loc[final_df["Setor"] == select]

        col_1, col_2 = st.columns([1, 1])

        with col_1:
            st.write(filtered_df, height=400)
            
        with col_2:
            # Ordenar os 50 maiores ganhadores por variação %
            top_gainers = final_df.sort_values(by='Variação %', ascending=False).head(50)

            # Plotar gráfico de setores para top gainers
            st.subheader('Top Gainers - Tipos de Setores')
            plot_sector_bar_chart(top_gainers)

            # Ordenar os 50 maiores valores
            most_valued = final_df.sort_values(by='Valor', ascending=False).head(50)

            # Plotar gráfico de setores para ativos mais valorizados
            st.subheader('Ativos Mais Valorizados - Tipos de Setores')
            plot_sector_bar_chart(most_valued)
else:
    st.write("Não foi possível carregar os dados.")
with tab_4:
    # Exemplo de coordenadas do Brasil
    df = pd.DataFrame({
        "latitude": np.random.randn(1000) / 50 + -15.8,
        "longitude": np.random.randn(1000) / 50 + -47.9,
        "size": np.random.randn(1000) * 100,
    })

    st.map(df, 
       latitude='latitude', 
       longitude='longitude', 
       size='size')
