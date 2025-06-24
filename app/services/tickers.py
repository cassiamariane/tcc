import re
import pandas as pd
import requests
from lxml import html, etree
from services import database, stocks
import datetime as dt
import yfinance as yf

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
        tree = html.fromstring(page.content)
        table = tree.xpath('//table[@class="table-Ngq2xrcG"]')[0]
        return pd.read_html(etree.tostring(table))[0]
    else:
        print(f"Erro ao carregar a página: {page.status_code}")
        return None
    
def clean_symbols(df):
    symbols = df["Símbolo"].to_list()
    tickers = [re.search(r'\w+3', symbol).group() for symbol in symbols]
    return [ticker + ".SA" for ticker in tickers]

def get_tickets(df, tickers):
    query = f'''
                SELECT MAX(insercao)
                FROM tcc.tickers;
            '''
    query_tickers = f'''
                SELECT *
                FROM tcc.tickers
                WHERE insercao = (SELECT MAX(insercao) FROM tcc.tickers);
            '''
    
    last_update_df = database.read_from_database(query)

    if last_update_df is not None and not last_update_df.empty:
        raw_last_update = last_update_df.iloc[0, 0]
        if pd.notna(raw_last_update):
            last_update = pd.to_datetime(raw_last_update)
        else:
            last_update = None
    else:
        last_update = None

    today = dt.datetime.now()

    if last_update is None or (today - last_update) >= dt.timedelta(days=7):
        periods = {
            'Últimos 15 dias': today - dt.timedelta(days=15),
            'Último mês': today - dt.timedelta(days=30),
            'Últimos 6 meses': today - dt.timedelta(days=182),
            'Último ano': today - dt.timedelta(days=365),
        }

        all_data = []

        for label, start_date in periods.items():
            data = yf.download(
                tickers,
                start=start_date.strftime('%Y-%m-%d'),
                end=today.strftime('%Y-%m-%d'),
                interval='1d',
                auto_adjust=False
            )['Adj Close']
            
            if data.empty:
                continue

            data = data.reset_index()
            data['faixa'] = label
            data['insercao'] = today

            stock_changes = stocks.get_stock_changes(data)
            last_prices = data.iloc[-1].dropna()
            
            structured_df = stocks.structure_df(stock_changes.dropna(), df, last_prices, label)
            all_data.append(structured_df)

    
        result = pd.concat(all_data, ignore_index=True)

    else:

        result = database.read_from_database(query_tickers)
        result["insercao"] = pd.to_datetime(result["insercao"]).dt.strftime('%d/%m/%Y')
        result = result.drop(columns=['id'])
        result = result.rename(columns={
            'ticker': 'Ticker',
            'nome': 'Nome',
            'volume': 'Volume',
            'preco_brl': 'Preço BRL',
            'vol_preco_brl': 'Vol * Preço BRL',
            'variacao': 'Variação %',
            'valor': 'Valor',
            'setor': 'Setor',
            'faixa': 'Faixa',
            'insercao': 'Data de inserção'
        })
    return result