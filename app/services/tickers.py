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
    today = dt.datetime.now()

    periods = {
        'Últimos 15 dias': today - dt.timedelta(days=15),
        'Último mês': today - dt.timedelta(days=30),
        'Últimos 6 meses': today - dt.timedelta(days=182),
        'Último ano': today - dt.timedelta(days=365),
    }

    all_data = []

    for faixa, start_date in periods.items():
        check_query = f"""
            SELECT MAX(insercao)
            FROM cnpj2.tickers
            WHERE faixa = %s;
        """
        last_update_df = database.read_from_database(check_query, params=(faixa,))
        last_update = pd.to_datetime(last_update_df.iloc[0, 0]) if not last_update_df.empty and pd.notna(last_update_df.iloc[0, 0]) else None

        if last_update is not None and (today - last_update) < dt.timedelta(days=7):
            
            faixa_data = database.read_from_database(
                "SELECT * FROM cnpj2.tickers WHERE faixa = %s AND insercao = %s",
                params=(faixa, last_update.strftime('%Y-%m-%d %H:%M:%S'))
            )
        else:
            try:
                print(f"Buscando dados da API para: {faixa}")
                data = yf.download(
                    tickers,
                    start=start_date.strftime('%Y-%m-%d'),
                    end=today.strftime('%Y-%m-%d'),
                    interval='1d',
                    auto_adjust=False
                )['Adj Close']

                if data.empty:
                    raise ValueError("Dados vazios da API")

                data = data.reset_index()
                data['faixa'] = faixa
                data['insercao'] = today

                stock_changes = stocks.get_stock_changes(data)
                last_prices = data.iloc[-1].dropna()
                structured_df = stocks.structure_df(stock_changes.dropna(), df, last_prices, faixa)
                faixa_data = structured_df

            except Exception as e:
                print(f"Erro ao buscar da API para a faixa '{faixa}': {e}")
                print("Carregando dados mais recentes do banco para essa faixa.")
                fallback_query = f"""
                    SELECT *
                    FROM cnpj2.tickers
                    WHERE faixa = %s
                    ORDER BY insercao DESC
                    LIMIT 1;
                """
                faixa_data = database.read_from_database(fallback_query, params=(faixa,))

        if not faixa_data.empty:
            all_data.append(faixa_data)

    if all_data:
        result = pd.concat(all_data, ignore_index=True)

        result["insercao"] = pd.to_datetime(result["insercao"]).dt.strftime('%d/%m/%Y')
        if 'id' in result.columns:
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
    else:
        print("Nenhum dado disponível.")
        return pd.DataFrame()