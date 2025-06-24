import pandas as pd
from services import utils
from services import database
import datetime as dt

def get_stock_changes(data: pd.DataFrame) -> pd.Series:
    data_numeric = data.select_dtypes(include=['number'])
    return (data_numeric.iloc[0] - data_numeric.iloc[0].shift(1)) / data_numeric.iloc[0].shift(1) * 100

def structure_df(tickers, df, last_prices, period):
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
            
            info_list.append([
                ticker_symbol, name, vol, price, activity,
                percent_change, last_price, sector,
                period,
                dt.datetime.today()
            ])
        
        except IndexError:
            print(f"Failed to extract info for ticker: {ticker}")
    
    info_df = pd.DataFrame(info_list, columns=['ticker', 'nome', 'volume', 'preco_brl', 'vol_preco_brl','variacao', 'valor', 'setor', 'faixa', 'insercao'])

    info_df['volume'] = info_df['volume'].apply(utils.format_value)
    info_df['preco_brl'] = info_df['preco_brl'].apply(utils.format_value)
    info_df['vol_preco_brl'] = info_df['vol_preco_brl'].apply(utils.format_value)
    
    database.write_to_database("tickers", info_df, "append")
    info_df['insercao'] = pd.to_datetime(info_df['insercao']).dt.strftime('%d/%m/%Y')
    return info_df.rename(columns={
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