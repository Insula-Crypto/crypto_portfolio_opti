import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from pycoingecko import CoinGeckoAPI
import datetime
import ccxt
from pandas._libs.tslibs.timestamps import Timestamp

def get_market_data_coingecko(symbol, _id, nb_days):
    cg = CoinGeckoAPI()
    try:
        data = cg.get_coin_market_chart_by_id(_id, 'eth', 500)
    except: return '[ERROR] No such coin ID found ({symbol}, {_id})'.format(symbol, _id)

    dates = [datetime.datetime.fromtimestamp(data['prices'][i][0] / 1000).strftime('%Y-%m-%d') for i in range(len(data['prices']))]
    ts = [data['prices'][i][0] for i in range(len(data['prices']))]
    price  = [data['prices'][i][1] for i in range(len(data['prices']))]
    market_caps = [data['market_caps'][i][1] for i in range(len(data['market_caps']))]
    total_volumes = [data['total_volumes'][i][1] for i in range(len(data['total_volumes']))]
    symbols = [symbol]*len(data['total_volumes'])
    source = ['coingecko']*len(data['total_volumes'])
    
    df_data = {'timestamp' : ts, 'date' : dates,'source':source, 'symbol': symbols, 'open': price, 'total_volume': total_volumes} 
    df = pd.DataFrame(df_data)
    df['date'] = df.apply(lambda x: Timestamp(x.date), axis=1)
    
    if df.iloc[-1,:]['date'] == df.iloc[-2,:]['date']:
        return df[-nb_days-1:-1]

    return df[-nb_days:]


def get_market_data_ccxt(symbol: str, exchange_: str, timeframe: str, nb_days: int = 7):
    '''
    symbol: The Symbol of the Instrument/Currency Pair To Download. You can see the list with exchange.markets.keys()
    exchange: The exchange to download from. ex: binance, kraken, bitfinex ...
    timeframe: choices=['1m', '5m','15m', '30m','1h', '2h', '3h', '4h', '6h', '12h', '1d', '1M', '1y']
    '''
    # Get our Exchange
    try:
        exchange = getattr (ccxt, exchange_) ()
    except AttributeError:
        print('-'*36,' ERROR ','-'*35)
        print('Exchange "{}" not found. Please check the exchange is supported.'.format(exchange_))
        print('-'*80)
        quit()

    # Check if fetching of OHLC Data is supported
    if exchange.has["fetchOHLCV"] != True:
        print('-'*36,' ERROR ','-'*35)
        print('{} does not support fetching OHLC data. Please use another exchange'.format(exchange_))
        print('-'*80)
        quit()

    # Check requested timeframe is available. If not return a helpful error.
    if (not hasattr(exchange, 'timeframes')) or (timeframe not in exchange.timeframes):
        print('-'*36,' ERROR ','-'*35)
        print('The requested timeframe ({}) is not available from {}\n'.format(timeframe,exchange_))
        print('Available timeframes are:')
        for key in exchange.timeframes.keys():
            print('  - ' + key)
        print('-'*80)
        quit()

    # Check if the symbol is available on the Exchange
    exchange.load_markets()
    if symbol not in exchange.symbols:
        print('-'*36,' ERROR ','-'*35)
        print('The requested symbol ({}) is not available from {}\n'.format(symbol,exchange_))
        print('Available symbols are:')
        for key in exchange.symbols:
            print('  - ' + key)
        print('-'*80)
        quit()

    # eliminate ETH/ETH
    # Get data
    data = exchange.fetch_ohlcv(symbol, timeframe)
    header = ['timestamp', 'open', 'high', 'low', 'close', 'total_volume']
    df = pd.DataFrame(data, columns=header)#.set_index('timestamp')
    df['date'] = pd.to_datetime(df.timestamp, unit='ms')
    df['symbol'] = symbol
    df['source'] = 'ccxt_' + exchange_
    
    if symbol == 'ETH/BTC':
        df[['open','high','low','close']] = 1. / df[['open','high','low','close']]
        df['symbol'] = 'BTC/ETH' 
    
    cols = ['timestamp', 'date', 'source', 'symbol', 'open', 'total_volume']
    df = df[cols]
        
    return df[-nb_days:]

def concat_tables(nb_days: int, list_currencies_ccxt: str, list_currencies_coingecko: str):
    for i,pair in enumerate(list_currencies_ccxt):
        if i ==0: pairs_df = get_market_data_ccxt(symbol=pair, 
                        exchange_='binance', 
                        timeframe='1d', 
                        nb_days=nb_days)
        else: 
            to_append_df = get_market_data_ccxt(symbol=pair, 
                        exchange_='binance', 
                        timeframe='1d', 
                        nb_days=nb_days)
            pairs_df = pd.concat([pairs_df, to_append_df], axis=0, ignore_index=True)

    for currency in list_currencies_coingecko:
        pairs_df = pd.concat([pairs_df, get_market_data_coingecko(currency[0], currency[1], nb_days)], axis=0, ignore_index=True)

    return pairs_df
