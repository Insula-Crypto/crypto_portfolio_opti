import ccxt
from datetime import datetime, timedelta, timezone
import math
import pandas as pd


def create_ETH_BTC(data_path):
    trading_pair = 'ETH_BTC'
    trading_pair_out = 'BTC_ETH'
    exchange_ = 'binance'
    timeframe = '1d'

    h = pd.read_csv(data_path + 'binance-{}-1d.csv'.format(trading_pair))
    h = h.set_index('Timestamp')
    h_ = h
    h_[['Open','High','Low','Close']] = 1. / h[['Open','High','Low','Close']]
    h_['Volume'] = h['Volume']

    filename = '{}-{}-{}.csv'.format(exchange_, trading_pair_out,timeframe)
    h_.to_csv(data_path + filename)
    
def create_ETH_ETH(data_path):
    trading_pair = 'ETH_USDT'
    trading_pair_out = 'ETH_ETH'
    exchange_ = 'binance'
    timeframe = '1d'
    
    h = pd.read_csv(data_path + 'binance-{}-1d.csv'.format(trading_pair))
    h = h.set_index('Timestamp')
    h_=h
    h_[['Open','High','Low','Close']] = 1.
    filename = '{}-{}-{}.csv'.format(exchange_, trading_pair_out,timeframe)
    h_.to_csv(data_path + filename)
 


def get_market_data(symbol: str, exchange_: str, timeframe: str, save=False, data_path=None):
    '''
    symbol: The Symbol of the Instrument/Currency Pair To Download. You can see the list with exchange.markets.keys()
    exchange: The exchange to download from. ex: binance, kraken, bitfinex ...
    timeframe: choices=['1m', '5m','15m', '30m','1h', '2h', '3h', '4h', '6h', '12h', '1d', '1M', '1y']
    save: if you want to save the dataframe in a csv
    data_path: the path to the data
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


    # Get data
    data = exchange.fetch_ohlcv(symbol, timeframe)
    header = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = pd.DataFrame(data, columns=header).set_index('Timestamp')
    df = df.set_index(pd.to_datetime(df.index, unit='ms'))
    
    # Save it
    if save and data_path:
        symbol_out = symbol.replace("/","_")
        filename = '{}-{}-{}.csv'.format(exchange_, symbol_out,timeframe)
        df.to_csv(data_path + filename)
        
    return df
