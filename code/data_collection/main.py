from collection import *
import json
import boto3
from datetime import date
from io import StringIO



bucket = 'insula-optim'
folder = 'pairs-data/'

list_currencies_ccxt = [
    'ETH/BTC', 
    'ETH/USDT', 
    'KNC/ETH', 
    'BAT/ETH', 
    'MANA/ETH', 
    'ZRX/ETH',
    'LINK/ETH', 
    'REP/ETH', 
    'RLC/ETH',
    'ELF/ETH', 
    'BNT/ETH', 
    'SNT/ETH', 
    'ENJ/ETH', 
    'LEND/ETH', 
    'LRC/ETH',  
    'POWR/ETH'
]

list_currencies_coingecko = [
    ['POLY_ETH', 'polymath-network'],
    ['REQ_ETH', 'request-network'],
    ['RDN_ETH', 'raiden-network'],
    ['RCN_ETH', 'ripio-credit-network'],
    ['MKR_ETH', 'maker'],
    ['ANT_ETH', 'aragon'],
    ['PNK_ETH', 'kleros'],
    ['GNO_ETH', 'gnosis'],
    ['GEN_ETH', 'daostack'],
    ['MLN_ETH', 'melon'],
    ['UBT_ETH', 'unibright'],
    ['NMR_ETH', 'numeraire'],
    ['LPT_ETH', 'livepeer'],
    ['XBASE_ETH', 'eterbase'],
    ['COT_ETH', 'cotrader'],
    ['AMN_ETH', 'amon'],
    ['MET_ETH', 'metal'],
    ['BLT_ETH', 'bloom'],
    ['SNX_ETH', 'havven'],
    ['TKN_ETH', 'tokencard'],
    ['LOC_ETH', 'lockchain'],
    ['REN_ETH', 'republic-protocol'],
    ['BTU_ETH', 'btu-protocol']
]

#create client
s3 = boto3.resource('s3')


if __name__ == "__main__":
    today = str(date.today())
    
    print('[INFO] Begining Creation Table')
    df_pairs = concat_tables(nb_days = 7, list_currencies_ccxt=list_currencies_ccxt, list_currencies_coingecko=list_currencies_coingecko)
    print('[INFO] Creation Table Finished')
    
    df_pairs_key = folder + 'crypto_pairs_' + '7d_' + today + '.csv'
    
    print('[INFO] Begining creation Object')
    crypto_object = s3.Object(bucket, df_pairs_key)
    
    # already created on S3
    csv_buffer = StringIO()
    
    print('[INFO] Begining putting into buffers')
    df_pairs.to_csv(csv_buffer)
    
    print('[INFO] Begining Saving into S3')
    crypto_object.put(Body=csv_buffer.getvalue())
    print('[INFO] WELL DONE, IT IS FINISHED !')
    
