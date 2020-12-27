import requests
import pandas as pd
import boto3

s3 = boto3.resource('s3')
bucket = 'insula-optim'

def get_malta_allocation():
    jsonData = '{"query": "{fundHoldings(where: {fund: \\"0x26491fc7da30b35d818de45982fb1de4f65ed8f5\\"}){asset {id}, assetGav}}"}'
    data = requests.post('https://api.thegraph.com/subgraphs/name/melonproject/melon', data = jsonData).json()['data']

    assetDict = {
        '0x0d8775f648430679a709e98d2b0cb6250d2887ef': 'BAT',
        '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984': 'UNI',
        '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599': 'WBTC',
        '0x408e41876cccdc0f92210600ef50372656052a38': 'REN',
        '0x514910771af9ca656af840dff83e8264ecf986ca': 'LINK',
        '0x607f4c5bb672230e8672085532f7e901544a7375': 'RLC',
        '0x960b236a07cf122663c4303350609a66a7b288c0': 'ANT',
        '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2': 'MKR',
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2': 'ETH',
        '0xe41d2489571d322189246dafa5ebde1f4699f498': 'ZRX',
        '0xec67005c4e498ec7f55e092bd1d35cbc47c91892': 'MLN',
        '0x0f5d2fb29fb7d3cfee444a200298f468908cc942': 'MANA',
        '0xdd974d5c2e2928dea5f71b9825b8b646686bd200': 'KNC'
    }

    assets = []
    nav = []

    for i in range(len(data['fundHoldings'])):
        assets.append(assetDict[data['fundHoldings'][i]['asset']['id']])
        nav.append(int(data['fundHoldings'][i]['assetGav']) / 1e18)

    df = pd.DataFrame({'Symbol': assets,'Value': nav}, dtype=float)
    df['Value'] = 100*(df['Value']/df['Value'].sum())
    df = df.set_index('Symbol')
    
    df_real = pd.DataFrame({'Symbol': assets,'Value': nav}, dtype=float)
    df_real = df_real.set_index('Symbol')
    
    
    return df, df_real
   
    
def get_anastasia_allocation():
    sent = requests.get('https://api.ethplorer.io/getAddressInfo/0x9C49c053a8b9106024793516EE3c5562875A5C9a?apiKey=freekey').json()

    data = {'Symbol' : ['ETH'], 'Value' : [sent['ETH']['balance'] * sent['ETH']['price']['rate']]}
    df = pd.DataFrame(data, dtype=float)

    tokens = sent['tokens']
    for token in tokens:
        if token['tokenInfo']['price'] != False:
            newRow = {'Symbol' : token['tokenInfo']['symbol'], 'Value' : token['balance'] * token['tokenInfo']['price']['rate'] / 10 ** (int(token['tokenInfo']['decimals']))}
            df = df.append(newRow, ignore_index=True)
   
    df['Value'] = 100*(df['Value']/df['Value'].sum())
    df = df.set_index('Symbol')
    
    return df


def get_insula_allocation():
    sent = requests.get('https://api.ethplorer.io/getAddressInfo/0xCB60D600160D005845Ec999f64266D5608fd8943?apiKey=freekey').json()

    data = {'Symbol' : ['ETH'], 'Value' : [sent['ETH']['balance'] * sent['ETH']['price']['rate']]}
    df = pd.DataFrame(data)

    tokens = sent['tokens']
    for token in tokens:
        if token['tokenInfo']['price'] != False:
            newRow = {'Symbol' : token['tokenInfo']['symbol'], 'Value' : token['balance'] * token['tokenInfo']['price']['rate'] / 10 ** (int(token['tokenInfo']['decimals']))}
            df = df.append(newRow, ignore_index=True)

    df['Value'] = 100*(df['Value']/df['Value'].sum())
    df = df.set_index('Symbol')
    
    return df
    
def loading_pairs_data(optim_env, begin_date):
    for i, object_summary in enumerate(s3_bucket.objects.filter(Prefix="optim_results/")):
        
        if i>0:
            key = str(object_summary.key)
            csv_obj = s3.Object(bucket, key)
            csv_string = csv_obj.get()['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_string))
            df = df[df['symbol'].isin(optim_env)]
            if i == 1 :
                data = df
                #print(data)
            else:
                data_add = df
                data = pd.concat([data, data_add], axis=0)

    data = data.reset_index().drop(columns=['Unnamed: 0', 'index'])  
    data = data[pd.to_datetime(data['date'])>= begin_date]
    
    return data