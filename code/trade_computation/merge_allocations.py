import datetime
import boto3
from io import StringIO
import pandas as pd


bucket = 'insula-optim'
folder = 'optim-results'
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
s3_bucket = s3.Bucket(bucket)

def get_all_s3_keys(bucket, prefix=''):
    """Get a list of all keys in an S3 bucket."""
    keys = []

    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            keys.append(obj['Key'])

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
    
    keys = list(set([key.split('/',2)[1] for key in keys]))
    keys.remove("")
    
    return keys


def get_latest_key(keys):
    today = pd.to_datetime(str(datetime.date.today()))
    keys_datetime = [ pd.to_datetime(key) for key in keys]
    diff_second = [(today - key).total_seconds() for key in keys_datetime]
    key = keys[diff_second.index(min(diff_second))]
    
    return key
    
def access_max_sharpe_portfolio(key):
    max_sharpe = 'optim-results/' + key + '/max_sharpe_port.csv'
    alloc_object = s3.Object(bucket, max_sharpe)
    file_content = alloc_object.get()['Body'].read().decode('utf-8')
    alloc = pd.read_csv(StringIO(file_content))

    return alloc

def arrange_max_sharpe_portfolio(alloc):
    max_sharpe_port = alloc
    max_sharpe_port.columns = ['Symbol', 'Value']
    max_sharpe_port = max_sharpe_port[max_sharpe_port.index.values >2]
    max_sharpe_port['Symbol'] = max_sharpe_port.apply(lambda x: x.Symbol[:-4], axis=1)
    max_sharpe_port['Value'] = max_sharpe_port.apply(lambda x: 100*x.Value, axis=1)
    max_sharpe_port = max_sharpe_port.set_index('Symbol')
    
    return max_sharpe_port

def merge_allocations(current, future):
    merged_allocations = pd.merge(current, future, on= 'Symbol', how = 'outer').fillna(0)
    merged_allocations.columns = ['current', 'future']
    merged_allocations['current'] = merged_allocations.apply(lambda x: round(x['current'],1), axis=1)
    merged_allocations['future'] = merged_allocations.apply(lambda x: round(x['future'],1), axis=1)
    
    sum_current = round(merged_allocations['current'].sum(),1)
    sum_future = round(merged_allocations['future'].sum(),1)
    diff = round(sum_current - sum_future, 1)
    
    if diff !=0:
        symbol = merged_allocations['current'].argmax()
        merged_allocations.iloc[symbol]['current'] = merged_allocations.iloc[symbol]['current'] - diff
    
    return merged_allocations