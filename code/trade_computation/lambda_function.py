import json
import boto3
from get_allocation import *
from merge_allocations import get_all_s3_keys,get_latest_key,access_max_sharpe_portfolio,arrange_max_sharpe_portfolio,merge_allocations
from compute_buy_sell import setup, opt
import datetime
from io import StringIO

#Choose which fund you want to reallocate
fund = 'malta' # 'insula' or 'anastasia'

today = pd.to_datetime(str(datetime.date.today()))


bucket = 'insula-optim'
folder = 'optim-results'
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
s3_bucket = s3.Bucket(bucket)


def lambda_handler(event, context):
    # Load future allocations (output by Optimization Algorithm)
    print('[INFO] Loading future allocations from Optimization Algo')
    keys = get_all_s3_keys(bucket, prefix='optim-results')
    key = get_latest_key(keys)
    max_sharpe = access_max_sharpe_portfolio(key)
    future = arrange_max_sharpe_portfolio(max_sharpe)
    
    # Load the current allocation for the right fund
    print('[INFO] Loading current allocations from requests to the fund')
    if fund == 'insula': current = get_insula_allocation()
    elif fund == 'malta': current = get_malta_allocation()
    elif fund == 'anastasia': current = get_anastasia_allocation()
    
    # Merge the 2 files
    print('[INFO] Merging tables')
    merged_allocations = merge_allocations(current, future)
    
    # Compute the Buy/Sell Trades
    print('[INFO] Computing Buy/Sell Trades to do')
    buy, sell = setup(merged_allocations)
    opt(buy, sell)
    print(buy.sort_values(by='Symbol'))
    
    
    print('[INFO] Begining storage on s3')
    folder = 'trades/'
    results_key = folder + 'reallocation_' + str(today)[:10] +'.csv'
    results_object = s3.Object(bucket, results_key)
    csv_buffer = StringIO()
    buy.sort_values(by='Symbol').to_csv(csv_buffer)
    results_object.put(Body=csv_buffer.getvalue())
    
    print('[INFO] WELL DONE, IT IS FINISHED !')
    print('Path to the output results: ' + bucket + '/' + results_key)
    
    return 