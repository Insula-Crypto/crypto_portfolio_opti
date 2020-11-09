import numpy as np
import pandas as pd
import boto3
import datetime

# We define the list of tokens we want to use 
optim_env = ['BNT/ETH', 
            'SNT/ETH', 
            'ENJ/ETH', 
            'LEND/ETH', 
            'LRC/ETH',  
            'POWR/ETH']

today = pd.to_datetime(str(date.today()))
nb_days = 200
begin_date = today - datetime.timedelta(days=nb_days)

# Parameters of the Optimization
nportfolio = 10000 # Number of portfolio simulations 
window = 28 # Set the time window that will be used to compute expected return and asset correlations
rebalance_period = 14 # Set the number of days between each portfolio rebalancing

if __name__ == "__main__":
    data = loading_pairs_data(optim_env, begin_date)
    data_optim = transformation_into_optim_format(data, optim_env)
    results_frame, max_sharpe_port, min_vol_port = compute_markowitz_optim(window, rebalance_period, nportfolio, optim_env, data_optim)
    
    folder = 'optim-results/' + today + '/'
    results_key = folder + 'results_frame.csv'
    max_sharpe_port_key = folder + 'max_sharpe_port.csv'
    min_vol_port_key = folder + 'min_vol_port.csv'
    
    print('[INFO] Begining creation Object')
    results_object = s3.Object(bucket, results_key)
    max_object = s3.Object(bucket, max_sharpe_port_key)
    min_object = s3.Object(bucket, min_vol_port_key)
    
    # already created on S3
    csv_buffer = StringIO()
    results_frame.to_csv(csv_buffer)
    results_object.put(Body=csv_buffer.getvalue())
    
    csv_buffer = StringIO()
    max_sharpe_port.to_csv(csv_buffer)
    max_object.put(Body=csv_buffer.getvalue())
    
    csv_buffer = StringIO()
    min_vol_port.to_csv(csv_buffer)
    min_object.put(Body=csv_buffer.getvalue())
    
    print('[INFO] WELL DONE, IT IS FINISHED !')
    
    