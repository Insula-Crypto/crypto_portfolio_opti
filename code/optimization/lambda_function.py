import pandas as pd
import numpy as np
import boto3
import datetime
from io import StringIO
from optim_functions import loading_pairs_data, transformation_into_optim_format, compute_markowitz_optim, plot_portfolio_simulation
from matplotlib import pyplot as plt
import io 

s3 = boto3.resource('s3')
bucket = 'insula-optim'

# We define the list of tokens we want to use 
optim_env = ['BTC/ETH'
            ,'LINK/ETH'
            ,'REN/ETH'
            ,'MLN/ETH'
            ,'ANT/ETH' 
            ,'ZRX/ETH'
            ,'MKR/ETH'
            ,'BAT/ETH'
            ,'MANA/ETH'
            ,'KNC/ETH'
            ,'RLC/ETH'
            ]
            
# Parameters of the Optimization
nportfolio = 50000 # Number of portfolio simulations 
window = 60 # Set the time window that will be used to compute expected return and asset correlations



today = pd.to_datetime(str(datetime.date.today()))
begin_date = today - datetime.timedelta(days=500)
folder = 'optim-results/' + str(today)[:10] + '/'

def lambda_handler(event, context):
    data = loading_pairs_data(optim_env, begin_date)
    data_optim = transformation_into_optim_format(data, optim_env)
    data_optim = data_optim.iloc[::-1]
    results_frame, max_sharpe_port, min_vol_port = compute_markowitz_optim(window, nportfolio, optim_env, data_optim)
    
    fig, ax = plt.subplots(1,1, figsize=(15,10))
    plot_portfolio_simulation(results_frame, max_sharpe_port, min_vol_port)
    
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    buf.seek(0)
    image = buf.read()
    
    # Put Image into s3
    image_key = folder + 'simulations_plot_' + str(today)[:10] + '.png'
    s3.Object(bucket, image_key).put(Body=bytes(image))
    
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
    
    print(max_sharpe_port)
    
    print('[INFO] WELL DONE, IT IS FINISHED !')
    
    return 0