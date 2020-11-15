import pandas as pd
import boto3
import datetime
import numpy as np
from io import StringIO

bucket = 'insula-optim'
folder = 'pairs-data/'
s3 = boto3.resource('s3')
s3_bucket = s3.Bucket(bucket)


def loading_pairs_data(optim_env, begin_date):
    for i, object_summary in enumerate(s3_bucket.objects.filter(Prefix="pairs-data/")):
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

def transformation_into_optim_format(data, optim_env):
    # Transforming the dataframe into the good format
    for i,symbol in enumerate(optim_env):
        if i == 0:
            data_optim = data[data['symbol'] == symbol][['date', 'open']]
            data_optim.columns = ['date', symbol]
        else: 
            data_to_add = data[data['symbol'] == symbol][['date', 'open']]
            data_to_add.columns = ['date', symbol]
            data_optim = pd.merge(data_to_add, data_optim, on='date', how='outer')

    # sorting /removing duplicate dates / ffill and bfill for NaN values (axis=0 <--> for the same symbol)
    data_optim = data_optim.sort_values(by='date').drop_duplicates(subset=['date']).ffill(axis=0).bfill(axis=0).set_index('date')

    # add ETH/ETH pair
    data_optim['ETH/ETH'] = 1.0
    
    return data_optim


def compute_markowitz_optim(window, rebalance_period, nportfolio, optim_env, data):
    nassets = len(optim_env) + 1 # Account for the ETH/ETH pair
    n = window
    prices = data
    pr = np.asmatrix(prices.values)
    t_prices = prices.iloc[1:n + 1]
    t_val = t_prices.values
    tminus_prices = prices.iloc[0:n]
    tminus_val = tminus_prices.values
    # Compute daily returns (r)
    r = np.asmatrix(t_val / tminus_val - 1)
    # Compute the expected returns of each asset with the average
    # daily return for the selected time window
    m = np.asmatrix(np.mean(r, axis=0))
    # ###
    stds = np.std(r, axis=0)
    # Compute excess returns matrix (xr)
    xr = r - m
    # Matrix algebra to get variance-covariance matrix
    cov_m = np.dot(np.transpose(xr), xr) / n
    # Compute asset correlation matrix (informative only)
    #corr_m = cov_m / np.dot(np.transpose(stds), stds)

    # Define portfolio optimization parameters
    n_portfolios = nportfolio
    results_array = np.zeros((3 + nassets, n_portfolios))
    for p in range(n_portfolios):
        weights = np.random.random(nassets)
        weights /= np.sum(weights)
        w = np.asmatrix(weights)
        p_r = np.sum(np.dot(w, np.transpose(m))) * 365
        p_std = np.sqrt(np.dot(np.dot(w, cov_m),
                               np.transpose(w))) * np.sqrt(365)

        # store results in results array
        results_array[0, p] = p_r
        results_array[1, p] = p_std
        # store Sharpe Ratio (return / volatility) - risk free rate element
        # excluded for simplicity
        results_array[2, p] = results_array[0, p] / results_array[1, p]

        for i, w in enumerate(weights):
            results_array[3 + i, p] = w

    columns = ['r', 'stdev', 'sharpe'] + optim_env + ['ETH/ETH']

    # convert results array to Pandas DataFrame
    results_frame = pd.DataFrame(np.transpose(results_array),
                                 columns=columns)
    # locate position of portfolio with highest Sharpe Ratio
    max_sharpe_port = results_frame.iloc[results_frame['sharpe'].idxmax()]
    # locate positon of portfolio with minimum standard deviation
    min_vol_port = results_frame.iloc[results_frame['stdev'].idxmin()]
    
    return results_frame, max_sharpe_port, min_vol_port
    