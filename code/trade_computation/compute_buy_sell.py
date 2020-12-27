import pandas as pd

def match(buy, sell):
    for i in range(len(buy)):
        found = False
        for k in range(len(sell)):
            if buy.iloc[i]['Change Left'] == -sell.iloc[k]['Change']:
                buy.iloc[i]['With'].append((sell.iloc[k]['Symbol'], int(buy.iloc[i]['Change Left'])))
                buy.at[i, 'Change Left'] = 0
                sell = sell.drop(k)
                break
    return buy.sort_values(by='Change Left', ascending=False).reset_index(drop=True), sell.sort_values(by='Change', ascending=True).reset_index(drop=True)

def groupMatch(buy, sell):
    for i in range(len(buy)):
        found = False
        for k in range(len(sell)):
            for j in range(k, len(sell)):
                if k != j and k < len(sell) and j < len(sell):
                    if -buy.iloc[i]['Change Left'] == sell.iloc[k]['Change'] + sell.iloc[j]['Change']:
                        buy.iloc[i]['With'].append(sell.iloc[k]['Symbol'])
                        buy.iloc[i]['With'].append(sell.iloc[j]['Symbol'])
                        buy.at[i, 'Change Left'] = 0
                        sell = sell.drop(k)
                        sell = sell.drop(j)
                        break
    return buy.sort_values(by='Change Left', ascending=False).reset_index(drop=True), sell.sort_values(by='Change', ascending=True).reset_index(drop=True)

def bucket(buy, sell):
    max_buy, max_sell = max(buy['Change Left']), -min(sell['Change'])
    if max_buy > max_sell:
        buy.iloc[0]['With'].append((sell.iloc[0]['Symbol'], -int(sell.iloc[0]['Change'])))
        buy.at[0, 'Change Left'] = max_buy - max_sell
        sell = sell.drop(0)
    else:
        buy.iloc[0]['With'].append((sell.iloc[0]['Symbol'], int(buy.iloc[0]['Change Left'])))
        buy.at[0, 'Change Left'] = 0
        sell.at[0, 'Change'] = max_buy - max_sell
    return buy.sort_values(by='Change Left', ascending=False).reset_index(drop=True), sell.sort_values(by='Change', ascending=True).reset_index(drop=True)

def setup(merged_allocations):
    buy, sell = pd.DataFrame(), pd.DataFrame()
    
    for index, x in merged_allocations.iterrows():
        if x['current'] == x['future']:
            pass
        elif x['current'] - x['future'] > 0:
            sell = sell.append(({'Symbol' : x.name, 'Change' : int(x['future']*10) - int(x['current']*10)}), ignore_index=True)
        else:
            buy = buy.append(({'Symbol' : x.name, 'Change Left' : int(x['future']*10) - int(x['current']*10), 'With' : []}), ignore_index=True)
    
    return buy.sort_values(by='Change Left', ascending=False).reset_index(drop=True), sell.sort_values(by='Change', ascending=True).reset_index(drop=True)    

def opt(buy, sell):
    while(len(sell) != 0): # for i in range(30):#
        buy, sell = match(buy, sell)
        if(len(sell) == 0):
            break
        buy, sell = bucket(buy, sell)   