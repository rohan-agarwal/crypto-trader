"""
This file pulls historical data from GDAX on the price of BTC/ETH
And stores to an SQL instance
"""

# Packages
import datetime as dt
import gdax as gx
import math
import pandas as pd

# Config
public_client = gx.PublicClient()

# Functions
def gdax_call(currency, end, granularity, start):
    """
    Pull and format historical data w/pandas using GDAX public API.

    NB: The granularity field must be one of the following values:
    {60, 300, 900, 3600, 21600, 86400}.
    """

    data = public_client.get_product_historic_rates(
        currency,
        start=start,
        end=end,
        granularity=granularity
    )
  
    if len(data) <= 1:
    # Rate limiting
        if ('Rate' in data['message']):
            data = gdax_call(currency, end, granularity, start)
  
    data = (
        pd.DataFrame(data)
        .rename(columns={
            0:'time',
            1:'low',
            2:'high',
            3:'open',
            4:'close',
            5:'volume'
        })
        .assign(time = lambda x: pd.to_datetime(x['time'], unit='s'))
    )
  
    return data


def gdax_daterange(start, end):
    """
    Generate list of timestamps to pull data chunks for
    """
  
    start = dt.datetime.strptime(start, '%Y-%m-%d')
    end = dt.datetime.strptime(end, '%Y-%m-%d')
    periods = math.ceil((end-start).days*4.8) + 1
  
    date_range = pd.date_range(start, periods=periods, freq='5H').tolist()
    return date_range



def get_from_gdax(currency, start, end):
    """
    Get data from any time frame by breaking into chunks and calling gdax API
    Chunks are needed due to API limits
    """
  
    combined_data = pd.DataFrame()
    date_range = gdax_daterange(start, end)
    
    for x in range(0, len(date_range)-2):
        data = gdax_call(
            currency=currency,
            start=str(date_range[x]),
            end=str(date_range[x+1]),
            granularity=60
        )
        combined_data = pd.concat([data, combined_data])
    
    combined_data = combined_data.reset_index()
    return combined_data
