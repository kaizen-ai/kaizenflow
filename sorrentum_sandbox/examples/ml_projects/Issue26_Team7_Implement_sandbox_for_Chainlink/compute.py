"""
Compute data from the chainlink_history table and chainlink_real_time table.

Import as:


import sorrentum_sandbox.examples.ml_projects.Issue26_Team7_Implement_sandbox_for_Chainlink.compute as compute
"""


import pandas as pd
import dask.dataframe as dd
import sorrentum_sandbox.common.download as ssandown
import sorrentum_sandbox.examples.ml_projects.Issue26_Team7_Implement_sandbox_for_Chainlink.query_API as query_API


def percentage_diff(df):
    return (df['price_shift']-df['price']) / df['price'] * 100

def computer(**kwargs):

    if 'start_roundid' in kwargs:
        start_roundid = kwargs['start_roundid']
        start_roundid = str(start_roundid)
        query = "SELECT roundid, price, updatedat, pair, decimals FROM chainlink_real_time WHERE roundid > {}"
        formatted_query = query.format(start_roundid)
        df = query_API.query_from_db(formatted_query)
    else: 
        history_df = query_API.query_from_db("Select roundid, price, updatedat, pair, decimals From chainlink_history")
        real_time_df = query_API.query_from_db("Select roundid, price, updatedat, pair, decimals From chainlink_real_time")
        df = pd.concat([history_df, real_time_df])
    

    df['price_shift'] = df['price'].shift(-1)
    ddf = dd.from_pandas(df, npartitions=10)
    
    # calculate the percentage difference using the map_partitions() method
    ddf['percentage_diff'] = ddf.map_partitions(percentage_diff, meta=('x', 'f8'))

    # drop the first row, which will have NaN values
    ddf = ddf.dropna()
    ddf = ddf.drop(columns=['price_shift'])

    # convert object to float and round to the second decimal points
    ddf['percentage_diff'] = ddf['percentage_diff'].astype(float).round(2)

    # save dask dataframe to pandas dataframe
    compute_df = ddf.compute()
        
    return ssandown.RawData(compute_df)