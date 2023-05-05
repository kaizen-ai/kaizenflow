import numpy as np
import pandas as pd
import datetime
from typing import Callable, List, Optional, Tuple, Union
import pyarrow as pa
import pyarrow.parquet as pq

def create_single(exchange_df):
    exchange_df = exchange_df.reset_index()
    exchange_df['timestamp'] = pd.to_datetime(exchange_df['timestamp'])
    exchange_df['timestamp'] = exchange_df['timestamp'].dt.tz_localize(None)
    exchange_df['timestamp'] = exchange_df['timestamp'].astype('datetime64[ns]')
    exchange_df = exchange_df.sort_values(by='timestamp')
    id = exchange_df['exchange_id'].unique()[0]
    exchange_df = exchange_df.drop(columns=['timestamp.1', 'knowledge_timestamp', 'open', 'close', 'year', 'month', 'exchange_id'])
    grouped = exchange_df.groupby('currency_pair')
    dfs = [grouped.get_group(x) for x in grouped.groups]
    timestamp = pd.DataFrame(exchange_df['timestamp'].unique())
    timestamp = timestamp.rename(columns={0:"timestamp"})
    merged = timestamp.copy()
    for df in dfs:
        df[f"vwap-{id}:{df['currency_pair'].unique()[0]}"] = np.cumsum(df['volume'] * (df['high'] + df['low'])/2) / np.cumsum(df['volume'])
        df.rename(columns={'volume' : 'volume-' + id +':'+df['currency_pair'].unique()[0]}, inplace=True)
        df.drop(columns=['high', 'low', 'currency_pair'], inplace=True)
        df.set_index('timestamp', inplace=True)
    for df in dfs:
        merged = pd.merge_asof(merged, df, on='timestamp')
    merged = merged.set_index('timestamp')
    return merged, timestamp

def convert_to_multi(df, timestamp):
    # creating most inner column 
    vols, vwap = "", ""
    vol_cols, vwap_cols = list(df.loc[:, df.columns.str.startswith('volume')].columns), list(df.loc[:, df.columns.str.startswith('vwap')].columns)
    for elem in vol_cols:
        vols = "".join([vols, elem]) + " "
    for elem in vwap_cols:
        vwap = "".join([vwap, elem]) + " "

    # creating the middle column (the exchange: okx, binance_spot, binance_future, binanceus)
    exchange_str = ""
    for col in vol_cols:
        h, s = col.rfind('-') + 1, col.rfind(':')
        exchange_str += col[h:s] + " "
    for col in vwap_cols:
        h, s = col.rfind('-') + 1, col.rfind(':')
        exchange_str += col[h:s] + " "

    # creating outer column (the feature: volume or vwap)
    volume_str, vwap_str = "volume " * int(len(list(df.columns))/2), "vwap " * int(len(list(df.columns))/2)

    new_df = pd.DataFrame(np.array(df), columns=[(volume_str + vwap_str).split(), exchange_str.split(), (vols + vwap).split()]) # this line needs to include a new level
    # new_df.set_index(timestamp, inplace=True)

    d = dict(zip(new_df.columns.levels[2], [word[2][word[2].rfind(':') + 1:] for word in new_df.columns]))
    new_df = new_df.rename(columns=d, level=2)
    new_df = new_df.set_index(timestamp['timestamp'].astype('datetime64[ns]'))
    return new_df