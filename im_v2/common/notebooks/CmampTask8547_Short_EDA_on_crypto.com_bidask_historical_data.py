# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Crypto.com bid/ask perpetual futures data EDA
#
# For details on how this data was obtained refer to
# `docs/datapull/ck.cryptocom_data_pipeline.explanation.md`

# %% [markdown]
# ## Dataset structure
#
# - Order book data is stored as a set of compressed `.gz` files 
# - The files contain order book snapshots 
# - Example data from 20th October 2023:
#
# ```
# sftp> pwd
# Remote working directory: /exchange/book_l2_150_0010/2023/10/20/cdc/BTCUSD-PERP
# sftp> ls
# 1697760000001.gz    1697760036853.gz    1697760093027.gz    1697760146098.gz    1697760195389.gz    1697760216857.gz    
# 1697760264748.gz    1697760316092.gz    1697760364434.gz    1697760396854.gz    1697760453532.gz    1697760512916.gz
# ```
#
# - Name of the file refers to the start timestamp of the data interval stored in that file
#
# - Example file /exchange/book_l2_150_0010/2023/10/20/cdc/BTCUSD-PERP/1697760000001.gz` (stored in gdrive [here](https://drive.google.com/file/d/1zKxQtraKtndUP8ZJgWeiQBdGSRspfb08/view?usp=drive_link))

# %% [markdown]
# ## File structure
#
# - Each line contains an order book update in JSON format:
#
# ```
# > head -n 1 1697760000001 
# {"S":"BTCUSD-PERP","s":"1","t":1697760000001,"p":1697760000006,"a":[[28725.5,6.0E-4],[28725.9,5.0E-4],[28726.2,0.035], ... ,"b":[[28722.9,0.035],[28722.4,0.04],... }
# ```
#
# - "t" - transaction time
# - "p" - message publish time

# %%
# %load_ext autoreload
# %autoreload 2

import logging
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import im_v2.common.data.client.im_raw_data_client as imvcdcimrdc

# %%
hdbg.init_logger(verbosity=logging.INFO)
log_level = logging.INFO

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# ## EDA
#
# Load a couple of files to have ~15 minutes worth of data

# %%
# After gunzip 1697760000001.gz
files = [
    "./1697760000001",                                                                                                           
    "./1697760036853",                                                                                                           
    "./1697760093027",                                                                                                           
    "./1697760146098",                                                                                                           
    "./1697760195389",                                                                                                           
    "./1697760216857",                                                                                                           
    "./1697760264748",                                                                                                           
    "./1697760316092",                                                                                                           
    "./1697760364434",                                                                                                           
    "./1697760396854",                                                                                                           
    "./1697760453532",                                                                                                           
    "./1697760512916",                                                                                                           
    "./1697760552803",                                                                                                           
    "./1697760576885",                                                                                                           
    "./1697760636132",                                                                                                           
    "./1697760688324",                                                                                                           
    "./1697760739299",                                                                                                           
    "./1697760756904",                                                                                                           
    "./1697760824041",                                                                                                           
    "./1697760890235",                                                                                                           
    "./1697760936888",
    "./1697760996328"
]
dfs = []
for file in files:
    df_ = pd.read_json(file, lines=True)
    dfs.append(df_)

df = pd.concat(dfs, axis=0)

# %%
df.head()

# %%
df.shape

# %%
df.t.nunique()

# %%
df.p.nunique()

# %% [markdown]
# It can happen that publish time of the message is not unique for simplicity, we drop
# those instances and set publish time as the index

# %%
df = df.drop_duplicates(subset=["p"])

# %%
df["p"] = pd.to_datetime(df['p'], unit="ms", utc=True)

# %%
df = df.set_index("p", drop=True)

# %% [markdown]
# ## Top of the book plot

# %%
df["top_bid"] = df["b"].map(lambda x: x[0])
df["top_ask"] = df["a"].map(lambda x: x[0])

# %%
df["top_bid_price"] = df["top_bid"].map(lambda x: x[0])
df["top_ask_price"] = df["top_ask"].map(lambda x: x[0])

# %%
df[["top_bid_price", "top_ask_price"]].plot()

# %% [markdown]
# ### Include OHLCV data in the plot

# %%
signature = "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7_4.ccxt.cryptocom.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
# We will be adjusting 
start_timestamp = df.index.min() - pd.Timedelta(minutes=2)
end_timestamp = df.index.max()
ohlcv_data = reader.read_data(start_timestamp, end_timestamp, currency_pairs=["BTC_USD"])
_LOG.log(log_level, hpandas.df_to_str(ohlcv_data, log_level=log_level))

# %%
df.head()

# %% [markdown]
# Add 1 minute to the index because crypto.com uses `b` to annotate interval `(a, b]`

# %%
ohlcv_data.index = ohlcv_data.index + pd.Timedelta(minutes=1)

# %%
ohlcv_data.head()

# %%
ohlcv_data = ohlcv_data[["high", "low"]]

# %%
ohlcv_orderbook = pd.concat([df, ohlcv_data]).sort_index().bfill()
ohlcv_orderbook[["top_ask_price", "top_bid_price", "high", "low"]].plot()

# %% [markdown]
# There are mild misalignments - we should investigate longer time interval but these might be possible/normal.

# %%
