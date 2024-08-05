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
# # Compare crypto.com historical data and our realtime archived data
#
# This notebook builds upon previous work. For details on the historical data source refer to `im_v2/common/notebooks/CmampTask8547_Short_EDA_on_crypto.com_bidask_historical_data.ipynb`
# - the dataset doesn't have a signature yet, we only have snippets of data, the epic to on-board the data is https://github.com/cryptokaizen/cmamp/issues/8520
#
# Realtime archived data comes from our downloaders, dataset signature:
# `periodic_daily.airflow.archived_200ms.parquet.bid_ask.futures.v7_4.ccxt.cryptocom.v1_0_0`

# %%
# %load_ext autoreload
# %autoreload 2

import logging
import numpy as np
import glob
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import im_v2.common.data.client.im_raw_data_client as imvcdcimrdc
import core.finance.resampling as cfinresa

# %%
hdbg.init_logger(verbosity=logging.INFO)
log_level = logging.INFO

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# ## Load Historical Data

# %% [markdown]
# Snippet of ~10 minutes of data ata for June 23th ~3:00PM

# %%
# ! ls /shared_data/CmTask8608

# %%
glob.glob("/shared_data/CmTask8608/*")

# %%
dfs = []
for file in glob.glob("/shared_data/CmTask8608/*"):
    df_ = pd.read_json(file, lines=True)
    dfs.append(df_)

df = pd.concat(dfs, axis=0)

# %%
df = df.drop_duplicates(subset=["p"])

# %% [markdown]
# - We have confirmation from CCXT discord that the timestamp used by CCXT here https://github.com/ccxt/ccxt/blob/1cca6b0883a0e471fede443ebf8501601e40836a/python/ccxt/pro/cryptocom.py#L208 is the time of message publish, AKA
# 't' field from https://exchange-docs.crypto.com/exchange/v1/rest-ws/index.html#book-instrument_name-depth
#
# - We have confirmation from telegram that "p" field in the historical data also corresponds to the publish time

# %%
df["p"] = pd.to_datetime(df["p"], unit="ms", utc=True)

# %%
historical_df = df.set_index("p", drop=True)

# %%
historical_df.index.min()

# %% [markdown]
# Get top of the book data

# %%
historical_df[["bid_price", "bid_size"]] = historical_df["b"].map(lambda x: x[0]).apply(pd.Series)
historical_df[["ask_price", "ask_size"]] = historical_df["a"].map(lambda x: x[0]).apply(pd.Series)

# %%
historical_df.index.name = "timestamp"

# %% [markdown]
# ## Load our data

# %%
signature = "periodic_daily.airflow.archived_200ms.parquet.bid_ask.futures.v7_4.ccxt.cryptocom.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
start_timestamp = historical_df.index.min() - pd.Timedelta(minutes=1)
end_timestamp = historical_df.index.max() + - pd.Timedelta(minutes=1)
archived_data = reader.read_data(start_timestamp, end_timestamp, currency_pairs=["BTC_USD"])
_LOG.log(log_level, hpandas.df_to_str(archived_data, log_level=log_level))

# %%
archived_data = archived_data[archived_data.level == 1].drop("timestamp", axis=1)

# %%
merged_df = pd.merge(historical_df, archived_data, on='timestamp', suffixes=('_historical', '_rt_archived'))

# Calculate the deviation percentage for each column
for column in ['bid_size', 'bid_price', 'ask_size', 'ask_price']:
    merged_df[f'{column}_deviation'] = abs(merged_df[f'{column}_historical'] - merged_df[f'{column}_rt_archived'])

# %%
merged_df.shape

# %%
merged_df[
    ["bid_size_deviation", "bid_price_deviation", "ask_size_deviation", "ask_price_deviation"]
].describe()

# %% [markdown]
# Conclusion, we have very small overlap of the timestamps between the datasets, which is surprising given the fact
# that both datasets should be snapshots using the same timestamp semantics - the message publish time

# %% [markdown]
# ### Align both datasets to 100ms grid
#
# - Choosing very conservative forward filling

# %%
historical_df_100ms_grid = cfinresa.resample(historical_df, rule="100ms").last().ffill(limit=10)

# %%
archived_data_100ms_grid = cfinresa.resample(archived_data, rule="100ms").last().ffill(limit=10)

# %%
merged_df = pd.merge(historical_df_100ms_grid, archived_data_100ms_grid, on='timestamp', suffixes=('_historical', '_rt_archived'))

# Calculate the deviation percentage for each column
for column in ['bid_size', 'bid_price', 'ask_size', 'ask_price']:
    merged_df[f'{column}_deviation'] = abs(merged_df[f'{column}_historical'] - merged_df[f'{column}_rt_archived'])

# %%
merged_df.shape

# %%
merged_df[
    ["bid_size_deviation", "bid_price_deviation", "ask_size_deviation", "ask_price_deviation"]
].describe()

# %% [markdown]
# After aligning on a grid the results are very encouraging, we see very close match at the top of the book
# for all levels

# %%
