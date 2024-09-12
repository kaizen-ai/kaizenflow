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
# # Master raw data gallery

# %% [markdown]
# This notebook showcases locations and basic structure of raw data from:
#
# - S3 (parquet datasets)
# - IM DB (Postgres)
#
# The secondary purpose is to provide a guide on how to use `RawDataReader`
#
# ## Specs
# - This notebook:
#   - is a catalog of all the datasets that exist in the our system
#   - shows how to load data using our low-level functions or specific API for specific datasets
#   - shows how a snippet of the data looks like (for this we want to load the minimal amount of data)
#   - doesn't compute any statistics
#   - should be quickly to execute, like < 1min, so we can run it in the unit tests
#
# ## Life cycle
# - Any time a new dataset is added (e.g., in real-time DB, Parquet) we add some information on how to load it and how it looks like
# - In general we try not to delete any data but we only add data loaders
#
# ## Monster dataset matrix spreadsheet
#
# The gallery should match 1-to-1 with the dataset matrix
#
# https://docs.google.com/spreadsheets/d/13Vyrxs9Eg-C6y91XIogLHi4A1_AFK7_KCF2KEnnxYv0/edit#gid=521856766

# %% [markdown]
# # Imports

# %%
import datetime
import logging

import pandas as pd

import helpers.hdbg as hdbg
import helpers.henv as henv
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
# # Active datasets

# %% [markdown]
# ## realtime.airflow.resampled_1min.postgres.bid_ask.futures.v8.ccxt.binance.v1_0_0

# %%
signature = "realtime.airflow.resampled_1min.postgres.bid_ask.futures.v8.ccxt.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## realtime.airflow.downloaded_200ms.postgres.bid_ask.futures.v8.ccxt.binance.v1_0_0

# %%
signature = "realtime.airflow.downloaded_200ms.postgres.bid_ask.futures.v8.ccxt.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## realtime.airflow.downloaded_200ms.postgres.bid_ask.futures.v8.binance.binance.v1_0_0

# %%
signature = "realtime.airflow.downloaded_200ms.postgres.bid_ask.futures.v8.binance.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## realtime.airflow.resampled_1min.postgres.bid_ask.futures.v8.binance.binance.v1_0_0

# %%
signature = "realtime.airflow.resampled_1min.postgres.bid_ask.futures.v8.binance.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.archived_200ms.parquet.bid_ask.futures.v8.ccxt.binance.v1_0_0

# %%
# TODO(Juraj): #CmTask8309

# %% [markdown]
# ## periodic_daily.airflow.resampled_1min.parquet.bid_ask.futures.v8.ccxt.binance.v1_0_0

# %%
# TODO(Juraj): #CmTask8309

# %% [markdown]
# ## periodic_daily.airflow.archived_200ms.parquet.bid_ask.futures.v8.binance.binance.v1_0_0

# %%
signature = "periodic_daily.airflow.archived_200ms.parquet.bid_ask.futures.v8.binance.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
start_timestamp = pd.Timestamp("2024-05-16T00:00:00+00:00")
end_timestamp = start_timestamp + pd.Timedelta(minutes=2)
data = reader.read_data(
    start_timestamp, end_timestamp, currency_pairs=["BTC_USDT", "ETH_USDT"]
)
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## realtime.airflow.downloaded_1min.postgres.ohlcv.futures.v8.ccxt.binance.v1_0_0

# %%
signature = "realtime.airflow.downloaded_1min.postgres.ohlcv.futures.v8.ccxt.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown] run_control={"marked": false}
# ## periodic_daily.airflow.downloaded_all.parquet.trades.futures.v8.binance.binance.v2_0_0

# %%
signature = "periodic_daily.airflow.downloaded_all.parquet.trades.futures.v8.binance.binance.v2_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
start_timestamp = pd.Timestamp("2024-01-01T00:00:00+00:00")
end_timestamp = start_timestamp + pd.Timedelta(minutes=2)
data = reader.read_data(
    start_timestamp, end_timestamp, currency_pairs=["BTC_USDT"]
)
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %%
## TODO(Sonaal): Add info about bid/ask historical data

# %% [markdown]
# ## realtime.airflow.downloaded_200ms.postgres.bid_ask.spot.v7_3.ccxt.binance.v1_0_0

# %%
signature = "realtime.airflow.downloaded_200ms.postgres.bid_ask.spot.v7_3.ccxt.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.archived_200ms.postgres.bid_ask.spot.v7.ccxt.binance.v1_0_0

# %%
signature = "periodic_daily.airflow.archived_200ms.postgres.bid_ask.spot.v7.ccxt.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## realtime.airflow.downloaded_200ms.postgres.bid_ask.spot.v7_3.ccxt.okx.v1_0_0

# %%
signature = (
    "realtime.airflow.downloaded_200ms.postgres.bid_ask.spot.v7_3.ccxt.okx.v1_0_0"
)
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.archived_200ms.postgres.bid_ask.spot.v7_3.ccxt.okx.v1_0_0

# %%
signature = "periodic_daily.airflow.archived_200ms.postgres.bid_ask.spot.v7_3.ccxt.okx.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## realtime.airflow.downloaded_200ms.postgres.bid_ask.futures.v7_4.ccxt.cryptocom.v1_0_0

# %%
signature = "realtime.airflow.downloaded_200ms.postgres.bid_ask.futures.v7_4.ccxt.cryptocom.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.archived_200ms.postgres.bid_ask.futures.v7_4.ccxt.cryptocom.v1_0_0

# %%
signature = "periodic_daily.airflow.archived_200ms.postgres.bid_ask.futures.v7_4.ccxt.cryptocom.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.downloaded_1min.parquet.ohlcv.spot.v7_3.ccxt.binance.v1_0_0

# %%
signature = "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.spot.v7_3.ccxt.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7_3.ccxt.binance.v1_0_0

# %%
signature = "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.spot.v7_3.ccxt.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v8.ccxt.binance.v1_0_0

# %%
signature = "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v8.ccxt.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
start_timestamp = pd.Timestamp("2024-01-01T00:00:00+00:00")
end_timestamp = start_timestamp + pd.Timedelta(minutes=5)
data = reader.read_data(
    start_timestamp, end_timestamp, currency_pairs=["BTC_USDT", "ETH_USDT"]
)
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7_4.ccxt.cryptocom.v1_0_0

# %%
signature = "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7_4.ccxt.cryptocom.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v8.ccxt.okx.v1_0_0

# %%
signature = "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v8.ccxt.okx.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
start_timestamp = pd.Timestamp("2024-01-01T00:00:00+00:00")
end_timestamp = start_timestamp + pd.Timedelta(minutes=5)
data = reader.read_data(
    start_timestamp, end_timestamp, currency_pairs=["BTC_USDT", "ETH_USDT"]
)
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.resampled_1min.parquet.bid_ask.futures.v8.ccxt.binance.v2_0_0

# %%
signature = "periodic_daily.airflow.resampled_1min.parquet.bid_ask.futures.v8.ccxt.binance.v2_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod", add_suffix="tokyo")
start_timestamp = pd.Timestamp("2024-06-11T00:00:00+00:00")
end_timestamp = start_timestamp + pd.Timedelta(minutes=5)
data = reader.read_data(
    start_timestamp, end_timestamp, currency_pairs=["XMR_USDT"]
)
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %%
# ## periodic_daily.airflow.downloaded_200ms.postgres.bid_ask.futures.v7_5.ccxt.cryptocom.v1_0_0
# TODO(Juraj): The downloader for this dataset runs in Tokyo, we do not have cross-region RDS access available, should
# it be added?

# %% [markdown]
# ## periodic_daily.airflow.archived_200ms.parquet.bid_ask.futures.v7_5.ccxt.cryptocom.v1_0_0

# %%
signature = "periodic_daily.airflow.archived_200ms.parquet.bid_ask.futures.v7_5.ccxt.cryptocom.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod", add_suffix="tokyo")
start_timestamp = pd.Timestamp("2024-07-09T06:00:00+00:00")
end_timestamp = start_timestamp + pd.Timedelta(minutes=1)
data = reader.read_data(
    start_timestamp, end_timestamp
)
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.resampled_1min.parquet.bid_ask.futures.v7_5.ccxt.cryptocom.v2_0_0

# %%
signature = "periodic_daily.airflow.resampled_1min.parquet.bid_ask.futures.v7_5.ccxt.cryptocom.v2_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod", add_suffix="tokyo")
start_timestamp = pd.Timestamp("2024-07-08T20:00:00+00:00")
end_timestamp = start_timestamp + pd.Timedelta(minutes=5)
data = reader.read_data(
    start_timestamp, end_timestamp
)
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# # Semi-Active datasets
#
#  - not currently downloaded but could be revived

# %% [markdown]
# ## periodic_daily.airflow.archived_200ms.parquet.bid_ask.spot.v7_4.ccxt.kraken.v1_0_0

# %%
signature = "periodic_daily.airflow.archived_200ms.parquet.bid_ask.spot.v7_4.ccxt.kraken.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
start_timestamp = pd.Timestamp("2024-01-01T00:00:00+00:00")
end_timestamp = start_timestamp + pd.Timedelta(minutes=1)
data = reader.read_data(
    start_timestamp, end_timestamp, currency_pairs=["BTC_USDT", "ETH_USDT"]
)
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.downloaded_all.parquet.trades.spot.v7_4.ccxt.kraken.v1_0_0

# %%
signature = "periodic_daily.airflow.downloaded_all.parquet.trades.spot.v7_4.ccxt.kraken.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
start_timestamp = pd.Timestamp("2024-01-01T00:00:00+00:00")
end_timestamp = start_timestamp + pd.Timedelta(minutes=1)
data = reader.read_data(
    start_timestamp, end_timestamp, currency_pairs=["BTC_USDT", "ETH_USDT"]
)
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# # Legacy datasets - datasets not actively maintained

# %% [markdown]
# ## periodic_daily.airflow.downloaded_1min.csv.ohlcv.futures.v7.ccxt.binance.v1_0_0

# %%
#  the dataset reside under previous, deprecated schema:
# s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/

# signature = "periodic_daily.airflow.downloaded_1min.csv.ohlcv.futures.v7.ccxt.binance.v1_0_0"
# reader = imvcdcimrdc.RawDataReader(signature)
# data = reader.read_data_head()
# _LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7.ccxt.binance.v1_0_0

# %%
signature = "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7.ccxt.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature)
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.downloaded_1min.parquet.ohlcv.spot.v7.ccxt.binance.v1_0_0

# %%
signature = "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7.ccxt.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature)
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.downloaded_1min.parquet.ohlcv.spot.v7.ccxt.binanceus.v1_0_0

# %% run_control={"marked": true}
signature = "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.spot.v7.ccxt.binanceus.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature)
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v3.crypto_chassis.binance.v1_0_0

# %%
signature = "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v3.crypto_chassis.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature)
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.downloaded_1sec.parquet.bid_ask.futures.v3.crypto_chassis.binance.v1_0_0

# %%
signature = "periodic_daily.airflow.downloaded_1sec.parquet.bid_ask.futures.v3.crypto_chassis.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature)
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.resampled_1min.parquet.bid_ask.futures.v3.crypto_chassis.binance.v1_0_0

# %%
signature = "periodic_daily.airflow.resampled_1min.parquet.bid_ask.futures.v3.crypto_chassis.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature)
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.downloaded_1sec.parquet.bid_ask.spot.v3.crypto_chassis.binance.v1_0_0

# %%
# The dataset reside under previous schema:
# s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/

# TODO(Juraj): Spot bid ask spot data are not collected currently
# signature = "periodic_daily.airflow.downloaded_1sec.parquet.bid_ask.spot.v3.crypto_chassis.binance.v1_0_0"
# reader = imvcdcimrdc.RawDataReader(signature)
# data = reader.read_data_head()
# _LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.resampled_1min.parquet.bid_ask.spot.v3.crypto_chassis.binance.v1_0_0

# %%
# The dataset reside under previous schema:
# s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/

# TODO(Juraj): Spot bid ask spot data are not collected currently
# signature = "periodic_daily.airflow.downloaded_1sec.parquet.bid_ask.spot.v3.crypto_chassis.binance.v1_0_0"
# reader = imvcdcimrdc.RawDataReader(signature)
# data = reader.read_data_head()
# _LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.downloaded_1min.parquet.ohlcv.spot.v3.crypto_chassis.binance.v1_0_0

# %%
signature = "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.spot.v3.crypto_chassis.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature)
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.downloaded_1sec.parquet.trades.futures.v3_1.crypto_chassis.binance.v1_0_0

# %%
signature = "periodic_daily.airflow.downloaded_1sec.parquet.trades.futures.v3_1.crypto_chassis.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature)
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7_3.ccxt.okx.v1_0_0

# %%
signature = "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7_3.ccxt.okx.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature)
data = reader.read_data_head()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## bulk.airflow.downloaded_1min.parquet.ohlcv.futures.v7_5.ccxt.binance.v1_0_0
#
# _This dataset is in the test stage only_

# %%
signature = (
    "bulk.airflow.downloaded_1min.parquet.ohlcv.futures.v7_5.ccxt.binance.v1_0_0"
)
reader = imvcdcimrdc.RawDataReader(signature, stage="test")
# 4 months of data is available.
start_timestamp = pd.Timestamp("2023-02-01T00:00:00+00:00")
end_timestamp = pd.Timestamp("2023-06-01T00:00:00+00:00")
binance_ohlcv_data = reader.read_data(start_timestamp, end_timestamp)
_LOG.log(
    log_level, hpandas.df_to_str(binance_ohlcv_data.head(), log_level=log_level)
)

# %% [markdown]
# # RawDataReader Guide

# %% [markdown]
# ## Loading parquet data with filters
#
# TODO(Juraj): Support for filtering by level for parquet bid/ask datasets will be added once
# #3694 is finished.

# %%
signature = "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7.ccxt.binance.v1_0_0"
start_timestamp = pd.Timestamp(
    datetime.datetime.utcnow() - datetime.timedelta(minutes=10, days=2), tz="UTC"
)
end_timestamp = start_timestamp + datetime.timedelta(minutes=10)
currency_pairs = ["BTC_USDT", "ETH_USDT"]
reader = imvcdcimrdc.RawDataReader(signature)
data = reader.read_data(
    start_timestamp, end_timestamp, currency_pairs=currency_pairs
)
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %%
# This works with stage preprod.
signature = "periodic_daily.airflow.downloaded_1min.parquet.ohlcv.futures.v7_6.ccxt.okx.v1_0_0"
start_timestamp = pd.Timestamp(
    datetime.datetime.utcnow() - datetime.timedelta(minutes=10, days=1), tz="UTC"
)
end_timestamp = start_timestamp + datetime.timedelta(minutes=10)
currency_pairs = ["BTC_USDT", "ETH_USDT"]
reader = imvcdcimrdc.RawDataReader(signature, stage="preprod")
data = reader.read_data(
    start_timestamp, end_timestamp, currency_pairs=currency_pairs
)
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## Loading postgres data with filters

# %%
signature = "realtime.airflow.resampled_1min.postgres.bid_ask.futures.v7.ccxt.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature)
start_timestamp = pd.Timestamp(
    datetime.datetime.utcnow() - datetime.timedelta(minutes=10, days=2), tz="UTC"
)
end_timestamp = start_timestamp + datetime.timedelta(minutes=10)
currency_pairs = ["BTC_USDT", "ETH_USDT"]
bid_ask_levels = [1, 2]
data = reader.read_data(
    start_timestamp,
    end_timestamp,
    currency_pairs=currency_pairs,
    bid_ask_levels=bid_ask_levels,
)
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))
