# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Imports

# %% run_control={"marked": false}
import logging
import os
from datetime import timedelta

import pandas as pd
import requests

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import im_v2.ccxt.data.client as icdcl
import im_v2.ccxt.data.extract.exchange_class as imvcdeexcl

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()

# %% [markdown]
# ## CCXT Historical Client

# %%
# Specify params.
resample_1min = True
root_dir = os.path.join("s3://cryptokaizen-data", "historical")
partition_mode = "by_year_month"
data_snapshot = "latest"
aws_profile = "ck"

# Initiate the client.
historical_client = icdcl.CcxtHistoricalPqByTileClient(
    resample_1min,
    root_dir,
    partition_mode,
    data_snapshot=data_snapshot,
    aws_profile=aws_profile,
)


# %% [markdown]
# ## Crypto-chassis Historical

# %%
def load_crypto_chassis_ohlcv(exhange_id, currency_pair):
    r = requests.get(
        f"https://api.cryptochassis.com/v1/ohlc/{exhange_id}/{currency_pair}?startTime=0"
    )
    df = pd.read_csv(r.json()["historical"]["urls"][0]["url"], compression="gzip")
    df["time_seconds"] = df["time_seconds"].apply(
        lambda x: hdateti.convert_unix_epoch_to_timestamp(x, unit="s")
    )
    df = df.set_index("time_seconds")
    return df


# %% [markdown]
# # `Kucoin` case

# %% [markdown]
# ## Example that the data is available in other sources

# %% [markdown]
# ### `full_symbol` = kucoin::ADA_USDT

# %% [markdown]
# #### Load the data

# %%
# Load historical data from CCXT.
full_symbols = ["kucoin::ADA_USDT"]
start_date = None
end_date = None
ada_kucoin_ccxt = historical_client.read_data(full_symbols, start_date, end_date)
# Load historical data from crypto-chassis.
ada_kucoin_ch = load_crypto_chassis_ohlcv("kucoin", "ada-usdt")

# %% [markdown]
# #### Take a specific period of time - [2022-02-09 00:00, 2022-02-09 13:33]

# %%
ccxt_ada_nans = ada_kucoin_ccxt.loc[
    (ada_kucoin_ccxt.index > "2022-02-09 00:00:00+00:00")
    & (ada_kucoin_ccxt.index < "2022-02-09 13:33:00+00:00")
]
chassis_ada = ada_kucoin_ch.loc[
    (ada_kucoin_ch.index > "2022-02-09 00:00:00+00:00")
    & (ada_kucoin_ch.index < "2022-02-09 13:33:00+00:00")
]
# Check that the lenght is identical.
print(
    f"Both datasets have the same length: {len(ccxt_ada_nans)==len(chassis_ada)}"
)

# %% [markdown]
# #### Data snapshot

# %%
print("Period of NaNs in CCXT:")
display(ccxt_ada_nans)
print("Complete data in chassis:")
display(chassis_ada)

# %%
print(
    f"Percentage of NaNs in CCXT data for the period: {len(ccxt_ada_nans[ccxt_ada_nans.open.isna()])*100/len(ccxt_ada_nans)}"
)
print(
    f"Percentage of NaNs in crypto-chassis data for the period: {len(chassis_ada[chassis_ada.open.isna()])*100/len(chassis_ada)}"
)

# %% [markdown]
# ### `full_symbol` = kucoin::BTC_USDT

# %% [markdown]
# This full symbol has the highest coverage according to https://github.com/cryptokaizen/cmamp/issues/1750#issuecomment-1106866542

# %% [markdown]
# #### Load the data

# %%
# Load historical data from CCXT.
full_symbols = ["kucoin::BTC_USDT"]
start_date = None
end_date = None
btc_kucoin_ccxt = historical_client.read_data(full_symbols, start_date, end_date)
# Load historical data from crypto-chassis.
btc_kucoin_ch = load_crypto_chassis_ohlcv("kucoin", "btc-usdt")

# %% [markdown]
# #### Take a specific period of time - a year 2021

# %%
ccxt_btc = btc_kucoin_ccxt.loc[
    (btc_kucoin_ccxt.index > "2021-08-07")
    & (btc_kucoin_ccxt.index < "2021-12-31")
]
chassis_btc = btc_kucoin_ch.loc[
    (btc_kucoin_ch.index > "2021-08-07") & (btc_kucoin_ch.index < "2021-12-31")
]

# %%
# From crypto-chassis docs:
# If there is a gap in "time_seconds", it means that
# the market depth snapshot at that moment is the same as the previous moment.
# It means that NaNs there are 'gaps' between timestamps.
chassis_btc["timestamp_diff"] = chassis_btc.index.to_series().diff()
# Exclude 1min gaps (means no NaNs) and calculate the num of mins in gaps (equals to num of NaNs).
chassis_btc[chassis_btc["timestamp_diff"] != "0 days 00:01:00"]
# Calculate the total rows with NaNs.
num_of_nans_chassis = chassis_btc["timestamp_diff"].sum() / timedelta(minutes=1)

# %%
print(
    f"NaNs percentage in CCXT for `kucoin::BTC_USDT` for 2021: {100*len(ccxt_btc[ccxt_btc.open.isna()])/len(ccxt_btc)}"
)
print(
    f"NaNs percentage in crypto-chassis for `kucoin::BTC_USDT` for 2021: {100*num_of_nans_chassis/(len(chassis_btc)+num_of_nans_chassis)}"
)

# %% [markdown]
# #### Take a specific period of time - [2022-01-15 12:00, 2022-01-15 21:00]

# %%
ccxt_btc2022 = btc_kucoin_ccxt.loc[
    (btc_kucoin_ccxt.index > "2022-01-15 12:00:00")
    & (btc_kucoin_ccxt.index < "2022-01-15 21:00:00")
]
chassis_btc2022 = btc_kucoin_ch.loc[
    (btc_kucoin_ch.index > "2022-01-15 12:00:00")
    & (btc_kucoin_ch.index < "2022-01-15 21:00:00")
]
print(
    f"Both datasets have the same length: {len(ccxt_btc2022)==len(chassis_btc2022)}"
)

# %%
print("Period of NaNs in CCXT:")
display(ccxt_btc2022)
print("Complete data in chassis:")
display(chassis_btc2022)

# %% [markdown]
# ### Summary

# %% [markdown]
# From what we can see in comparison with other data vendors, __the problem of data coverage has little to do with the initial data provider__ (i.e., `kucoin`), since the NaN data in `CCXT` is available in other providers (`crypto-chassis`)

# %% [markdown]
# ## Look at S3

# %%
# Take the previously spotted NaN sequence.
ccxt_ada_nans = ada_kucoin_ccxt.loc[
    (ada_kucoin_ccxt.index > "2022-02-01 00:00:00+00:00")
    & (ada_kucoin_ccxt.index < "2022-02-09 13:33:00+00:00")
]
ccxt_ada_nans

# %%
print(
    f"Percentage of NaN data: {100*len(ccxt_ada_nans[ccxt_ada_nans.open.isna()])/len(ccxt_ada_nans)}"
)

# %% [markdown]
# S3 has the only file in the February for `kucoin::ADA_USDT`:
#
# ![%D0%A1%D0%BD%D0%B8%D0%BC%D0%BE%D0%BA%20%D1%8D%D0%BA%D1%80%D0%B0%D0%BD%D0%B0%202022-04-26%20%D0%B2%2019.14.16.png](attachment:%D0%A1%D0%BD%D0%B8%D0%BC%D0%BE%D0%BA%20%D1%8D%D0%BA%D1%80%D0%B0%D0%BD%D0%B0%202022-04-26%20%D0%B2%2019.14.16.png)

# %%
file_path = "s3://cryptokaizen-data/historical/ccxt/latest/kucoin/currency_pair=ADA_USDT/year=2022/month=2/d5ea82924aa046f59a559985a95ea1c9.parquet"
pq_ada = pd.read_parquet(file_path)
pq_ada


# %% [markdown]
# One can notice that the data for February starts exactly from the point where NaN sequence is ending.

# %% [markdown]
# ### Summary

# %% [markdown]
# The raw data doesn't have include the data for NaN sequences in client which means that with great probability, __the client doesn't distorts the raw data and it comes initially with these NaN sequences__.

# %% [markdown]
# # Load CCXT data using requests from CCXT

# %% [markdown]
# ## Function

# %%
def timestamp_to_datetime(timestamps):
    times = []
    for time in timestamps:
        times.append(hdateti.convert_unix_epoch_to_timestamp(time))
    return times


# %% [markdown]
# ## Load data

# %%
ccxt_kucoin_ada_exchange = imvcdeexcl.CcxtExchange("kucoin")
ccxt_kucoin_ada_data = ccxt_kucoin_ada_exchange.download_ohlcv_data("ADA/USDT")

# %%
_LOG.info(ccxt_kucoin_ada_data.shape)
ccxt_kucoin_ada_data.head()

# %% [markdown]
# ## Set Datetime index to loaded data

# %%
indexes = timestamp_to_datetime(ccxt_kucoin_ada_data['timestamp'])
ccxt_kucoin_ada_data.set_index(pd.to_datetime(indexes), inplace=True)
ccxt_kucoin_ada_data.head(2)

# %% [markdown]
# ## Take a specific period of time - ["2019-02-18 00:00:00+00:00"]

# %%
ccxt_ada_nans = ada_kucoin_ccxt.loc[
    (ada_kucoin_ccxt.index > "2019-02-18 00:00:00+00:00")
    & (ada_kucoin_ccxt["open"].isna == True)
]
ccxt_exchange_ada = ccxt_kucoin_ada_data.loc[
    (ccxt_kucoin_ada_data.index > "2019-02-18 00:00:00+00:00")
]
# Check that the lenght is identical.
print(
    f"Both datasets have the same length: {len(ccxt_ada_nans)==len(ccxt_exchange_ada)}"
)

# %%
print("Period of NaNs in CCXT:")
_LOG.info(ccxt_ada_nans.shape)
display(ccxt_ada_nans.head())
print("\n******************************************\n")
print("Complete data:")
_LOG.info(ccxt_exchange_ada.shape)
display(ccxt_exchange_ada.head())

# %%
ccxt_ada_nans = ada_kucoin_ccxt.loc[
    (ada_kucoin_ccxt.index < "2019-02-18 00:00:00+00:00")
    & (ada_kucoin_ccxt["open"].isna == True)
]
ccxt_exchange_ada = ccxt_kucoin_ada_data.loc[
    (ccxt_kucoin_ada_data.index < "2019-02-18 00:00:00+00:00")
]
# Check that the lenght is identical.
print(
    f"Both datasets have the same length: {len(ccxt_ada_nans)==len(ccxt_exchange_ada)}"
)

# %%
print("Period of NaNs in CCXT:")
_LOG.info(ccxt_ada_nans.shape)
display(ccxt_ada_nans.head())
print("\n******************************************\n")
print("Complete data:")
_LOG.info(ccxt_exchange_ada.shape)
display(ccxt_exchange_ada.head())

# %%
print(f"Start: {ccxt_kucoin_ada_data.index.min()}\nEnd: {ccxt_kucoin_ada_data.index.max()}")
