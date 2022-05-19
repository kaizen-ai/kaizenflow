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

# %%
# %load_ext autoreload
# %autoreload 2


import logging
from datetime import timedelta
import datetime

import pandas as pd
import seaborn as sns

import core.plotting.normality as cplonorm
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import research_amp.transform as ramptran
import numpy as np

# %%
# Read saved 1 month of data.
ohlcv_cc = pd.read_csv("/shared_data/cc_ohlcv.csv", index_col="timestamp")
btc_ohlcv = ohlcv_cc[ohlcv_cc["full_symbol"] == "binance::BTC_USDT"]
btc_ohlcv.index = pd.to_datetime(btc_ohlcv.index)
ohlcv_cols = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "full_symbol",
]
btc_ohlcv = btc_ohlcv[ohlcv_cols]
btc_ohlcv.head(3)

# %%
# Read saved 1 month of data.
bid_ask_btc = pd.read_csv(
    "/shared_data/bid_ask_btc_jan22_1min_last.csv", index_col="timestamp"
)
bid_ask_btc.index = pd.to_datetime(bid_ask_btc.index)

# Transform the data.
bid_ask_btc.index = pd.to_datetime(bid_ask_btc.index)
# Compute bid ask stats.
bid_ask_btc = ramptran.calculate_bid_ask_statistics(bid_ask_btc)
# Choose only necessary values.
bid_ask_btc = bid_ask_btc.swaplevel(axis=1)["binance::BTC_USDT"][
    ["bid_size", "ask_size", "bid_price", "ask_price", "mid", "quoted_spread"]
]
bid_ask_btc.index = bid_ask_btc.index.shift(-1, freq="T")

bid_ask_btc.head(3)

# %%
# OHLCV + bid ask
btc = pd.concat([btc_ohlcv, bid_ask_btc], axis=1)
btc.head(3)


# %% [markdown]
# # Estimate intraday spread, volume

# %%
def get_real_spread(df: pd.DataFrame, time_and_date: pd.Timestamp):
    value = df["quoted_spread"].loc[time_and_date]
    return value

def get_real_volume(df: pd.DataFrame, time_and_date: pd.Timestamp):
    value = df["volume"].loc[date]
    return value


# %%
date = pd.Timestamp("2022-01-01 00:01", tz="UTC")
date

# %%
get_real_spread(btc, date)

# %%
get_real_volume(btc, date)


# %% [markdown]
# # Naive estimator

# %% [markdown]
# Value(t+2) = Value(t)

# %%
def get_naive_spread(df: pd.DataFrame, time_and_date: pd.Timestamp):
    naive_value_date = time_and_date - timedelta(minutes=2)
    real_spread = get_real_spread(df, naive_value_date)
    return real_spread

def get_naive_volume(df: pd.DataFrame, time_and_date: pd.Timestamp):
    naive_value_date = time_and_date - timedelta(minutes=2)
    real_spread = get_real_volume(df, naive_value_date)
    return real_spread


# %%
date = pd.Timestamp("2022-01-01 00:03", tz="UTC")
date

# %%
get_naive_spread(btc, date)

# %%
get_naive_volume(btc, date)

# %% [markdown]
# # Look back N days

# %%
btc["time"] = btc.index.time
btc.head(3)


# %%
def get_lookback_spread(df, time_and_date: pd.Timestamp, lookback_days, mode: str = "mean"):
    # Choose sample data using lookback period.
    start_date = time_and_date - timedelta(days=lookback_days)
    sample = df.loc[start_date : time_and_date]
    # Look for the reference value for the period.
    time_grouper = sample.groupby("time")
    if mode == "mean":
        grouped = time_grouper["quoted_spread"].mean()
    else:
        grouped = time_grouper["quoted_spread"].median()
    # Choose the value.
    lookback_spread = grouped[time_and_date.time()]
    return lookback_spread

def get_lookback_volume(df, time_and_date: pd.Timestamp, lookback_days, mode: str = "mean"):
    # Choose sample data using lookback period.
    start_date = time_and_date - timedelta(days=lookback_days)
    sample = df.loc[start_date : time_and_date]
    # Look for the reference value for the period.
    time_grouper = sample.groupby("time")
    if mode == "mean":
        grouped = time_grouper["volume"].mean()
    else:
        grouped = time_grouper["volume"].median()
    # Choose the value.
    lookback_spread = grouped[time_and_date.time()]
    return lookback_spread


# %%
date = pd.Timestamp("2022-01-21 19:59", tz="UTC")
date

# %%
get_lookback_spread(btc, date, 14)

# %%
get_lookback_volume(btc, date, 14)

# %% [markdown]
# # Collect estimators

# %%
estimators = pd.DataFrame(index=btc.index[1:])

# %%
estimators["real_spread"] = estimators.index
estimators["real_spread"] = estimators["real_spread"].apply(lambda x: get_real_spread(btc, x))

# %%
estimators["naive_spread"] = estimators.index
estimators["naive_spread"].iloc[2:] = estimators["naive_spread"].iloc[2:].apply(lambda x: get_naive_spread(btc, x))
estimators["naive_spread"].iloc[:2] = np.nan

# %%
start_date = pd.Timestamp("2022-01-15 00:00", tz="UTC")
lookback = 14
estimators["lookback_spread"] = estimators.index
estimators["lookback_spread"].loc[start_date:] = estimators["lookback_spread"].loc[start_date:].apply(lambda x: get_lookback_spread(btc, x, lookback))

# %% run_control={"marked": false}
estimators = estimators.copy()
estimators["lookback_spread"].loc[:start_date] = np.nan

# %% run_control={"marked": false}
estimators

# %%
## Evaluate results
dd = estimators[estimators["lookback_spread"].notna()]
dd["naive_spread"] = dd["naive_spread"].astype(float)
dd["lookback_spread"] = dd["lookback_spread"].astype(float)

# %%

# %%
Compare by asset (but also accumulate on all the coins):
- err(naive, spread) = avg(|spread_read(t) - spread_naive(t)|)
- err(lookback, spread) = avg(|spread_read(t) - spread_lookback(t)|)

# %%
dd

# %%
naive_err = abs(dd["real_spread"]-dd["naive_spread"]).sum()
lookback_err = abs(dd["real_spread"]-dd["lookback_spread"]).sum()
print(naive_err)
print(lookback_err)

# %%
dd[["real_spread", "lookback_spread"]].plot(figsize=(15,7))

# %%
