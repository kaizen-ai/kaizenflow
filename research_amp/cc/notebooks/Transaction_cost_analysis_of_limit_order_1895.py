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


from datetime import timedelta

import pandas as pd

import research_amp.transform as ramptran

# %% [markdown]
# # Load the data

# %% run_control={"marked": false}
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
]
btc_ohlcv = btc_ohlcv[ohlcv_cols]
btc_ohlcv.head(3)

# %%
# Read saved 1 month of data.
bid_ask_btc = pd.read_csv(
    "/shared_data/bid_ask_btc_jan22_1min.csv", index_col="timestamp"
)
bid_ask_btc.index = pd.to_datetime(bid_ask_btc.index)
bid_ask_btc = ramptran.calculate_bid_ask_statistics(bid_ask_btc)
bid_ask_btc = bid_ask_btc.swaplevel(axis=1)["binance::BTC_USDT"][
    ["bid_size", "ask_size", "bid_price", "ask_price", "mid", "quoted_spread"]
]
bid_ask_btc
bid_ask_btc.head(3)

# %%
# OHLCV + bid ask
btc = pd.concat([btc_ohlcv, bid_ask_btc], axis=1)
btc.head(3)

# %% [markdown]
# # Analysis

# %%
# Set limit price.
btc["limit_price"] = btc["ask_price"] - 0.01

# Use case for one N interval
N = 15
delta_N = timedelta(minutes=N)
start_time = pd.Timestamp("2022-01-01 10:00:00+00:00", tz="UTC")
end_time_N = start_time + delta_N
first_N_min = btc.loc[start_time:end_time_N]
M = 5
delta_M = timedelta(minutes=M)
end_time_M = start_time + delta_M
first_M_min = btc.loc[start_time:end_time_M]

# %%
# Execution is triggered if `limit_price` is inside [low,high] interval
first_M_min["execution_is_triggered"] = (
    first_M_min["high"] > first_M_min["limit_price"]
) & (first_M_min["low"] < first_M_min["limit_price"])
first_M_min

# %%
first_M_min["executed_volume"] = first_M_min["volume"] / (
    first_M_min["high"] - first_M_min["low"]
)
first_M_min

# %%
M_summary = first_M_min[first_M_min["execution_is_triggered"] == True][
    ["limit_price", "executed_volume"]
]
M_summary

# %%
execution_value = (M_summary["limit_price"] * M_summary["executed_volume"]).sum()
execution_value

# %%
holding_value = first_N_min.iloc[-1]["close"] * (
    M_summary["executed_volume"].sum()
)
holding_value
