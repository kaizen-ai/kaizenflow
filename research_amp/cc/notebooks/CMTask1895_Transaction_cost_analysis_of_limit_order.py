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
import seaborn as sns

import core.plotting.normality as cplonorm
import research_amp.transform as ramptran
import research_amp.cc.crypto_chassis_api as raccchap

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
#bid_ask_btc = pd.read_csv(
#    "/shared_data/bid_ask_btc_jan22_1min.csv", index_col="timestamp"
#)
#bid_ask_btc.index = pd.to_datetime(bid_ask_btc.index)

# Read from crypto_chassis directly.
# Specify the params.
full_symbols = ["binance::BTC_USDT"]
start_date = pd.Timestamp("2022-01-01", tz="UTC")
end_date = pd.Timestamp("2022-02-01", tz="UTC")
# Get the data.
bid_ask_df = raccchap.read_and_resample_bid_ask_data(
   full_symbols, start_date, end_date, "1T"
)
bid_ask_df.head(3)

# %%
# Transform the data.
bid_ask_df.index = pd.to_datetime(bid_ask_df.index)
# Compute bid ask stats.
bid_ask_btc = ramptran.calculate_bid_ask_statistics(bid_ask_df)
# Choose only necessary values.
bid_ask_btc = bid_ask_btc.swaplevel(axis=1)["binance::BTC_USDT"][
    ["bid_size", "ask_size", "bid_price", "ask_price", "mid", "quoted_spread"]
]
bid_ask_btc.index = bid_ask_btc.index.shift(-1)
bid_ask_btc

# %%
# OHLCV + bid ask
btc = pd.concat([btc_ohlcv, bid_ask_btc], axis=1)
btc.head(3)

# %% run_control={"marked": false}
close_outside_ba = len(btc.loc[(btc["close"]>btc["ask_price"])|(btc["close"]<btc["bid_price"])])
print(f"Share of close prices outside bid-ask spread: %.3f" % (close_outside_ba/len(btc)))
btc[["close", "bid_price", "ask_price"]].head(100).plot(figsize=(15,7))

# %% [markdown]
# # Analysis

# %%
# Set limit price.
btc["limit_price"] = btc["ask_price"] - 0.01

# %% [markdown]
# ## Use case for one interval

# %%
# Use case for one N interval
N = 15
delta_N = timedelta(minutes=N)
start_time = pd.Timestamp("2022-01-01 09:59:00+00:00", tz="UTC")
end_time_N = start_time + delta_N
first_N_min = btc.loc[start_time:end_time_N]
M = 5
delta_M = timedelta(minutes=M)
end_time_M = start_time + delta_M
first_M_min = btc.loc[start_time:end_time_M]

# %%
# Execution is triggered if `limit_price` is inside [low,high] interval.
first_M_min = first_M_min.copy()
first_M_min["execution_is_triggered"] = (
    first_M_min["high"] > first_M_min["limit_price"]
) & (first_M_min["low"] < first_M_min["limit_price"])
# Calculate execution volume.
first_M_min["executed_volume"] = first_M_min["volume"] / (
    first_M_min["high"] - first_M_min["low"]
)
first_M_min

# Executed deals.
M_summary = first_M_min[first_M_min["execution_is_triggered"] == True][
    ["limit_price", "executed_volume"]
]
M_summary

# %%
execution_value_1p = (
    M_summary["limit_price"] * M_summary["executed_volume"]
).sum()
execution_value_1p

# %%
holding_value_1p = first_N_min.iloc[-1]["close"] * (
    M_summary["executed_volume"].sum()
)
holding_value_1p

# %% [markdown]
# ## Extend the analysis for the whole period

# %%
num_intervals = (btc.index.max() - btc.index.min()).total_seconds() / (15 * 60)
num_intervals = range(int(num_intervals))
num_intervals

# %%
N = 15
M = 5

start_time = btc.index.min()
delta_N = timedelta(minutes=N)
delta_M = timedelta(minutes=M)

results = pd.DataFrame()

for i in num_intervals:
    # Construct periods.
    end_time_N = start_time + delta_N
    end_time_M = start_time + delta_M
    N_min = btc.loc[start_time:end_time_N]
    M_min = btc.loc[start_time:end_time_M]
    # Execution is triggered if `limit_price` is inside [low,high] interval.
    first_M_min = M_min.copy()
    first_M_min["execution_is_triggered"] = (
        first_M_min["high"] > first_M_min["limit_price"]
    ) & (first_M_min["low"] < first_M_min["limit_price"])
    # Calculate execution volume.
    first_M_min["executed_volume"] = first_M_min["volume"] / (
        first_M_min["high"] - first_M_min["low"]
    )
    # Executed deals.
    M_summary = first_M_min[first_M_min["execution_is_triggered"] == True][
        ["limit_price", "executed_volume"]
    ]
    execution_value = (
        M_summary["limit_price"] * M_summary["executed_volume"]
    ).sum()
    holding_value = N_min.iloc[-1]["close"] * (M_summary["executed_volume"].sum())
    # Collect the results.
    results.loc[i, "start_period"] = start_time
    results.loc[i, "end_period"] = end_time_N
    results.loc[i, f"execution_value_first_{M}_mins"] = execution_value
    results.loc[i, f"holding_value_at_the_end_of_{N}_min"] = holding_value
    # New start time.
    start_time = start_time + delta_N

# %%
results

# %%
# Check the results (compare with the use case for one period).
whole_period = results[
    results["start_period"] == pd.Timestamp("2022-01-01 09:59:00+00:00", tz="UTC")
]["execution_value_first_5_mins"].values
one_period = execution_value_1p
whole_period == one_period

# %% [markdown]
# # The distribution of money made - money spent

# %%
post_results = results.copy()

# %%
post_results["money_diff"] = (
    post_results["holding_value_at_the_end_of_15_min"]
    - post_results["execution_value_first_5_mins"]
)
post_results["money_diff"].describe()

# %%
sns.displot(post_results, x="money_diff")

# %%
# Visualize the result
cplonorm.plot_qq(post_results["money_diff"])
