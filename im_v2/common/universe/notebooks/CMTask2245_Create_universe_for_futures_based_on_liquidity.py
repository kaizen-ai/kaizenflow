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
# # Import

# %%
# %load_ext autoreload
# %autoreload 2

import logging
import os

import matplotlib.pyplot as plt
import pandas as pd

import core.config.config_ as cconconf
import core.config.config_utils as ccocouti
import dataflow.system.source_nodes as dtfsysonod
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import helpers.hs3 as hs3
import im_v2.crypto_chassis.data.client as iccdc
import research_amp.transform as ramptran
from datetime import timedelta

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %% [markdown]
# # Config

# %%
def get_cmtask2245_config() -> cconconf.Config:
    """
    Get config for loading and processing crypto-chassis futures data.
    """
    config = cconconf.Config()
    param_dict = {
        "data_ohlcv": {
            # Parameters for client initialization.
            "im_client": {
                "universe_version": "v2",
                "resample_1min": False,
                "root_dir": os.path.join(
                    hs3.get_s3_bucket_path("ck"), "reorg", "historical.manual.pq"
                ),
                "partition_mode": "by_year_month",
                "dataset": "ohlcv",
                "contract_type": "futures",
                "data_snapshot": "20220620",
                "aws_profile": "ck",
            },
            # Parameters for data query.
            "read_data": {
                "start_ts": None,
                "end_ts": None,
                "columns": ["full_symbol", "close", "volume"],
                "filter_data_mode": "assert",
            },
        },
        "data_bid_ask": {
            # Parameters for client initialization.
            "im_client": {
                "universe_version": "v2",
                "resample_1min": True,  # False,
                "root_dir": os.path.join(
                    hs3.get_s3_bucket_path("ck"), "reorg", "historical.manual.pq"
                ),
                "partition_mode": "by_year_month",
                "dataset": "bid_ask",
                "contract_type": "futures",
                "data_snapshot": "20220620",
                "aws_profile": "ck",
            },
            # Parameters for data query.
            "read_data": {
                "start_ts": None,
                "end_ts": None,
                "columns": None,  # ["full_symbol", "close", "volume"],
                "filter_data_mode": "assert",
            },
        },
        "column_names": {
            "full_symbol": "full_symbol",
            "close_price": "close",
        },
        "stats": {
            "n_days": 30,
        },
    }
    config = ccocouti.get_config_from_nested_dict(param_dict)
    return config


config = get_cmtask2245_config()
print(config)


# %% [markdown]
# # Functions

# %%
def filter_last_n_days(df, n_days):
    # Specify number of days.
    period = timedelta(days=n_days)
    # Set the min date for the desired period.
    start_date = df.index.max() - period + timedelta(minutes=1)
    # Filter out the required period.
    filtered_df = df.loc[start_date:]
    return filtered_df

def compute_moving_average_in_multiindex(df, value_col, rolling_window):
    # Compute MA.
    ma = df[value_col].rolling(rolling_window).mean()
    # Attach to Multiindex.
    ma_converted = pd.concat({f"{value_col}_{rolling_window}": ma},axis=1)
    return ma_converted


# %% [markdown]
# # Load the data

# %%
# Initiate clients for OHLCV and bid ask data.
client_ohlcv = iccdc.CryptoChassisHistoricalPqByTileClient(
    **config["data_ohlcv"]["im_client"]
)
client_bid_ask = iccdc.CryptoChassisHistoricalPqByTileClient(
    **config["data_bid_ask"]["im_client"]
)

# %%
# Specify universe.
universe_ohlcv = client_ohlcv.get_universe()

binance_universe = [
    full_symbol
    for full_symbol in universe_ohlcv
    if full_symbol.startswith("binance")
]
binance_universe

# %%
# Load both types of data.
binance_data_ohlcv = client_ohlcv.read_data(
    binance_universe, **config["data_ohlcv"]["read_data"]
)
binance_data_bid_ask = client_bid_ask.read_data(
    binance_universe, **config["data_bid_ask"]["read_data"]
)

display(binance_data_ohlcv.head(3))
display(binance_data_bid_ask.head(3))

# %% [markdown]
# # Process the data

# %%
# Add bid ask features.
binance_bid_ask_stats = ramptran.calculate_bid_ask_statistics(
    binance_data_bid_ask
)
# Process OHLCV data.
binance_ohlcv_converted = dtfsysonod._convert_to_multiindex(
    binance_data_ohlcv, "full_symbol"
)
# Combine OHLCV and bid ask data.
data = pd.concat([binance_ohlcv_converted, binance_bid_ask_stats], axis=1)
display(data.shape)
data.head(3)

# %% [markdown]
# Then we compute some metrics for each coin (@cryptomtc to confirm)
# - spread and spread_bps
# - mdv and mdv_shares, we assume that mdv is median daily volume
# - compute all this in a rolling fashion using 3 windows: (21, 42, 64) days
#
# Ideally we want to select the universe based on:
# - average bid/ask spread
# - daily trading volume in dollar (typically median)
# - daily market cap
#
# Typically trading volume and market cap are highly correlated, so we can just use trading volume.
# Then we compute some derived metrics (spread_bps, ...), we smooth, and apply a filter every 30 days

# %% [markdown]
# # Liquidity metrics

# %% [markdown]
# ## Spread

# %% [markdown]
# ### General values for the whole period

# %%
# Average quoted bid/ask spread.
avg_quoted_spread = data["quoted_spread"].mean().sort_values(ascending=False)
display(avg_quoted_spread)
# Plot the graph.
avg_quoted_spread.plot.bar()
plt.title("Avg quoted bid/ask spread")
plt.show()

# %%
# Average relative bid/ask spread (in bps).
avg_relative_spread = (
    data["relative_spread_bps"].mean().sort_values(ascending=False)
)
display(avg_relative_spread)
# Plot the graph.
avg_relative_spread.plot.bar()
plt.title("Avg relative bid/ask spread (in bps)")
plt.show()

# %% [markdown]
# ### Smoothing values

# %%
# Combine all three windows in one DataFrame.
spread_bps = pd.concat([
    compute_moving_average_in_multiindex(data, "relative_spread_bps", "21D"),
    compute_moving_average_in_multiindex(data, "relative_spread_bps", "42D"),
    compute_moving_average_in_multiindex(data, "relative_spread_bps", "63D"),
], axis=1)
# Show the window columns and data snippet.
window_cols = list(spread_bps.columns.get_level_values(0).unique())
display(window_cols)
display(spread_bps.head(3))

# %%
# Plot the results.
for col in window_cols:
    spread_bps[col].plot()
    plt.title(col)

# %% [markdown]
# ## Volume

# %% [markdown]
# ## Median daily volume in dollar

# %% [markdown]
# ### General values for the whole period

# %%
# Compute notional volume (price*volume).
notional_volume = data["volume"].mul(data["close"], fill_value=0)
notional_volume.head(3)

# %%
# For each day choose median notional volume.
mdv = notional_volume.resample("1D").median()
mdv.head(3)

# %%
# Then it becomes unclear how to use this data.
# E.g. we can compute avg median notional volume for the last 30 days.
mdv.mean().sort_values(ascending=False).plot.bar()

# %% [markdown]
# ### Smoothing values

# %%
# Or create DataFrame with smoothed MDV.
# Original MDV.
mdv_converted = pd.concat({f"mdv": mdv},axis=1)
# Combine original and all three windows in one DataFrame.
median_daily_volume = pd.concat([
    mdv_converted,
    compute_moving_average_in_multiindex(mdv_converted, "mdv", "21D"),
    compute_moving_average_in_multiindex(mdv_converted, "mdv", "42D"),
    compute_moving_average_in_multiindex(mdv_converted, "mdv", "63D"),
], axis=1)
# Show the window columns and data snippet.
window_cols_mdv = list(median_daily_volume.columns.get_level_values(0).unique())
display(window_cols_mdv)
display(median_daily_volume.head(3))

# %%
# Plot the results.
for col in window_cols_mdv:
    median_daily_volume[col].plot()
    plt.title(col)
