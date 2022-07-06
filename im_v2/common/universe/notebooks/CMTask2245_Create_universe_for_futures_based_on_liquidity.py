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
from datetime import timedelta

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

import core.config.config_ as cconconf
import core.config.config_utils as ccocouti
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import helpers.hs3 as hs3
import im_v2.crypto_chassis.data.client as iccdc
import research_amp.transform as ramptran

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
            "resample_rule_overtime_stats": "10T",
            "smoothing_window": "42D",
        },
    }
    config = ccocouti.get_config_from_nested_dict(param_dict)
    return config


config = get_cmtask2245_config()
print(config)


# %% [markdown]
# # Functions

# %%
def filter_last_n_days(df: pd.DataFrame, n_days: int) -> pd.DataFrame:
    """
    Isolate last N days from a given dataset.

    :param df: data with timestamp as an index
    :param n_days: number of days from the latest date to starting period
    :return: data for last N days
    """
    # Specify number of days.
    period = timedelta(days=n_days)
    # Set the min date for the desired period.
    start_date = df.index.max() - period + timedelta(minutes=1)
    # Filter out the required period.
    filtered_df = df.loc[start_date:]
    return filtered_df


def compute_moving_average_in_multiindex(
    df: pd.DataFrame, value_col: str, rolling_window: str
) -> pd.DataFrame:
    """
    Calculate Moving Average and convert to multiindex.

    :param df: data with values for MA convertation
    :param value_col: name of the column to compute MA
    :param rolling_window: size of the moving window
    :return: multiindex data with computed MA
    """
    # Compute MA.
    ma = df[value_col].rolling(rolling_window).mean()
    # Attach to Multiindex.
    multiindex_df = pd.concat({f"{value_col}_{rolling_window}": ma}, axis=1)
    return multiindex_df


def convert_df_to_same_scale(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    """
    Present numbers in a different format.

    :param df: data with values for convertation
    :param mode: i.e., "all_thousands", "all_millions", "engineering_notation"
    :return: data with converted number scales
    """
    df_new = df.copy()
    if mode == "all_thousands":
        formatter = mpl.ticker.FuncFormatter(
            lambda x, pos: "{:,.0f}".format(x / 1000) + "K"
        )
        for col in df_new.columns:
            df_new[col] = df_new[col].apply(lambda x: formatter(x))
    elif mode == "all_millions":
        formatter = mpl.ticker.FuncFormatter(
            lambda x, pos: "{:,.0f}".format(x / 1000000) + "M"
        )
        for col in df_new.columns:
            df_new[col] = df_new[col].apply(lambda x: formatter(x))
    elif mode == "engineering_notation":
        # Everything is a multiple of 1000s.
        formatter = mpl.ticker.EngFormatter(
            sep="\N{NARROW NO-BREAK SPACE}", usetex=True
        )
        for col in df_new.columns:
            df_new[col] = df_new[col].apply(lambda x: formatter(x))
    return df_new


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
# Process OHLCV data: add vwap, twap and returns.
binance_data_ohlcv_vwap_twap = ramptran.calculate_vwap_twap(
    binance_data_ohlcv, "1T"
)
binance_data_ohlcv_vwap_twap_rets = ramptran.calculate_returns(
    binance_data_ohlcv_vwap_twap, rets_type="pct_change"
)
# Combine OHLCV and bid ask data.
data = pd.concat(
    [binance_data_ohlcv_vwap_twap_rets, binance_bid_ask_stats], axis=1
)
display(data.shape)
data.head(3)

# %% [markdown]
# # Liquidity metrics

# %% [markdown]
# ## Spread

# %% [markdown]
# ### General values for the whole period

# %%
# One can reuse all the functions for breaking down the stats.
# E.g., `calculate_overtime_quantities_multiple_symbols`.
resample_rule_stats = "10T"
stats_df_mult_symbols = ramptran.calculate_overtime_quantities_multiple_symbols(
    data, binance_universe, resample_rule_stats
)
display(stats_df_mult_symbols.head(3))

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
# Combine original spread with smoothing one.
smoothing_window = config["stats"]["smoothing_window"]
spread_bps = pd.concat(
    [
        pd.concat({"relative_spread_bps": data["relative_spread_bps"]}, axis=1),
        compute_moving_average_in_multiindex(
            data, "relative_spread_bps", smoothing_window
        ),
    ],
    axis=1,
)
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
# ## Median daily volume in dollar

# %% [markdown]
# ### General values for the whole period

# %%
# Compute notional volume (price*volume).
notional_volume = data["volume"].mul(data["close"], fill_value=0)
display(convert_df_to_same_scale(notional_volume, "all_thousands").head(3))

# %%
# Mean value for daily total notional volume per day in last N days.
daily_notional_vol = notional_volume.resample("1D").sum()
mean_daily_vol = (
    filter_last_n_days(daily_notional_vol, config["stats"]["n_days"])
    .mean()
    .sort_values(ascending=False)
)
display(
    convert_df_to_same_scale(
        mean_daily_vol.rename("avg_daily_volume").to_frame(), "all_millions"
    )
)
mean_daily_vol.plot.bar()

# %% [markdown]
# ### Smoothing values

# %%
# Create DataFrame with smoothed MDVs.
# Original MDV.
mdv_converted = pd.concat({"daily_notional_vol": daily_notional_vol}, axis=1)
# Combine original and all three windows in one DataFrame.
mean_daily_volume = pd.concat(
    [
        mdv_converted,
        compute_moving_average_in_multiindex(
            mdv_converted, "daily_notional_vol", smoothing_window
        ),
    ],
    axis=1,
)
# Show the window columns and data snippet.
window_cols_mdv = list(mean_daily_volume.columns.get_level_values(0).unique())
display(window_cols_mdv)
# display(mean_daily_volume.head(3))
display(convert_df_to_same_scale(mean_daily_volume, "all_millions").head(3))

# %%
# Plot the results.
for col in window_cols_mdv:
    mean_daily_volume[col].plot()
    plt.title(col)
