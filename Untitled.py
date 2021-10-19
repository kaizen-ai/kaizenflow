# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Imports

# %%
# # %load_ext autoreload
# # %autoreload 2
# # %matplotlib inline

# %%
import logging

import pandas as pd

import core.config.config_ as ccocon
import core.explore as cexplore
import core.plotting as cplot
import helpers.dbg as dbg
import helpers.env as henv
import helpers.git as hgit
import helpers.printing as hprint
import im.ccxt.data.load.loader as cdlloa

# %%
dbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %% [markdown]
# # Config

# %%
def get_eda_config() -> ccocon.Config:
    """
    Get config that controls EDA parameters.
    
    :return: config object
    """
    config = ccocon.Config()
    # Load parameters.
    config.add_subconfig("load")
    config["load"]["aws_profile"] = "am"
    config["load"]["data_dir"] = "s3://alphamatic-data/data"
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["close_price_col_name"] = "close"
    config["data"]["datetime_col_name"] = "timestamp"
    config["data"]["frequency"] = "T"
    config["data"]["timezone"] = "US/Eastern"
    # Statistics parameters.
    config.add_subconfig("statistics")
    config["statistics"]["z_score_boundary"] = 3
    config["statistics"]["z_score_window"] = "D"
    return config

config = get_eda_config()
print(config)

# %% [markdown]
# # Load data

# %%
# TODO(Grisha): potentially read data from the db.
ccxt_loader = cdlloa.CcxtLoader(
    root_dir=config["load"]["data_dir"],
    aws_profile=config["load"]["aws_profile"]
)
ccxt_data = ccxt_loader.read_data(exchange_id="binance", currency_pair="BTC/USDT", data_type="OHLCV")
print(ccxt_data.shape[0])
ccxt_data.head(3)

# %%
# Check the timezone info.
ccxt_data["timestamp"].iloc[0]

# %%
# TODO(*): change tz in `CcxtLoader`.
ccxt_data["timestamp"] = ccxt_data["timestamp"].dt.tz_convert(config["data"]["timezone"])
ccxt_data["timestamp"].iloc[0]

# %%
# TODO(*): set index in the `CcxtLoader`: either read from db or add
# `pandas.read_csv` kwargs to the `CcxtLoader.read_data()`.
ccxt_data = ccxt_data.set_index("timestamp")
ccxt_data.head(3)

# %%
ccxt_data = ccxt_data[[config["data"]["close_price_col_name"]]]
ccxt_data.head(3)


# %%
def resample_index(index: pd.DatetimeIndex, frequency: str) -> pd.DatetimeIndex:
    """
    Resample `DatetimeIndex`.
    
    :param index: `DatetimeIndex` to resample
    :param frequency: frequency from `pd.date_range()` to resample to 
    :return: resampled `DatetimeIndex`
    """
    dbg.dassert_isinstance(index, pd.DatetimeIndex)
    min_date = index.min()
    max_date = index.max()
    resampled_index = pd.date_range(
        start = min_date, 
        end = max_date, 
        freq=frequency,
    )
    return resampled_index

resampled_index = resample_index(ccxt_data.index, config["data"]["frequency"])
ccxt_data_reindex = ccxt_data.reindex(resampled_index)    
print(ccxt_data_reindex.shape[0])
ccxt_data_reindex.head(3)


# %%
def filter_by_date(df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Filter data by date [start_date, end_date).
    
    :param df: data
    :param start_date: lower bound
    :param end_date: upper bound
    :return: filtered data
    """
    # Convert dates to timestamps.
    filter_start_date = pd.Timestamp(start_date, tz=config["data"]["timezone"])
    filter_end_date = pd.Timestamp(end_date, tz=config["data"]["timezone"])
    mask = (df.index >= filter_start_date) & (df.index < filter_end_date)
    _LOG.info(
        "Filtering in [%s; %s), selected rows=%s",
        start_date,
        end_date,
        hprint.perc(mask.sum(), df.shape[0]),
    )
    ccxt_data_filtered = ccxt_data_reindex[mask]
    return ccxt_data_filtered

ccxt_data_filtered = filter_by_date(ccxt_data_reindex, "2019-01-01", "2020-01-01")
ccxt_data_filtered.head(3)

# %%
nan_stats_df = cexplore.report_zero_nan_inf_stats(ccxt_data_filtered)
nan_stats_df

# %%
nan_data = ccxt_data_filtered[ccxt_data_filtered["close"].isna()]
nan_data.groupby(nan_data.index.date).apply(lambda x: x.isnull().sum()).sort_values(by="close", ascending=False)

# %%
not_nan = ccxt_data_filtered[~ccxt_data_filtered["close"].isna()]
cplot.plot_timeseries_distribution(not_nan["close"], datetime_types = ["day"])


# %%
def detect_outliers(df, z_score_boundary):
    df_copy = df.copy()
    roll = df_copy["close"].rolling(window="D")
    df_copy["z-score"] = (df_copy["close"] - roll.mean()) / roll.std()
    df_outliers = df_copy[abs(df_copy["z-score"]) > z_score_boundary]
    return df_outliers

detect_outliers(ccxt_data_filtered, 10)
