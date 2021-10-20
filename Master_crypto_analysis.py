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
# # Description

# %% [markdown]
# This notebook performs EDA on the crypto prices and returns.

# %% [markdown]
# # Imports

# %%
# # %load_ext autoreload
# # %autoreload 2
# # %matplotlib inline

# %%
import logging
import os

import pandas as pd

import core.config.config_ as ccocon
import core.explore as cexp
import core.plotting as cplo
import helpers.dbg as hdbg
import helpers.env as henv
import helpers.printing as hprintin
import helpers.s3 as hs3
import im.ccxt.data.load.loader as imccdaloloa

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprintin.config_notebook()


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
    config["load"]["data_dir"] = os.path.join(hs3.get_path(), "data")
    # Data parameters.
    config.add_subconfig("data")
    # TODO(Grisha): maybe we want to have a convention about column names so
    # that we do not need to pass it via config.
    config["data"]["close_price_col_name"] = "close"
    config["data"]["datetime_col_name"] = "timestamp"
    config["data"]["frequency"] = "T"
    config["data"]["timezone"] = "US/Eastern"
    # Statistics parameters.
    config.add_subconfig("stats")
    config["stats"]["z_score_boundary"] = 3
    config["stats"]["z_score_window"] = "D"
    return config


config = get_eda_config()
print(config)

# %% [markdown]
# # Load data

# %%
# TODO(Grisha): potentially read data from the db.
ccxt_loader = imccdaloloa.CcxtLoader(
    root_dir=config["load"]["data_dir"], aws_profile=config["load"]["aws_profile"]
)
ccxt_data = ccxt_loader.read_data(
    exchange_id="binance", currency_pair="BTC/USDT", data_type="OHLCV"
)
print(ccxt_data.shape[0])
ccxt_data.head(3)

# %%
# Check the timezone info.
ccxt_data[config["data"]["datetime_col_name"]].iloc[0]

# %%
# TODO(*): change tz in `CcxtLoader`.
ccxt_data[config["data"]["datetime_col_name"]] = ccxt_data[
    config["data"]["datetime_col_name"]
].dt.tz_convert(config["data"]["timezone"])
ccxt_data[config["data"]["datetime_col_name"]].iloc[0]

# %%
# TODO(*): set index in the `CcxtLoader`: either read from db or add
# `pandas.read_csv` kwargs to the `CcxtLoader.read_data()`.
ccxt_data = ccxt_data.set_index(config["data"]["datetime_col_name"])
ccxt_data.head(3)

# %% [markdown]
# # Select subset

# %%
ccxt_data_subset = ccxt_data[[config["data"]["close_price_col_name"]]]
ccxt_data_subset.head(3)


# %% [markdown]
# # Resample index

# %%
def resample_index(index: pd.DatetimeIndex, frequency: str) -> pd.DatetimeIndex:
    """
    Resample `DatetimeIndex`.

    :param index: `DatetimeIndex` to resample
    :param frequency: frequency from `pd.date_range()` to resample to
    :return: resampled `DatetimeIndex`
    """
    hdbg.dassert_isinstance(index, pd.DatetimeIndex)
    min_date = index.min()
    max_date = index.max()
    resampled_index = pd.date_range(
        start=min_date,
        end=max_date,
        freq=frequency,
    )
    return resampled_index


resampled_index = resample_index(
    ccxt_data_subset.index, config["data"]["frequency"]
)
ccxt_data_reindex = ccxt_data_subset.reindex(resampled_index)
print(ccxt_data_reindex.shape[0])
ccxt_data_reindex.head(3)


# %% [markdown]
# # Filter data

# %%
def filter_by_date(
    df: pd.DataFrame, config: ccocon.Config, start_date: str, end_date: str
) -> pd.DataFrame:
    """
    Filter data by date [start_date, end_date).

    :param df: data
    :param config: config object
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
        hprintin.perc(mask.sum(), df.shape[0]),
    )
    ccxt_data_filtered = ccxt_data_reindex[mask]
    return ccxt_data_filtered


ccxt_data_filtered = filter_by_date(
    ccxt_data_reindex, config, "2019-01-01", "2020-01-01"
)
ccxt_data_filtered.head(3)

# %% [markdown]
# # Statistics

# %% [markdown]
# ## Plot timeseries

# %%
# TODO(Grisha): replace with a function that does the plotting.
ccxt_data_filtered[config["data"]["close_price_col_name"]].plot()

# %% [markdown]
# ## Plot timeseries distribution

# %%
# TODO(Grisha): fix the function behavior in #204.
cplo.plot_timeseries_distribution(
    ccxt_data_filtered[config["data"]["close_price_col_name"]],
    datetime_types=["hour"],
)

# %% [markdown]
# ## NaN statistics

# %%
nan_stats_df = cexp.report_zero_nan_inf_stats(ccxt_data_filtered)
nan_stats_df


# %%
# TODO(Grisha): pretify the function: add assertions, logging.
# TODO(Grisha): add support for zeros, infinities.
def count_nans_by_period(
    df: pd.DataFrame,
    config: ccocon.Config,
    period: str,
    top_n: int = 10,
) -> pd.DataFrame:
    """
    Count NaNs by period.

    :param df: data
    :param config: config object
    :param period: time period, e.g. "D" - to group by day
    :param top_n: display top N counts
    :return: table with NaN counts by period
    """
    # Select only NaNs.
    nan_data = df[df[config["data"]["close_price_col_name"]].isna()]
    # Group by specified period.
    nan_grouped = nan_data.groupby(pd.Grouper(freq=period))
    # Count NaNs.
    nan_grouped_counts = nan_grouped.apply(lambda x: x.isnull().sum())
    nan_grouped_counts.columns = ["nan_count"]
    nan_grouped_counts_sorted = nan_grouped_counts.sort_values(
        by=["nan_count"], ascending=False
    )
    return nan_grouped_counts_sorted.head(top_n)


nan_counts = count_nans_by_period(
    ccxt_data_filtered,
    config,
    "D",
)
nan_counts


# %% [markdown]
# ## Detect outliers

# %%
# TODO(Grisha): add support for other approaches, e.g. IQR-based approach.
def detect_outliers(df: pd.DataFrame, config: ccocon.Config) -> pd.DataFrame:
    """
    Detect outliers in a rolling fashion.

    :param df: data
    :param config: config object
    :return: outliers
    """
    df_copy = df.copy()
    roll = df_copy[config["data"]["close_price_col_name"]].rolling(
        window=config["stats"]["z_score_window"]
    )
    # Compute z-score for a rolling window.
    df_copy["z-score"] = (
        df_copy[config["data"]["close_price_col_name"]] - roll.mean()
    ) / roll.std()
    # Select outliers based on the z-score.
    df_outliers = df_copy[
        abs(df_copy["z-score"]) > config["stats"]["z_score_boundary"]
    ]
    return df_outliers


outliers = detect_outliers(ccxt_data_filtered, config)
print(outliers.shape[0])
outliers.head(3)
