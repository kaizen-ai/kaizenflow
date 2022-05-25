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
# # Description

# %% [markdown]
# This notebooks performs QA checks for a single vendor:
#    - Number of NaN data points as % of total
#    - Number of data points where `volume=0` as % of total

# %% [markdown]
# # Imports

# %%
import logging
import os

import numpy as np
import pandas as pd

import core.config.config_ as cconconf
import core.config.config_utils as ccocouti
import core.statistics as costatis
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import helpers.hs3 as hs3
import im_v2.ccxt.data.client as icdcl
import im_v2.common.data.client as icdc

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %% [markdown]
# # Configs

# %%
def get_cmtask1866_config_ccxt() -> cconconf.Config:
    """
    Get task1866-specific config.
    """
    config = cconconf.Config()
    param_dict = {
        "data": {
            # Parameters for client initialization.
            "im_client": {
                "universe_version": "v3",
                "resample_1min": False,
                "root_dir": os.path.join(
                    hs3.get_s3_bucket_path("ck"), "reorg", "historical.manual.pq"
                ),
                "partition_mode": "by_year_month",
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
        "column_names": {
            "full_symbol": "full_symbol",
            "close_price": "close",
        },
        "stats": {
            "threshold": 30,
        },
    }
    config = ccocouti.get_config_from_nested_dict(param_dict)
    return config


# %%
config = get_cmtask1866_config_ccxt()
print(config)


# %% [markdown]
# # Functions

# %%
# TODO(Dan): Clean up and move to a lib.
# TODO(Dan): @Nina add more detailed description of functions. 
def _preprocess_data_for_qa_stats_computation(
    config: cconconf.Config, data: pd.DataFrame
) -> pd.DataFrame:
    """
    Preprocess vendor data for QA stats computations.
    """
    # Fill NaN values with `np.inf` in order to differentiate them
    # from missing bars.
    preprocessed_data = data.fillna(np.inf)
    # Resample data for each full symbol to insert missing bars.
    resampled_symbol_data = []
    for full_symbol, symbol_data in preprocessed_data.groupby(
        config["column_names"]["full_symbol"]
    ):
        symbol_data = hpandas.resample_df(symbol_data, "T")
        symbol_data[config["column_names"]["full_symbol"]] = symbol_data[
            config["column_names"]["full_symbol"]
        ].fillna(method="bfill")
        resampled_symbol_data.append(symbol_data)
    preprocessed_data = pd.concat(resampled_symbol_data)
    # Add year and month columns to allow grouping data by them.
    preprocessed_data["year"] = preprocessed_data.index.year
    preprocessed_data["month"] = preprocessed_data.index.month
    return preprocessed_data


def _get_timestamp_stats(
    config: cconconf.Config, data: pd.DataFrame
) -> pd.DataFrame:
    """
    Get min max timstamp stats per full symbol.
    """
    res_stats = []
    for full_symbol, symbol_data in data.groupby(
        config["column_names"]["full_symbol"]
    ):
        # Compute stats for a full symbol.
        symbol_stats = pd.Series(dtype="object", name=full_symbol)
        index = symbol_data.index
        symbol_stats["min_timestamp"] = index.min()
        symbol_stats["max_timestamp"] = index.max()
        symbol_stats["days_available"] = (
            symbol_stats["max_timestamp"] - symbol_stats["min_timestamp"]
        ).days
        res_stats.append(symbol_stats)
    # Combine all full symbol stats.
    res_stats_df = pd.concat(res_stats, axis=1).T
    return res_stats_df


# TODO(Dan): Merge with `_get_bad_data_stats_by_year_month()` by passing `agg_level`.
def _get_bad_data_stats(
    config: cconconf.Config, data: pd.DataFrame
) -> pd.DataFrame:
    """
    Get quality assurance stats per full symbol.
    """
    res_stats = []
    for full_symbol, symbol_data in data.groupby(
        config["column_names"]["full_symbol"]
    ):
        symbol_stats = pd.Series(dtype="object", name=full_symbol)
        # Compute NaNs in initially loaded data by counting `np.inf` values
        # in preprocessed data.
        symbol_stats["NaNs [%]"] = 100 * (
            symbol_data[
                symbol_data[config["column_names"]["close_price"]] == np.inf
            ].shape[0] / symbol_data.shape[0]
        )
        # Compute missing bars stats by counting NaNs created by resampling.
        symbol_stats["missing bars [%]"] = 100 * (
            costatis.compute_frac_nan(
                symbol_data[config["column_names"]["close_price"]]
            )
        )
        #
        symbol_stats["volume=0 [%]"] = 100 * (
            symbol_data[symbol_data["volume"] == 0].shape[0]
            / symbol_data.shape[0]
        )
        #
        symbol_stats["bad data [%]"] = (
            symbol_stats["NaNs [%]"]
            + symbol_stats["missing bars [%]"]
            + symbol_stats["volume=0 [%]"]
        )
        res_stats.append(symbol_stats)
    # Combine all full symbol stats and reorder columns.
    res_stats_df = pd.concat(res_stats, axis=1).T
    cols = ["bad data [%]", "missing bars [%]", "volume=0 [%]", "NaNs [%]"]
    res_stats_df = res_stats_df[cols]
    return res_stats_df


def _get_bad_data_stats_by_year_month(
    config: cconconf.Config, data: pd.DataFrame
) -> pd.DataFrame:
    """
    Get quality assurance stats per full symbol, year, and month.
    """
    res_stats = []
    for index, data_monthly in data.groupby(["year", "month"]):
        #
        year, month = index
        #
        stats_monthly = _get_bad_data_stats(config, data_monthly)
        #
        stats_monthly["year"] = year
        stats_monthly["month"] = month
        res_stats.append(stats_monthly)
    res_stats_df = pd.concat(res_stats)
    # Set index by full symbol, year, and month.
    res_stats_df[config["column_names"]["full_symbol"]] = res_stats_df.index
    index_columns = [config["column_names"]["full_symbol"], "year", "month"]
    res_stats_df = res_stats_df.sort_values(index_columns)
    res_stats_df = res_stats_df.set_index(index_columns)
    return res_stats_df


# TODO(Dan): Add filtering by dates.
def _plot_bad_data_by_year_month_stats(
    config: cconconf.Config, bad_data_stats: pd.DataFrame
) -> None:
    """
    Plot bad data stats by year and month per unique full symbol in data.

    Bad data is the sum of NaNs and "volume=0" stats.
    """
    full_symbols = bad_data_stats.index.get_level_values(0).unique()
    for full_symbol in full_symbols:
        bad_data_col_name = "bad data [%]"
        ax = bad_data_stats.loc[full_symbol].plot.bar(
            y=bad_data_col_name, rot=0, title=full_symbol
        )
        #
        ax.hlines(
            y=config["stats"]["threshold"],
            xmin=0,
            xmax=len(bad_data_stats),
            color="r",
        )
        # TODO(Dan): Make ticklabels more readable.
        # Get ticks and labels for x-axis.
        ticks = ax.xaxis.get_ticklocs()
        ticklabels = [
            l.get_text().strip("()").split(", ")
            for l in ax.xaxis.get_ticklabels()
        ]
        ticklabels = [".".join([l[0], l[1]]) for l in ticklabels]
        # Adjust x-axis labels so they do not overlap on plot by
        # picking ticks and labels by specified stride that limits
        # the number of final ticks to 10.
        stride = len(ticks) // 10 + 1
        ax.xaxis.set_ticks(ticks[::stride])
        ax.xaxis.set_ticklabels(ticklabels[::stride])
        ax.figure.show()


# %% [markdown]
# # QA checks

# %% [markdown]
# Major metric for a QA check is `"bad data [%]"` which is the sum of `"volume=0 [%]"` and `"NaNs [%]"`.

# %%
client = icdcl.CcxtHistoricalPqByTileClient(**config["data"]["im_client"])

# %%
universe = client.get_universe()
universe

# %% [markdown]
# ## Binance

# %%
binance_universe = [
    full_symbol
    for full_symbol in universe
    if full_symbol.startswith("binance")
]
binance_universe

# %%
binance_data = client.read_data(
    binance_universe, **config["data"]["read_data"]
)
binance_data = _preprocess_data_for_qa_stats_computation(config, binance_data)
binance_data.head(3)

# %%
binance_timestamp_stats = _get_timestamp_stats(config, binance_data)
binance_timestamp_stats

# %%
binance_bad_data_stats = _get_bad_data_stats(config, binance_data)
binance_bad_data_stats

# %%
binance_bad_data_stats_by_year_month = _get_bad_data_stats_by_year_month(
    config, binance_data
)
binance_bad_data_stats_by_year_month

# %%
_ = _plot_bad_data_by_year_month_stats(config, binance_bad_data_stats_by_year_month)

# %% [markdown]
# ## FTX

# %%
ftx_universe = [
    full_symbol
    for full_symbol in universe
    if full_symbol.startswith("ftx")
]
ftx_universe

# %%
ftx_data = client.read_data(
    ftx_universe, **config["data"]["read_data"]
)
ftx_data = _preprocess_data_for_qa_stats_computation(config, ftx_data)
ftx_data.head(3)

# %%
ftx_timestamp_stats = _get_timestamp_stats(config, ftx_data)
ftx_timestamp_stats

# %%
ftx_bad_data_stats = _get_bad_data_stats(config, ftx_data)
ftx_bad_data_stats

# %%
ftx_bad_data_stats_by_year_month = _get_bad_data_stats_by_year_month(
    config, ftx_data
)
ftx_bad_data_stats_by_year_month

# %%
_ = _plot_bad_data_by_year_month_stats(config, ftx_bad_data_stats_by_year_month)

# %% [markdown]
# ## Gateio

# %%
gateio_universe = [
    full_symbol
    for full_symbol in universe
    if full_symbol.startswith("gateio")
]
gateio_universe

# %%
gateio_data = client.read_data(
    gateio_universe, **config["data"]["read_data"]
)
gateio_data = _preprocess_data_for_qa_stats_computation(config, gateio_data)
gateio_data.head(3)

# %%
gateio_timestamp_stats = _get_timestamp_stats(config, gateio_data)
gateio_timestamp_stats

# %%
gateio_bad_data_stats = _get_bad_data_stats(config, gateio_data)
gateio_bad_data_stats

# %%
gateio_bad_data_stats_by_year_month = _get_bad_data_stats_by_year_month(
    config, gateio_data
)
gateio_bad_data_stats_by_year_month

# %%
_ = _plot_bad_data_by_year_month_stats(config, gateio_bad_data_stats_by_year_month)

# %% [markdown]
# ## Kucoin

# %%
kucoin_universe = [
    full_symbol
    for full_symbol in universe
    if full_symbol.startswith("kucoin")
]
kucoin_universe

# %%
kucoin_data = client.read_data(
    kucoin_universe, **config["data"]["read_data"]
)
kucoin_data = _preprocess_data_for_qa_stats_computation(config, kucoin_data)
kucoin_data.head(3)

# %%
kucoin_timestamp_stats = _get_timestamp_stats(config, kucoin_data)
kucoin_timestamp_stats

# %%
kucoin_bad_data_stats = _get_bad_data_stats(config, kucoin_data)
kucoin_bad_data_stats

# %%
kucoin_bad_data_stats_by_year_month = _get_bad_data_stats_by_year_month(
    config, kucoin_data
)
kucoin_bad_data_stats_by_year_month

# %%
_ = _plot_bad_data_by_year_month_stats(config, kucoin_bad_data_stats_by_year_month)

# %%
