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
# This notebook performs cross-vendor QA checks to compare vendors in terms of:
#    - Difference and intersection of vendor universes
#    - Time intervals, i.e. which vendor has the longest data available for each full symbol in intersecting universe
#    - Data quality (NaN [%], volume=0 [%], bad data [%]) for intersecting universe and time intervals

# %% [markdown]
# # Imports

# %%
import logging
import os
from typing import List, Optional, Tuple

import pandas as pd

import core.config.config_ as cconconf
import core.config.config_utils as ccocouti
import core.statistics as costatis
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import helpers.hs3 as hs3
import im_v2.ccxt.data.client as icdcl
import im_v2.crypto_chassis.data.client as iccdc

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()


# %% [markdown]
# # Configs

# %%
def get_cmtask1966_config_ccxt() -> cconconf.Config:
    """
    Get task1966-specific config.
    """
    config = cconconf.Config()
    param_dict = {
        "data": {
            "ccxt": {
                "universe_version": "v3",
                "resample_1min": True,
                "root_dir": os.path.join(
                    hs3.get_s3_bucket_path("ck"), "reorg", "historical.manual.pq"
                ),
                "partition_mode": "by_year_month",
                "aws_profile": "ck",
            },
            "crypto_chassis": {
                "universe_version": "v1",
                "resample_1min": True,
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
config = get_cmtask1966_config_ccxt()
print(config)


# %% [markdown]
# # Functions

# %%
# TODO(Dan): Clean up and move to a lib.
# TODO(Dan): Make functions independent from hard-coded vendor names.
def _compare_vendor_universes(
    crypto_chassis_client: iccdc.CryptoChassisHistoricalPqByTileClient,
    ccxt_client: icdcl.CcxtHistoricalPqByTileClient,
) -> Tuple[List[Optional[str]], List[Optional[str]], List[Optional[str]]]:
    """
    Get common and unique vendors universes.
    """
    crypto_chassis_universe = crypto_chassis_client.get_universe()
    ccxt_universe = ccxt_client.get_universe()
    #
    common_universe = list(
        set(ccxt_universe).intersection(set(crypto_chassis_universe))
    )
    unique_crypto_chassis_universe = list(
        set(crypto_chassis_universe) - set(ccxt_universe)
    )
    unique_ccxt_universe = list(set(ccxt_universe) - set(crypto_chassis_universe))
    return common_universe, unique_crypto_chassis_universe, unique_ccxt_universe


def _compare_timestamp_stats(
    config, crypto_chassis_data, ccxt_data
) -> pd.DataFrame:
    """
    Compare timestamp stats for vendors data.
    """
    crypto_chassis_timestamp_stats = _get_timestamp_stats(
        config, crypto_chassis_data
    )
    ccxt_timestamp_stats = _get_timestamp_stats(config, ccxt_data)
    # Combine stats.
    stat_df = pd.concat(
        [crypto_chassis_timestamp_stats, ccxt_timestamp_stats],
        keys=["crypto_chassis", "ccxt"],
        axis=1,
    )
    # Reorder columns.
    cols = ["min_timestamp", "max_timestamp", "days_available"]
    stat_df = _swap_column_levels(stat_df, cols)
    return stat_df


def _compare_bad_data_stats(
    config, crypto_chassis_data, ccxt_data
) -> pd.DataFrame:
    """
    Compare bad data stats for vendors data.
    """
    crypto_chassis_bad_data_stats = _get_bad_data_stats(
        config, crypto_chassis_data
    )
    ccxt_bad_data_stats = _get_bad_data_stats(config, ccxt_data)
    # Combine stats.
    stat_df = pd.concat(
        [crypto_chassis_bad_data_stats, ccxt_bad_data_stats],
        keys=["crypto_chassis", "ccxt"],
        axis=1,
    )
    # Compute difference between bad data stats.
    for col in stat_df.columns.levels[1]:
        stat_df["diff", col] = (
            stat_df["crypto_chassis"][col] - stat_df["ccxt"][col]
        )
    # Reorder columns.
    cols = ["bad data [%]", "NaNs [%]", "volume=0 [%]"]
    stat_df = _swap_column_levels(stat_df, cols)
    return stat_df


def _compare_bad_data_stats_by_year_month(
    config, crypto_chassis_data, ccxt_data
) -> pd.DataFrame:
    """
    Compare bad data stats for vendors data by year and month.

    Stats are compared only for intersecting time intervals.
    """
    crypto_chassis_bad_data_stats_by_year_month = (
        _get_bad_data_stats_by_year_month(config, crypto_chassis_data)
    )
    ccxt_bad_data_stats_by_year_month = _get_bad_data_stats_by_year_month(
        config, ccxt_data
    )
    # Combine stats.
    stat_df = pd.concat(
        [
            crypto_chassis_bad_data_stats_by_year_month,
            ccxt_bad_data_stats_by_year_month,
        ],
        keys=["crypto_chassis", "ccxt"],
        axis=1,
    )
    # Drop stats for not intersecting time periods.
    stat_df = stat_df.dropna()
    # Reorder columns.
    cols = ["bad data [%]", "NaNs [%]", "volume=0 [%]"]
    stat_df = _swap_column_levels(stat_df, cols)
    return stat_df


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
        # Compute stats for a full symbol.
        symbol_stats = pd.Series(dtype="object", name=full_symbol)
        symbol_stats["NaNs [%]"] = 100 * (
            costatis.compute_frac_nan(
                symbol_data[config["column_names"]["close_price"]]
            )
        )
        symbol_stats["volume=0 [%]"] = 100 * (
            symbol_data[symbol_data["volume"] == 0].shape[0]
            / symbol_data.shape[0]
        )
        symbol_stats["bad data [%]"] = (
            symbol_stats["NaNs [%]"] + symbol_stats["volume=0 [%]"]
        )
        res_stats.append(symbol_stats)
    # Combine all full symbol stats.
    res_stats_df = pd.concat(res_stats, axis=1).T
    return res_stats_df


def _get_bad_data_stats_by_year_month(
    config: cconconf.Config, data: pd.DataFrame
) -> pd.DataFrame:
    """
    Get quality assurance stats per full symbol, year, and month.
    """
    # Get year and month columns to group by them.
    data["year"] = data.index.year
    data["month"] = data.index.month
    #
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


def _swap_column_levels(
    df: pd.DataFrame, upper_level_cols: List[str]
) -> pd.DataFrame:
    """
    Swap column levels with specified upper-level column order.
    """
    df.columns = df.columns.swaplevel(0, 1)
    new_cols = df.columns.reindex(upper_level_cols, level=0)
    df = df.reindex(columns=new_cols[0])
    return df


# %% [markdown]
# # Compare universes

# %%
crypto_chassis_client = iccdc.CryptoChassisHistoricalPqByTileClient(
    **config["data"]["crypto_chassis"]
)
ccxt_client = icdcl.CcxtHistoricalPqByTileClient(**config["data"]["ccxt"])

# %%
(
    common_universe,
    unique_crypto_chassis_universe,
    unique_ccxt_universe,
) = _compare_vendor_universes(crypto_chassis_client, ccxt_client)

# %%
print(len(common_universe))
common_universe

# %%
print(len(unique_crypto_chassis_universe))
unique_crypto_chassis_universe

# %%
print(len(unique_ccxt_universe))
unique_ccxt_universe

# %% [markdown]
# # Compare Binance QA stats

# %%
binance_universe = [
    full_symbol
    for full_symbol in common_universe
    if full_symbol.startswith("binance")
]
binance_universe

# %%
ccxt_binance_data = ccxt_client.read_data(
    binance_universe, **config["data"]["read_data"]
)
ccxt_binance_data.head()

# %%
crypto_chassis_binance_data = crypto_chassis_client.read_data(
    binance_universe, **config["data"]["read_data"]
)
crypto_chassis_binance_data.head()

# %%
binance_timestamp_stats_qa = _compare_timestamp_stats(
    config, crypto_chassis_binance_data, ccxt_binance_data
)
binance_timestamp_stats_qa

# %%
binance_bad_data_stats_qa = _compare_bad_data_stats(
    config, crypto_chassis_binance_data, ccxt_binance_data
)
binance_bad_data_stats_qa

# %%
binance_bad_data_stats_by_year_month_qa = _compare_bad_data_stats_by_year_month(
    config, crypto_chassis_binance_data, ccxt_binance_data
)
binance_bad_data_stats_by_year_month_qa

# %%
_plot_bad_data_by_year_month_stats(
    config, binance_bad_data_stats_by_year_month_qa
)

# %% [markdown]
# # Compare FTX QA stats

# %%
ftx_universe = [
    full_symbol
    for full_symbol in common_universe
    if full_symbol.startswith("ftx")
]
ftx_universe

# %%
ccxt_ftx_data = ccxt_client.read_data(ftx_universe, **config["data"]["read_data"])
ccxt_ftx_data.head()

# %%
crypto_chassis_ftx_data = crypto_chassis_client.read_data(
    ftx_universe, **config["data"]["read_data"]
)
crypto_chassis_ftx_data.head()

# %%
ftx_timestamp_stats_qa = _compare_timestamp_stats(
    config, crypto_chassis_ftx_data, ccxt_ftx_data
)
ftx_timestamp_stats_qa

# %%
ftx_bad_data_stats_qa = _compare_bad_data_stats(
    config, crypto_chassis_ftx_data, ccxt_ftx_data
)
ftx_bad_data_stats_qa

# %%
ftx_bad_data_stats_by_year_month_qa = _compare_bad_data_stats_by_year_month(
    config, crypto_chassis_ftx_data, ccxt_ftx_data
)
ftx_bad_data_stats_by_year_month_qa

# %%
_plot_bad_data_by_year_month_stats(config, ftx_bad_data_stats_by_year_month_qa)

# %% [markdown]
# # Compare Gateio QA stats

# %%
gateio_universe = [
    full_symbol
    for full_symbol in common_universe
    if full_symbol.startswith("gateio")
]
gateio_universe

# %%
ccxt_gateio_data = ccxt_client.read_data(
    gateio_universe, **config["data"]["read_data"]
)
ccxt_gateio_data.head()

# %%
crypto_chassis_gateio_data = crypto_chassis_client.read_data(
    gateio_universe, **config["data"]["read_data"]
)
crypto_chassis_gateio_data.head()

# %%
gateio_timestamp_stats_qa = _compare_timestamp_stats(
    config, crypto_chassis_gateio_data, ccxt_gateio_data
)
gateio_timestamp_stats_qa

# %%
gateio_bad_data_stats_qa = _compare_bad_data_stats(
    config, crypto_chassis_gateio_data, ccxt_gateio_data
)
gateio_bad_data_stats_qa

# %%
gateio_bad_data_stats_by_year_month_qa = _compare_bad_data_stats_by_year_month(
    config, crypto_chassis_gateio_data, ccxt_gateio_data
)
gateio_bad_data_stats_by_year_month_qa

# %%
_plot_bad_data_by_year_month_stats(config, gateio_bad_data_stats_by_year_month_qa)

# %% [markdown]
# # Compare Kucoin QA stats

# %%
kucoin_universe = [
    full_symbol
    for full_symbol in common_universe
    if full_symbol.startswith("kucoin")
]
kucoin_universe

# %%
ccxt_kucoin_data = ccxt_client.read_data(
    kucoin_universe, **config["data"]["read_data"]
)
ccxt_kucoin_data.head()

# %%
crypto_chassis_kucoin_data = crypto_chassis_client.read_data(
    kucoin_universe, **config["data"]["read_data"]
)
crypto_chassis_kucoin_data.head()

# %%
kucoin_timestamp_stats_qa = _compare_timestamp_stats(
    config, crypto_chassis_kucoin_data, ccxt_kucoin_data
)
kucoin_timestamp_stats_qa

# %%
kucoin_bad_data_stats_qa = _compare_bad_data_stats(
    config, crypto_chassis_kucoin_data, ccxt_kucoin_data
)
kucoin_bad_data_stats_qa

# %%
kucoin_bad_data_stats_by_year_month_qa = _compare_bad_data_stats_by_year_month(
    config, crypto_chassis_kucoin_data, ccxt_kucoin_data
)
kucoin_bad_data_stats_by_year_month_qa

# %%
_plot_bad_data_by_year_month_stats(config, kucoin_bad_data_stats_by_year_month_qa)

# %%
