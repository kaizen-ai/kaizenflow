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
import logging
import os

import pandas as pd

import core.config.config_ as cconconf
import core.config.config_utils as ccocouti
import core.statistics as costatis
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import im_v2.common.data.client as icdc
import im_v2.ccxt.data.client as icdcl

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
    # Data parameters.
    config["data"] = ccocouti.get_config_from_nested_dict(
        # Parameters for IM client initialization.
        {
            "im_client": {
                "universe_version": "v3",
                "resample_1min": True,
                # TODO(Dan): @all replace `s3://cryptokaizen-data` with `get_s3_bucket()` after it is fixed.
                "root_dir": os.path.join(
                    "s3://cryptokaizen-data", "historical"
                ),
                "partition_mode": "by_year_month",
                "aws_profile": "ck",
            },
            # Parameters for data query.
            "read_data": {
                "start_ts": None,
                "end_ts": None,
                "columns": None,
                "filter_data_mode": "assert",
            },
        }
    )
    # Column names.
    config["column_names"] = ccocouti.get_config_from_nested_dict(
        {
            "full_symbol": "full_symbol",
            "close_price": "close",
        }
    )
    return config


# %%
config = get_cmtask1866_config_ccxt()
print(config)


# %% [markdown]
# # Functions

# %%
def perform_qa_per_exchange(
    config: cconconf.Config,
    exchange_id: str,
    client: icdc.ImClient,
    *,
    by_year_month: bool = False,
) -> pd.DataFrame:
    """
    Get quality assurance stats for specified exchange.
    """
    # Get exchange data for related full symbols.
    universe = client.get_universe()
    exchange_universe = [
        full_symbol for full_symbol in universe if full_symbol.startswith(exchange_id)
    ]
    exchange_data = client.read_data(
        exchange_universe, **config["data"]["read_data"]
    )
    # Compute exchange stats.
    if by_year_month:
        qa_stats = _get_qa_stats_by_year_month(config, exchange_data)
    else:
        qa_stats = _get_qa_stats(config, exchange_data)
    return qa_stats


def _get_qa_stats(config: cconconf.Config, data: pd.DataFrame) -> pd.DataFrame:
    """
    Get quality assurance stats per full symbol.
    """
    res_stats = []
    for full_symbol, symbol_data in data.groupby(
        config["column_names"]["full_symbol"]
    ):
        # Compute stats for a full symbol.
        symbol_stats = pd.Series(dtype="object", name=full_symbol)
        symbol_stats["min_timestamp"] = symbol_data.index.min()
        symbol_stats["max_timestamp"] = symbol_data.index.max()
        symbol_stats["NaNs %"] = 100 * (
            costatis.compute_frac_nan(
                symbol_data[config["column_names"]["close_price"]]
            )
        )
        symbol_stats["volume=0 %"] = 100 * (
            symbol_data[symbol_data["volume"] == 0].shape[0]
            / symbol_data.shape[0]
        )
        symbol_stats["bad data %"] = (
            symbol_stats["NaNs %"] + symbol_stats["volume=0 %"]
        )
        res_stats.append(symbol_stats)
    # Combine all full symbol stats.
    res_stats_df = pd.concat(res_stats, axis=1).T
    return res_stats_df


def _get_qa_stats_by_year_month(
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
        stats_monthly = _get_qa_stats(config, data_monthly)
        #
        stats_monthly["year"] = year
        stats_monthly["month"] = month
        res_stats.append(stats_monthly)
    res_stats_df = pd.concat(res_stats)
    res_stats_df = res_stats_df.drop(["min_timestamp", "max_timestamp"], axis=1)
    #
    # Set index by full symbol, year, and month.
    res_stats_df[config["column_names"]["full_symbol"]] = res_stats_df.index
    index_columns = [config["column_names"]["full_symbol"], "year", "month"]
    res_stats_df = res_stats_df.sort_values(index_columns)
    res_stats_df = res_stats_df.set_index(index_columns)
    return res_stats_df


def _plot_bad_data_stats(bad_data_stats: pd.DataFrame) -> None:
    """
    Plot bad data stats per unique full symbol in data.
    """
    full_symbols = bad_data_stats.index.get_level_values(0).unique()
    for full_symbol in full_symbols:
        bad_data_col_name = "bad data %"
        ax = bad_data_stats.loc[full_symbol].plot.bar(
            y=bad_data_col_name, rot=0, title=full_symbol
        )
        # Set ticks and labels for time axis.
        ticks = ax.xaxis.get_ticklocs()
        ticklabels = [l.get_text() for l in ax.xaxis.get_ticklabels()]
        coef = len(ticks) // 10 + 1
        ax.xaxis.set_ticks(ticks[::coef])
        ax.xaxis.set_ticklabels(ticklabels[::coef])
        ax.figure.show()


# %% [markdown]
# # Load CCXT data from the historical bucket

# %% [markdown]
# ## Initialize client

# %%
client = icdcl.CcxtHistoricalPqByTileClient(**config["data"]["im_client"])

# %%
universe = client.get_universe()
universe

# %% [markdown]
# ## Binance stats

# %%
binance_stats = perform_qa_per_exchange(config, "binance", client)
binance_stats

# %%
binance_stats_by_year_month = perform_qa_per_exchange(
    config, "binance", client, by_year_month=True
)
binance_stats_by_year_month

# %%
_ = _plot_bad_data_stats(binance_stats_by_year_month)

# %% [markdown]
# ## FTX stats

# %%
ftx_stats = perform_qa_per_exchange(config, "ftx", client)
ftx_stats

# %%
ftx_stats_by_year_month = perform_qa_per_exchange(
    config, "ftx", client, by_year_month=True
)
ftx_stats_by_year_month

# %%
_ = _plot_bad_data_stats(ftx_stats_by_year_month)

# %% [markdown]
# ## Gateio stats

# %%
gateio_stats = perform_qa_per_exchange(config, "gateio", client)
gateio_stats

# %%
gateio_stats_by_year_month = perform_qa_per_exchange(
    config, "gateio", client, by_year_month=True
)
gateio_stats_by_year_month

# %%
_ = _plot_bad_data_stats(gateio_stats_by_year_month)

# %% [markdown]
# ## Kucoin stats

# %%
kucoin_stats = perform_qa_per_exchange(config, "kucoin", client)
kucoin_stats

# %%
kucoin_stats_by_year_month = perform_qa_per_exchange(
    config, "kucoin", client, by_year_month=True
)
kucoin_stats_by_year_month

# %%
_ = _plot_bad_data_stats(kucoin_stats_by_year_month)

# %%
