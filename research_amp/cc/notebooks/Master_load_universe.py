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

import matplotlib.pyplot as plt
import pandas as pd

import core.config.config_ as cconconf
import core.statistics as costatis
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
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
    # Load parameters.
    config.add_subconfig("load")
    config["load"]["aws_profile"] = "ck"
    #
    s3_bucket_path = hs3.get_s3_bucket_path(config["load"]["aws_profile"])
    s3_path = os.path.join(s3_bucket_path, "historical")
    config["load"]["data_dir"] = s3_path
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["vendor"] = "CCXT"
    config["data"]["data_snapshot"] = "latest"
    config["data"]["version"] = "v3"
    config["data"]["resample_1min"] = True
    config["data"]["partition_mode"] = "by_year_month"
    config["data"]["start_ts"] = None
    config["data"]["end_ts"] = None
    config["data"]["columns"] = None
    config["data"]["filter_data_mode"] = "assert"
    # Column names.
    config.add_subconfig("column_names")
    config["column_names"]["full_symbol"] = "full_symbol"
    config["column_names"]["close_price"] = "close"
    return config


# %%
config = get_cmtask1866_config_ccxt()
print(config)


# %% [markdown]
# # Functions

# %%
def _get_qa_stats(data: pd.DataFrame, config: cconconf.Config) -> pd.DataFrame:
    """
    Get quality assurance stats per full symbol in data.
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
        symbol_stats["bad data %"] = symbol_stats["NaNs %"] + symbol_stats["volume=0 %"]
        res_stats.append(symbol_stats)
    # Combine all full symbol stats.
    res_stats_df = pd.concat(res_stats, axis=1).T
    return res_stats_df


def _get_qa_stats_by_year_month(
    data: pd.DataFrame, config: cconconf.Config
) -> pd.DataFrame:
    """
    Get quality assurance stats per full symbol, year, and month.
    """
    #
    data["year"] = data.index.year
    data["month"] = data.index.month
    #
    res_stats = []
    columns_to_groupby = [config["column_names"]["full_symbol"], "year", "month"]
    for index, symbol_data in data.groupby(columns_to_groupby):
        #
        full_symbol, year, month = index
        # Get stats for a full symbol and add them to overall stats.
        symbol_stats = pd.Series(dtype="object", name=full_symbol)
        symbol_stats["year"] = year
        symbol_stats["month"] = month
        symbol_stats["NaNs %"] = 100 * (
            costatis.compute_frac_nan(
                symbol_data[config["column_names"]["close_price"]]
            )
        )
        symbol_stats["volume=0 %"] = 100 * (
            symbol_data[symbol_data["volume"] == 0].shape[0]
            / symbol_data.shape[0]
        )
        symbol_stats["bad data %"] = symbol_stats["NaNs %"] + symbol_stats["volume=0 %"]
        res_stats.append(symbol_stats)
    res_stats_df = pd.concat(res_stats, axis=1).T
    #
    res_stats_df["year"] = res_stats_df["year"].astype(int)
    res_stats_df["month"] = res_stats_df["month"].astype(int)
    # Set index by full symbol, year, and month.
    res_stats_df = res_stats_df.set_index([res_stats_df.index, "year", "month"])
    return res_stats_df


def _plot_bad_data_stats(bad_data_stats: pd.DataFrame) -> None:
    """
    Plot bad data stats per unique full symbol in data.
    """
    full_symbols = bad_data_stats.index.get_level_values(0).unique()
    for full_symbol in full_symbols:
        bad_data_col_name = "bad data %"
        _ = bad_data_stats.loc[full_symbol].plot.bar(
            y=bad_data_col_name, rot=0, title=full_symbol
        )


# %% [markdown]
# # Load CCXT data from the historical bucket

# %% [markdown]
# ## Initialize client

# %%
client = icdcl.CcxtHistoricalPqByTileClient(
    config["data"]["version"],
    config["data"]["resample_1min"],
    config["load"]["data_dir"],
    config["data"]["partition_mode"],
    aws_profile=config["load"]["aws_profile"],
)

# %%
universe = client.get_universe()
universe

# %% [markdown]
# ## Binance stats

# %%
binance_symbols = [
    full_symbol for full_symbol in universe if full_symbol.startswith("binance")
]
binance_symbols

# %%
binance_data = client.read_data(
    binance_symbols,
    config["data"]["start_ts"],
    config["data"]["end_ts"],
    config["data"]["columns"],
    config["data"]["filter_data_mode"],
)

# %%
binance_data.head(3)

# %%
binance_stats = _get_qa_stats(binance_data, config)
binance_stats

# %%
binance_stats_by_year_month = _get_qa_stats_by_year_month(binance_data, config)
binance_stats_by_year_month

# %%
_ = _plot_bad_data_stats(binance_stats_by_year_month)

# %% [markdown]
# ## FTX stats

# %%
ftx_symbols = [
    full_symbol for full_symbol in universe if full_symbol.startswith("ftx")
]
ftx_symbols

# %%
ftx_data = client.read_data(
    ftx_symbols,
    config["data"]["start_ts"],
    config["data"]["end_ts"],
    config["data"]["columns"],
    config["data"]["filter_data_mode"],
)

# %%
ftx_data.head(3)

# %%
ftx_stats = _get_qa_stats(ftx_data, config)
ftx_stats

# %%
ftx_stats_by_year_month = _get_qa_stats_by_year_month(ftx_data, config)
ftx_stats_by_year_month

# %%
_ = _plot_bad_data_stats(ftx_stats_by_year_month)

# %% [markdown]
# ## Gateio stats

# %%
gateio_symbols = [
    full_symbol for full_symbol in universe if full_symbol.startswith("gateio")
]
gateio_symbols

# %%
gateio_data = client.read_data(
    gateio_symbols,
    config["data"]["start_ts"],
    config["data"]["end_ts"],
    config["data"]["columns"],
    config["data"]["filter_data_mode"],
)

# %%
gateio_data.head(3)

# %%
gateio_stats = _get_qa_stats(gateio_data, config)
gateio_stats

# %%
gateio_stats_by_year_month = _get_qa_stats_by_year_month(gateio_data, config)
gateio_stats_by_year_month

# %%
_ = _plot_bad_data_stats(gateio_stats_by_year_month)

# %% [markdown]
# ## Kucoin stats

# %%
kucoin_symbols = [
    full_symbol for full_symbol in universe if full_symbol.startswith("kucoin")
]
kucoin_symbols

# %%
kucoin_data = client.read_data(
    kucoin_symbols,
    config["data"]["start_ts"],
    config["data"]["end_ts"],
    config["data"]["columns"],
    config["data"]["filter_data_mode"],
)

# %%
kucoin_data.head(3)

# %%
kucoin_stats = _get_qa_stats(kucoin_data, config)
kucoin_stats

# %%
kucoin_stats_by_year_month = _get_qa_stats_by_year_month(kucoin_data, config)
kucoin_stats_by_year_month

# %%
_ = _plot_bad_data_stats(kucoin_stats_by_year_month)

# %%
