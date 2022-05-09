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
    # TODO(Nina): @all replace `s3://cryptokaizen-data` with `get_s3_bucket()` after #1667 is implemented.
    config["load"]["data_dir"] = os.path.join(
        "s3://cryptokaizen-data",
        "historical",
    )
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["vendor"] = "CCXT"
    config["data"]["data_snapshot"] = "latest"
    config["data"]["version"] = "v3"
    config["data"]["resample_1min"] = True
    config["data"]["partition_mode"] = "by_year_month"
    config["data"]["start_ts"] = pd.Timestamp("2021-10-01T00:00:00+00:00")
    config["data"]["end_ts"] = pd.Timestamp("2022-02-10T00:00:00+00:00")
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
        # Get series with close price.
        symbol_data[config["column_names"]["close_price"]]
        # Compute stats for a full symbol.
        symbol_stats = pd.Series(dtype="object", name=full_symbol)
        symbol_stats["min_timestamp"] = symbol_data.first_valid_index()
        symbol_stats["max_timestamp"] = symbol_data.last_valid_index()
        symbol_stats["coverage %"] = 100 * (
            1
            - costatis.compute_frac_nan(
                symbol_data[config["column_names"]["close_price"]]
            )
        )
        symbol_stats["volume=0 %"] = 100 * (
            symbol_data[symbol_data["volume"] == 0].shape[0]
            / symbol_data.shape[0]
        )
        res_stats.append(symbol_stats)
    # Combine all full symbol stats.
    res_stats_df = pd.concat(res_stats, axis=1).T
    return res_stats_df


def _plot_nan_values(data: pd.DataFrame, full_symbol: str) -> None:
    """
    Plot NaN values per full symbol in data.

    "1" for "True" and "0" for "False".
    """
    for full_symbol, symbol_data in data.groupby(
        config["column_names"]["full_symbol"]
    ):
        # Get series with close price.
        nan_values = (
            symbol_data[config["column_names"]["close_price"]].isna().astype(int)
        )
        #
        plt.figure()
        plt.title(full_symbol)
        plt.scatter(nan_values.index, nan_values)
        plt.ylabel("is NaN", rotation=0)
        plt.show()


# %% [markdown]
# # Load CCXT data from the historical bucket

# %% [markdown]
# ## Initialize client

# %%
client = icdcl.ccxt_clients.CcxtHistoricalPqByTileClient(
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
_ = _plot_nan_values(binance_data, config)

# %% [markdown]
# ## ftx stats

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
_ = _plot_nan_values(ftx_data, config)

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
_ = _plot_nan_values(gateio_data, config)

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
_ = _plot_nan_values(kucoin_data, config)

# %%
