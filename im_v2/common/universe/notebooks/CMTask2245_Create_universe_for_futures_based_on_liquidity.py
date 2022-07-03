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

import core.config.config_ as cconconf
import core.config.config_utils as ccocouti
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import helpers.hs3 as hs3
import im_v2.crypto_chassis.data.client as iccdc
import research_amp.cc.qa as ramccqa
import research_amp.transform as ramptran
import core.finance as cofinanc
import core.finance.bid_ask as cfibiask
import core.finance.resampling as cfinresa
import dataflow.core as dtfcore
import dataflow.system.source_nodes as dtfsysonod
import pandas as pd

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
                "resample_1min": False,
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
                "columns": None,#["full_symbol", "close", "volume"],
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

config = get_cmtask2245_config()
print(config)

# %% [markdown]
# # Load the data

# %%
# Initiate clients for OHLCV and bid ask data.
client_ohlcv = iccdc.CryptoChassisHistoricalPqByTileClient(**config["data_ohlcv"]["im_client"])
client_bid_ask = iccdc.CryptoChassisHistoricalPqByTileClient(**config["data_bid_ask"]["im_client"])

# %%
# Specify universe.
universe_ohlcv = client_ohlcv.get_universe()

binance_universe = [
    full_symbol for full_symbol in universe_ohlcv if full_symbol.startswith("binance")
]
binance_universe

# %%
# Load both types of data.
binance_data_ohlcv = client_ohlcv.read_data(binance_universe, **config["data_ohlcv"]["read_data"])
binance_data_bid_ask = client_bid_ask.read_data(binance_universe, **config["data_bid_ask"]["read_data"])

display(binance_data_ohlcv.head(3))
display(binance_data_bid_ask.head(3))

# %% [markdown]
# # Process the data

# %%
binance_bid_ask_stats = ramptran.calculate_bid_ask_statistics(binance_data_bid_ask)
# binance_bid_ask_stats.tail(3)

# %%
binance_ohlcv_converted = dtfsysonod._convert_to_multiindex(binance_data_ohlcv, "full_symbol")

# %%
data = pd.concat([binance_ohlcv_converted, binance_bid_ask_stats],axis=1)
data

# %%
data.columns.get_level_values(0).unique()

# %%

# %%

# %%
