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
# This notebook computes earliest/latest data timestamps available per data provider, exchange, currency pair.

# %% [markdown]
# # Imports

# %%
# TODO(Grisha): move to `core/dataflow_model/notebooks` in #205.

import logging
import os
from typing import Union

import numpy as np
import pandas as pd

import core.config.config_ as ccocon
import helpers.dbg as hdbg
import helpers.env as henv
import helpers.git as hgit
import helpers.io_ as hio
import helpers.printing as hprintin
import helpers.s3 as hs3
import im.ccxt.data.load.loader as imccdaloloa
import im.cryptodatadownload.data.load.loader as imcrdaloloa

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprintin.config_notebook()

# %%
_LOADER = Union[imccdaloloa.CcxtLoader, imcrdaloloa.CddLoader]


# %% [markdown]
# # Config

# %%
def get_cmtask232_config() -> ccocon.Config:
    """
    Get task232-specific config.
    """
    config = ccocon.Config()
    # Load parameters.
    config.add_subconfig("load")
    config["load"]["aws_profile"] = "am"
    config["load"]["data_dir"] = os.path.join(hs3.get_path(), "data")
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["data_providers"] = ["CCXT", "CDD"]
    config["data"]["data_type"] = "OHLCV"
    config["data"]["universe_file_path"] = os.path.join(
        hgit.get_client_root(False),
        "im/data/downloaded_currencies.json",
    )
    return config


config = get_cmtask232_config()
print(config)


# %% [markdown]
# # Compute start-end-table

# %%
def compute_start_end_table(data_provider: str, loader: _LOADER, config: ccocon.Config) -> pd.DataFrame:
    """
    Compute data provider specific start-end-table.

    Start-end-table's structure is:
        - exchange name
        - currency pair
        - minimum observed timestamp
        - maximum observed timestamp

    :param data_provider: data provider, e.g. `CCXT`
    :param loader: provider specific loader instance
    :return: data provider specific start-end-table
    """
    # Load the universe.
    universe = hio.from_json(config["data"]["universe_file_path"])
    # TODO(Grisha): fix loading in #244.
    universe["CDD"]["binance"].remove("SCU/USDT")
    # TODO(Grisha): fix timestamps in #253.
    universe["CDD"].pop("kucoin")
    hdbg.dassert_in(data_provider, universe.keys())
    # Get provider-specific universe.
    provider_universe = universe[data_provider]
    start_end_tables = []
    for exchange in provider_universe.keys():
        # Get the downloaded currency pairs for a particular exchange.
        currency_pairs = provider_universe[exchange]
        for currency_pair in currency_pairs:
            # Read data for current data provider, exchange, currency pair.
            cur_df = loader.read_data_from_filesystem(
                exchange,
                currency_pair,
                config["data"]["data_type"],
            )
            # Compute `start-end-table`.
            cur_start_end_table = pd.DataFrame({
                "data_provider": [data_provider],
                "exchange": [exchange],
                "currency": [currency_pair],
                "min_timestamp": [cur_df.index.min()],
                "max_timestamp": [cur_df.index.max()],
            })
            start_end_tables.append(cur_start_end_table)
    # Concatenate the results.
    start_end_table = pd.concat(start_end_tables, ignore_index=True)
    return start_end_table


def get_loader_for_data_provider(data_provider: str, config: ccocon.Config) -> _LOADER:
    """
    Get data provider specific loader instance.

    :param data_provider: data provider, e.g. `CCXT`
    :return: loader instance
    """
    if data_provider == "CCXT":
        loader = imccdaloloa.CcxtLoader(
            root_dir=config["load"]["data_dir"], aws_profile=config["load"]["aws_profile"]
        )
    elif data_provider == "CDD":
        loader = imcrdaloloa.CddLoader(
            root_dir=config["load"]["data_dir"], aws_profile=config["load"]["aws_profile"]
        )
    else:
        raise ValueError(f"Unsupported data provider={data_provider}")
    return loader


# %% [markdown]
# ## Per data provider, exchange, currency pair

# %%
start_end_tables = []
for data_provider in config["data"]["data_providers"]:
    loader = get_loader_for_data_provider(data_provider, config)
    cur_start_end_table = compute_start_end_table(data_provider, loader, config)
    start_end_tables.append(cur_start_end_table)

# %%
start_end_table = pd.concat(start_end_tables, ignore_index=True)
_LOG.info("The number of unique data provider, exchange, currency pair combinations=%s", start_end_table.shape[0])
start_end_table

# %% [markdown]
# ## Per currency pair

# %%
currency_start_end_table = start_end_table.groupby("currency").agg({"min_timestamp": np.min, "max_timestamp": np.max}).reset_index()
_LOG.info("The number of unique currency pairs=%s", currency_start_end_table.shape[0])
currency_start_end_table
