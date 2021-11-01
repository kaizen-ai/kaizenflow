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
import logging
import os
from typing import Union

import numpy as np
import pandas as pd

import core.config.config_ as ccocon
import helpers.dbg as hdbg
import helpers.env as henv
import helpers.hpandas as hpandas
import helpers.printing as hprintin
import helpers.s3 as hs3
import im.ccxt.data.load.loader as imccdaloloa
import im.cryptodatadownload.data.load.loader as imcrdaloloa
import im.data.universe as imdauni
import research.cc.statistics as rccsta

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
    config["data"]["data_type"] = "OHLCV"
    config["data"]["target_frequency"] = "T"
    config["data"]["universe_version"] = "01"
    # Column names.
    config.add_subconfig("column_names")
    config["column_names"]["close_price"] = "close"
    config["column_names"]["currency_pair"] = "currency_pair"
    config["column_names"]["exchange"] = "exchange_id"
    return config


config = get_cmtask232_config()
print(config)


# %% [markdown]
# # Compute start-end table

# %%
def get_loader_for_vendor(vendor: str, config: ccocon.Config) -> _LOADER:
    """
    Get vendor specific loader instance.

    :param vendor: data provider, e.g. `CCXT`
    :return: loader instance
    """
    if vendor == "CCXT":
        loader = imccdaloloa.CcxtLoader(
            root_dir=config["load"]["data_dir"],
            aws_profile=config["load"]["aws_profile"],
        )
    elif vendor == "CDD":
        loader = imcrdaloloa.CddLoader(
            root_dir=config["load"]["data_dir"],
            aws_profile=config["load"]["aws_profile"],
        )
    else:
        raise ValueError(f"Unsupported vendor={vendor}")
    return loader


def compute_start_end_table_for_vendor(
    vendor_universe: str, loader, config: ccocon.Config
) -> pd.DataFrame:
    """
    Same as `compute_start_end_table` but for all exchanges, currency pairs
    available for a certain vendor.

    :param vendor_universe: all exchanges, currency pairs avaiable for a vendor
    :param loader: vendor specific loader instance
    :return: vendor specific start-end table
    """
    start_end_tables = []
    for exchange in vendor_universe.keys():
        # Get the downloaded currency pairs for a particular exchange.
        currency_pairs = vendor_universe[exchange]
        for currency_pair in currency_pairs:
            # Read data for current data provider, exchange, currency pair.
            cur_df = loader.read_data_from_filesystem(
                exchange,
                currency_pair,
                config["data"]["data_type"],
            )
            # Remove duplicates.
            # TODO(Grisha): move it into the loader.
            cur_df_no_dups = cur_df.drop_duplicates()
            # Resample data.
            cur_df_resampled = hpandas.resample_df(cur_df_no_dups, config["data"]["target_frequency"])
            # Compute `start-end table`.
            cur_start_end_table = rccsta.compute_start_end_table(cur_df_resampled, config)
            start_end_tables.append(cur_start_end_table)
    # Concatenate the results.
    start_end_table = pd.concat(start_end_tables, ignore_index=True)
    # Sort values.
    start_end_table_sorted = start_end_table.sort_values(
        by="days_available", ascending=False
    )
    return start_end_table_sorted


def compute_start_end_table_for_vendors(config: ccocon.Config) -> pd.DataFrame:
    """
    Same as `compute_start_end_table_for_vendor` but for all vendors in the
    universe.

    :return: start-end table for all vendors in the universe
    """
    # Load the universe.
    universe = imdauni.get_trade_universe(config["data"]["universe_version"])
    # Exclude CDD for now since there are many problems with data.
    # TODO(Grisha): file a bug about CDD data.
    universe.pop("CDD")
    # TODO(Grisha): fix the duplicates problem in #274.
    universe["CCXT"].pop("bitfinex")
    #
    start_end_tables = []
    for vendor in universe.keys():
        # Get vendor-specific universe.
        vendor_universe = universe[vendor]
        # Get vendor-specific loader.
        loader = get_loader_for_vendor(vendor, config)
        # Compute start-end table for the current vendor.
        cur_start_end_table = compute_start_end_table_for_vendor(
            vendor_universe, loader, config
        )
        cur_start_end_table["vendor"] = vendor
        start_end_tables.append(cur_start_end_table)
    # Concatenate the results.
    start_end_table = pd.concat(start_end_tables, ignore_index=True)
    # Sort values.
    start_end_table_sorted = start_end_table.sort_values(
        by="days_available", ascending=False
    )
    return start_end_table_sorted


# %% [markdown]
# ## Per data provider, exchange, currency pair

# %%
start_end_table = compute_start_end_table_for_vendors(config)

# %%
_LOG.info(
    "The number of unique vendor, exchange, currency pair combinations=%s",
    start_end_table.shape[0],
)
start_end_table

# %%
loader = get_loader_for_vendor("CCXT", config)
data = loader.read_data_from_filesystem("ftx", "DOGE/USDT", "OHLCV")
data_no_dups = data.drop_duplicates()
# Resample data.
data_resampled = hpandas.resample_df(data_no_dups, config["data"]["target_frequency"])
data_resampled = data_resampled.reset_index()
print(data_resampled.shape[0])
data_resampled.head(3)

# %%
data_resampled[["exchange_id", "currency_pair"]] = data_resampled[["exchange_id", "currency_pair"]].fillna(method="ffill")
data_grouped = data_resampled.groupby(["exchange_id", "currency_pair"], dropna=False, as_index=False)

# %%
import core.statistics as csta

data_grouped.agg(
    min_ts=("index", "min"),
    max_ts=("index", "max"),
    n_data_points=("close", "count"),
    coverage=("close", lambda x: 1 - csta.compute_frac_nan(x))
)

# %% [markdown]
# ## Per currency pair

# %%
currency_start_end_table = (
    start_end_table.groupby("currency_pair")
    .agg({"min_timestamp": np.min, "max_timestamp": np.max, "exchange_id": list})
    .reset_index()
)
currency_start_end_table["days_available"] = (
    currency_start_end_table["max_timestamp"] - currency_start_end_table["min_timestamp"]
).dt.days
currency_start_end_table_sorted = currency_start_end_table.sort_values(
    by="days_available",
    ascending=False,
)
_LOG.info(
    "The number of unique currency pairs=%s", currency_start_end_table_sorted.shape[0]
)
currency_start_end_table_sorted
