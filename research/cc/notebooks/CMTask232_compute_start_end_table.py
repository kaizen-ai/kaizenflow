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
import helpers.printing as hprintin
import helpers.s3 as hs3
import im.ccxt.data.load.loader as imccdaloloa
import im.cryptodatadownload.data.load.loader as imcrdaloloa
import im.data.universe as imdauni

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
    config["data"]["universe_version"] = "01"
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


# TODO(Grisha): convert all start-end table related functions into a class.
def compute_start_end_table(
    price_data: pd.DataFrame, config: ccocon.Config
) -> pd.DataFrame:
    """
    Compute start-end table on exchange-currency level.

    Start-end table's structure is:
        - exchange name
        - currency pair
        - minimum observed timestamp
        - maximum observed timestamp
        - the number of data points
        - the number of days for which data is available
        - average number of data points per day
        - data coverage, which is actual number of observations divided by
          expected number of observations (assuming 1 minute resolution)
          as percentage

    :param price_data: crypto price data
    :return: start-end table
    """
    # Reset `DatetimeIndex` to use it for stats computation.
    price_data_no_index = price_data.reset_index()
    # Group by exchange, currency.
    price_data_grouped = price_data_no_index.groupby(
        ["exchange_id", "currency_pair"]
    )
    # Compute the stats.
    start_end_table = (
        price_data_grouped["timestamp"]
        .agg(
            min_timestamp=np.min,
            max_timestamp=np.max,
            n_data_points="count",
        )
        .reset_index()
    )
    start_end_table["days_available"] = (
        start_end_table["max_timestamp"] - start_end_table["min_timestamp"]
    ).dt.days
    start_end_table["avg_data_points_per_day"] = (
        start_end_table["n_data_points"] / start_end_table["days_available"]
    )
    # One minute resolution is assumed, i.e. 24 * 60 observations per day.
    start_end_table["coverage"] = round(
        (100 * start_end_table["n_data_points"])
        / (start_end_table["days_available"] * 24 * 60),
        2,
    )
    return start_end_table


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
            # Compute `start-end table`.
            cur_start_end_table = compute_start_end_table(cur_df, config)
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

# %% [markdown]
# ## Per currency pair

# %%
currency_start_end_table = (
    start_end_table.groupby("currency_pair")
    .agg({"min_timestamp": np.min, "max_timestamp": np.max, "exchange_id": list})
    .reset_index()
)
_LOG.info(
    "The number of unique currency pairs=%s", currency_start_end_table.shape[0]
)
currency_start_end_table
