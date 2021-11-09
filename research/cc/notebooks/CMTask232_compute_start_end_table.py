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

import numpy as np

import core.config.config_ as ccocon
import helpers.dbg as hdbg
import helpers.env as henv
import helpers.printing as hprintin
import helpers.s3 as hs3
import im.data.universe as imdauni
import research.cc.statistics as rccsta

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprintin.config_notebook()


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
    config["data"]["universe_version"] = "v0_2"
    config["data"]["vendor"] = "CCXT"
    # Column names.
    config.add_subconfig("column_names")
    config["column_names"]["close_price"] = "close"
    config["column_names"]["currency_pair"] = "currency_pair"
    config["column_names"]["exchange_id"] = "exchange_id"
    return config


config = get_cmtask232_config()
print(config)

# %% [markdown]
# # Compute start-end table

# %% [markdown]
# ## Per exchange id and currency pair for a specified vendor

# %%
vendor_universe = imdauni.get_vendor_universe_as_tuples(
    config["data"]["universe_version"], config["data"]["vendor"]
)
vendor_universe

# %%
compute_start_end_stats = lambda data: rccsta.compute_start_end_stats(data, config)

start_end_table = rccsta.compute_stats_for_universe(
    vendor_universe, config, compute_start_end_stats
)

# %%
_LOG.info(
    "The number of unique exchange and currency pair combinations=%s",
    start_end_table.shape[0],
)
start_end_table

# %% [markdown]
# ## Per currency pair

# %%
currency_start_end_table = rccsta.compute_start_end_table_by_currency(start_end_table)
currency_start_end_table
