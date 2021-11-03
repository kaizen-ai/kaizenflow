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
    config["data"]["universe_version"] = "02"
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

# %% [markdown]
# ## Per data provider, exchange, currency pair

# %%
compute_start_end_table = lambda data: rccsta.compute_start_end_table(data, config)

start_end_table = rccsta.compute_stats_for_universe(
    config, compute_start_end_table
)

# %%
_LOG.info(
    "The number of unique vendor, exchange, currency pair combinations=%s",
    start_end_table.shape[0],
)
start_end_table.sort_values(by="days_available", ascending=False).reset_index(
    drop=True
)

# %% [markdown]
# ## Per currency pair

# %%
# TODO(Grisha): Move to a lib.
currency_start_end_table = (
    start_end_table.groupby("currency_pair")
    .agg({"min_timestamp": np.min, "max_timestamp": np.max, "exchange_id": list})
    .reset_index()
)
currency_start_end_table["days_available"] = (
    currency_start_end_table["max_timestamp"]
    - currency_start_end_table["min_timestamp"]
).dt.days
currency_start_end_table_sorted = currency_start_end_table.sort_values(
    by="days_available",
    ascending=False,
).reset_index(drop=True)
_LOG.info(
    "The number of unique currency pairs=%s",
    currency_start_end_table_sorted.shape[0],
)
currency_start_end_table_sorted
