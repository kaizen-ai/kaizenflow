# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.7
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

import numpy as np
import pandas as pd

import core.config.config_ as cconconf
import core.finance.resampling as cfinresa
import core.finance.returns as cfinretu
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hsql as hsql
import im_v2.ccxt.data.client as icdcl
import im_v2.im_lib_tasks as imvimlita

import research_amp.cc.statistics as ramccsta


# %% [markdown]
# # Config

# %%
def get_cmtask1706_config() -> cconconf.Config:
    """
    Config for calculating quality assurance statistics.
    """
    config = cconconf.Config()
        # Load parameters.
    config.add_subconfig("load")
    env_file = imvimlita.get_db_env_path("dev")
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    config["load"]["connection"] = hsql.get_connection(*connection_params)
    config["load"]["aws_profile"] = "ck"
    config["load"]["data_dir"] = os.path.join(
        "s3://cryptokaizen-data", "historical"
    )
    config["load"]["data_snapshot"] = "latest"
    config["load"]["partition_mode"] = "by_year_month"
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["target_frequency"] = "T"
    config["data"]["universe_version"] = "v03"
    config["data"]["vendor"] = "CCXT"
    config["data"]["extension"] = "csv.gz"
    # Column names.
    config.add_subconfig("column_names")
    config["column_names"]["close_price"] = "close"
    config["column_names"]["currency_pair"] = "currency_pair"
    config["column_names"]["exchange_id"] = "exchange_id"
    config["column_names"]["full_symbol"] = "full_symbol"
    return config


# %%
config = get_cmtask1706_config()
print(config)

# %% [markdown]
# # QA stats

# %%
full_symbols = ['binance::ADA_USDT', 'binance::AVAX_USDT']

# %% [markdown]
# ## Real-time

# %%
compute_start_end_stats_rt = lambda data: ramccsta.compute_start_end_stats(
    data, config
)

rt_start_end_table = ramccsta.compute_stats_for_universe(
    full_symbols, config, compute_start_end_stats_rt, "real_time"
)

# %%
rt_start_end_table

# %% [markdown]
# ## Historical

# %%
compute_start_end_stats_hist = lambda data: ramccsta.compute_start_end_stats(
    data, config
)

historical_start_end_table = ramccsta.compute_stats_for_universe(
    full_symbols, config, compute_start_end_stats_hist, "historical"
)

# %%
historical_start_end_table
