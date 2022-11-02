# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Imports
#

# %%
import json
import logging
import os
from datetime import timedelta

import pandas as pd
import requests

import core.config.config_ as cconconf
import core.config.config_utils as ccocouti

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hs3 as hs3
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hsql as hsql
import helpers.hprint as hprint
import im_v2.ccxt.data.client as icdcl
import im_v2.crypto_chassis.data.client as iccdc
import im_v2.im_lib_tasks as imvimlita

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# * Gaps in the data (e.g. data missing for a time period)
# * Basically we want to get the earliest and the latest date and check for holes in the time range, given that the data is stored by-minute
# * 0s and NaNs in volume and other columns, as well as their location (see description of "spikes" in gdocs above)

# %% [markdown]
# # Realtime (the DB data and the archives stored to S3)

# %% [markdown]
# ## OHLCV

# %% [markdown]
# ### CCXT (futures)

# %%
# Get DB connection.
env_file = imvimlita.get_db_env_path("dev")
# Connect with the parameters from the env file.
connection_params = hsql.get_connection_info_from_env_file(env_file)
connection = hsql.get_connection(*connection_params)

# %%
ccxt_rt_im_client = icdcl.CcxtSqlRealTimeImClient(
           False, connection, "ccxt_ohlcv_futures"
        )
# Get the full symbol universe.
universe = ccxt_rt_im_client.get_universe()
# Get the real time data.
ccxt_rt = ccxt_rt_im_client.read_data(
            universe, None, None, None, "assert"
        )

# %%
ccxt_rt

# %%

# %% [markdown]
# # Historical (data updated daily)

# %% [markdown]
# ## OHLCV 

# %% [markdown]
# ### CCXT (futures)

# %%
# Initiate the client.
ccxt_client = icdcl.CcxtHistoricalPqByTileClient(
    universe_version="v3",
    resample_1min=True,
    root_dir=os.path.join("s3://cryptokaizen-data", "reorg", "daily_staged.airflow.pq"),
    partition_mode="by_year_month",
    data_snapshot="", # does it mean all the snapshots?
    aws_profile="ck",
    dataset="ohlcv",
    contract_type="futures"
)

# Get the historical data.
ccxt_futures_daily = ccxt_client.read_data(
    full_symbols=universe, 
    start_ts=None, 
    end_ts=None, 
    columns=None, 
    filter_data_mode="assert")


# %%
ccxt_futures_daily

# %%
