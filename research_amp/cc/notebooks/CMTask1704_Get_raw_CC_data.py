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

import pandas as pd

import core.config.config_ as cconconf
import helpers.hdbg as hdbg
import helpers.hprint as hprint
import helpers.hsql as hsql
import im_v2.ccxt.data.client as icdcl
import im_v2.im_lib_tasks as imvimlita

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

hprint.config_notebook()


# %% [markdown]
# # Config

# %%
def get_cmtask1704_config_ccxt() -> cconconf.Config:
    """
    Get task232-specific config.
    """
    config = cconconf.Config()
    # Load parameters.
    config.add_subconfig("load")
    env_file = imvimlita.get_db_env_path("dev")
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    config["load"]["connection"] = hsql.get_connection(*connection_params)
    config["load"]["aws_profile"] = "ck"
    config["load"]["data_dir_hist"] = os.path.join(
        "s3://cryptokaizen-data", "historical"
    )
    config["load"]["data_snapshot"] = "latest"
    config["load"]["partition_mode"] = "by_year_month"
    # Data parameters.
    config.add_subconfig("data")
    config["data"]["vendor"] = "CCXT"
    config["data"]["start_date"] = pd.Timestamp("2022-04-01", tz="UTC")
    config["data"]["end_date"] = pd.Timestamp("2022-04-15", tz="UTC")
    return config


# %%
config = get_cmtask1704_config_ccxt()
print(config)

# %% [markdown]
# # Load the data

# %% [markdown]
# ## Real-time

# %%
# Specify params.
vendor = config["data"]["vendor"]
resample_1min = True
connection = config["load"]["connection"]
# Initiate the client.
ccxt_rt_client = icdcl.CcxtCddDbClient(vendor, resample_1min, connection)

# %% [markdown]
# ### Universe

# %%
# Specify the universe.
rt_universe = ccxt_rt_client.get_universe()
len(rt_universe)

# %%
# Choose cc for analysis.
full_symbols = rt_universe[0:2]
full_symbols

# %% [markdown]
# ### Data Loader

# %%
# Specify time period.
start_date = config["data"]["start_date"]
end_date = config["data"]["end_date"]

# Load the data.
data = ccxt_rt_client.read_data(full_symbols, start_date, end_date)
display(data.shape)
display(data.head(3))

# %% [markdown]
# ## Historical

# %%
# Specify params.
resample_1min = True
root_dir = config["load"]["data_dir_hist"]
partition_mode = config["load"]["partition_mode"]
data_snapshot = config["load"]["data_snapshot"]
aws_profile = config["load"]["aws_profile"]

# Initiate the client.
historical_client = icdcl.CcxtHistoricalPqByTileClient(
    resample_1min,
    root_dir,
    partition_mode,
    data_snapshot=data_snapshot,
    aws_profile=aws_profile,
)

# %% [markdown]
# ### Universe

# %%
# Specify the universe.
historical_universe = historical_client.get_universe()
len(historical_universe)

# %%
# Choose cc for analysis.
full_symbols = historical_universe[0:2]
full_symbols

# %% [markdown]
# ### Data Loader

# %%
# Specify time period.
start_date = pd.Timestamp("2021-09-01", tz="UTC")
end_date = pd.Timestamp("2021-09-15", tz="UTC")

# Load the data.
data_hist = historical_client.read_data(full_symbols, start_date, end_date)
display(data_hist.shape)
display(data_hist.head(3))
