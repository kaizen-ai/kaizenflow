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
# # Description
#

# %% [markdown]
# This notebook showcases locations and basic structure of raw data from:
#
# - S3 (parquet datasets)
# - DB (PostGres)
#
# ## Specs
# - This notebook:
#   - is a catalog of all the datasets that exist in the our system
#   - shows how to load data using our low-level functions or specific API for specific datasets
#   - shows how a snippet of the data looks like (for this we want to load the minimal amount of data)
#   - doesn't compute any statistics
#   - should be quickly to execute, like < 1min, so we can run it in the unit tests
#
# ## Life cycle
# - Any time a new dataset is added (e.g., in real-time DB, Parquet) we add some information on how to load it and how it looks like
# - In general we try not to delete any data but we only add data loaders

# %% [markdown]
# # Imports

# %%
import logging

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hprint as hprint
import im_v2.common.data.client.im_raw_data_client as imvcdcimrdc

# %%
hdbg.init_logger(verbosity=logging.INFO)
log_level = logging.INFO

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Realtime (the DB data)

# %% [markdown]
# ## periodic_daily.airflow.downloaded_1min.csv.ohlcv.futures.v7.ccxt.binance.v1_0_0

# %%
signature = "periodic_daily.airflow.downloaded_1min.csv.ohlcv.futures.v7.ccxt.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature)
data = reader.read_data()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# # Historical (data updated daily)

# %% [markdown]
# ## bulk.airflow.downloaded_1min.parquet.ohlcv.futures.v7.ccxt.binance.v1_0_0

# %%
signature = (
    "bulk.airflow.downloaded_1min.parquet.ohlcv.futures.v7.ccxt.binance.v1_0_0"
)
reader = imvcdcimrdc.RawDataReader(signature)
data = reader.read_data()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## bulk.airflow.downloaded_1sec.parquet.bid_ask.futures.v3.crypto_chassis.binance.v1_0_0

# %%
signature = "bulk.airflow.downloaded_1sec.parquet.bid_ask.futures.v3.crypto_chassis.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature)
data = reader.read_data()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## bulk.airflow.resampled_1min.parquet.bid_ask.futures.v3.crypto_chassis.binance.v1_0_0

# %%
signature = "bulk.airflow.resampled_1min.parquet.bid_ask.futures.v3.crypto_chassis.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature)
data = reader.read_data()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## bulk.airflow.downloaded_1sec.parquet.bid_ask.spot.v3.crypto_chassis.binance.v1_0_0

# %%
signature = "bulk.airflow.downloaded_1sec.parquet.bid_ask.spot.v3.crypto_chassis.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature)
data = reader.read_data()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %% [markdown]
# ## bulk.airflow.resampled_1min.parquet.bid_ask.spot.v3.crypto_chassis.binance.v1_0_0

# %%
signature = "bulk.airflow.downloaded_1sec.parquet.bid_ask.spot.v3.crypto_chassis.binance.v1_0_0"
reader = imvcdcimrdc.RawDataReader(signature)
data = reader.read_data()
_LOG.log(log_level, hpandas.df_to_str(data, log_level=log_level))

# %%