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
import helpers.hparquet as hparque
import helpers.hprint as hprint
from im_v2.common.notebooks.master_raw_data_gallery_lib import *

# %%
hdbg.init_logger(verbosity=logging.INFO)
log_level = logging.INFO

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Realtime (the DB data)

# %% [markdown]
# ## periodic.airflow.downloaded_1hour.csv.ohlcv.futures.1_min.universe_v7_0.ccxt.binance.v1_0

# %%
# Get the real time data from DB.
ccxt_rt = get_raw_data_from_db(
    "ccxt_ohlcv_futures", "binance", start_ts=None, end_ts=None
)
_LOG.log(log_level, hpandas.df_to_str(ccxt_rt, log_level=log_level))

# %% [markdown]
# # Historical (data updated daily)

# %% [markdown]
# ## bulk.airflow.downloaded_EOD.parquet.ohlcv.futures.1_min.universe_v7_0.ccxt.binance.v1_0

# %%
s3_path = "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/ohlcv-futures/ccxt/binance"
# Load daily data from s3 parquet.
ccxt_futures_daily = hparque.from_parquet(s3_path, aws_profile="ck")
_LOG.log(log_level, hpandas.df_to_str(ccxt_futures_daily, log_level=log_level))

# %% [markdown]
# ## bulk.airflow.downloaded_EOD.parquet.bid_ask.futures.1_sec.universe_v3_0.crypto_chassis.binance.v1_0

# %% [markdown]
# The amount of data is too big to process it all at once, so the data will be loaded separately for each month.

# %%
s3_path = "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/bid_ask-futures/crypto_chassis/binance"
start_ts = "20220627-000000"
end_ts = "20221130-000000"
process_s3_data_in_chunks(start_ts, end_ts, s3_path, 3)

# %% [markdown]
# ## bulk.airflow.downloaded_EOD.parquet.bid_ask.futures.1_min.universe_v3_0.crypto_chassis.binance.v1_0

# %%
s3_path = "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/bid_ask-futures/crypto_chassis.resampled_1min/binance"
# Load daily data from s3 parquet.
cc_ba_futures_resampled = hparque.from_parquet(s3_path, aws_profile="ck")
_LOG.log(
    log_level, hpandas.df_to_str(cc_ba_futures_resampled, log_level=log_level)
)

# %% [markdown]
# ## bulk.airflow.downloaded_EOD.parquet.bid_ask.spot.1_sec.universe_v3.crypto_chassis.binance.v1_0

# %%
s3_path = "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/bid_ask/crypto_chassis/binance"
start_ts = "20220501-000000"
end_ts = "20221130-000000"
process_s3_data_in_chunks(start_ts, end_ts, s3_path, 3)

# %% [markdown]
# ## bulk.airflow.downloaded_EOD.parquet.bid_ask.spot.1_min.universe_v3_0.crypto_chassis.binance.v1_0

# %%
s3_path = "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/bid_ask/crypto_chassis.resampled_1min/binance"
# Load daily data from s3 parquet.
cc_ba_spot_resampled = hparque.from_parquet(s3_path, aws_profile="ck")
_LOG.log(log_level, hpandas.df_to_str(cc_ba_spot_resampled, log_level=log_level))

# %%
