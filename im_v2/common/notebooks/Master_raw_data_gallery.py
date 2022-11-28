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

# %% [markdown]
# # Imports

# %%
import logging

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hprint as hprint
import im_v2.common.notebooks.master_raw_data_gallery_lib as imvcnmrdgl

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)
log_level = logging.INFO
_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# # Realtime (the DB data)

# %% [markdown]
# ## real_time.airflow.csv.ohlcv.futures.1_min.ccxt.binance

# %%
# Get the real time data from DB.
ccxt_rt = imvcnmrdgl.get_raw_data_from_db(
    "ccxt_ohlcv_futures", "binance", start_ts=None, end_ts=None
)
_LOG.info(f"{len(ccxt_rt)} rows overall")
_LOG.log(log_level, hpandas.df_to_str(ccxt_rt, log_level=log_level))

# %% [markdown]
# # Historical (data updated daily)

# %% [markdown]
# ## historical.daily.parquet.ohlcv.futures.1_min.ccxt.binance

# %%
s3_path = "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/ohlcv-futures/ccxt/binance"
# Load daily data from s3 parquet.
ccxt_futures_daily = hparque.from_parquet(s3_path, aws_profile="ck")
_LOG.info(f"{len(ccxt_futures_daily)} rows overall")
_LOG.log(log_level, hpandas.df_to_str(ccxt_futures_daily, log_level=log_level))

# %% [markdown]
# ## historical.daily.parquet.bid_ask.futures.1_sec.crypto_chassis.binance

# %% [markdown]
# The amount of data is too big to process it all at once, so the data will be loaded separately for each month.

# %%
s3_path = "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/bid_ask-futures/crypto_chassis/binance"
start_ts = "20220627-000000"
end_ts = "20221130-000000"
imvcnmrdgl.process_s3_data_in_chunks(start_ts, end_ts, s3_path, log_level, 3)

# %% [markdown]
# ## historical.daily.parquet.bid_ask.futures.1_min.crypto_chassis.binance

# %%
s3_path = "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/bid_ask-futures/crypto_chassis.resampled_1min/binance"
# Load daily data from s3 parquet.
cc_ba_futures_resampled = hparque.from_parquet(s3_path, aws_profile="ck")
_LOG.info(f"{len(cc_ba_futures_resampled)} rows overall")
_LOG.log(
    log_level, hpandas.df_to_str(cc_ba_futures_resampled, log_level=log_level)
)

# %% [markdown]
# ## historical.daily.parquet.bid_ask.spot.1_sec.crypto_chassis.binance

# %%
s3_path = "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/bid_ask/crypto_chassis/binance"
start_ts = "20220501-000000"
end_ts = "20221130-000000"
imvcnmrdgl.process_s3_data_in_chunks(start_ts, end_ts, s3_path, log_level, 3)

# %% [markdown]
# ## historical.daily.parquet.bid_ask.spot.1_min.crypto_chassis.binance

# %%
s3_path = "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/bid_ask/crypto_chassis.resampled_1min/binance"
# Load daily data from s3 parquet.
cc_ba_spot_resampled = hparque.from_parquet(s3_path, aws_profile="ck")
_LOG.info(f"{len(cc_ba_spot_resampled)} rows overall")
_LOG.log(log_level, hpandas.df_to_str(cc_ba_spot_resampled, log_level=log_level))
