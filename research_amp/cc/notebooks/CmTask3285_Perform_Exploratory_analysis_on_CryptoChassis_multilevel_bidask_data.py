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
# # Perform Exploratory analysis on CryptoChassis multilevel bid/ask data
#
# We would like to conduct a small exploratory analysis and QA on these data (both raw data of 1-second granurality and resampled data with 1-min granularity) to understand if they are usable
#
#  - How many missing bars per symbol are there
#  - How many NaNs per symbol per level per bar
#  - Conclude if it holds that CC provides good enough data for symbols in v3
#  
# The dataset is temporarily stored in:
# `s3://cryptokaizen-data-test/reorg/historical.airflow.pq/latest/bid_ask/futures/universe_v3/crypto_chassis.downloaded_1sec/`
# and
# `s3://cryptokaizen-data-test/reorg/historical.airflow.pq/latest/bid_ask/futures/universe_v3/crypto_chassis.resampled_1min/`

# %% [markdown]
# ## Setup and imports

# %%
# %load_ext autoreload
# %autoreload 2

import logging

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hprint as hprint
import helpers.hsql as hsql
import im_v2.im_lib_tasks as imvimlita

# %%
hdbg.init_logger(verbosity=logging.INFO)

_LOG = logging.getLogger(__name__)

_LOG.info("%s", henv.get_system_signature()[0])

hprint.config_notebook()

# %% [markdown]
# ## Load data
#
# Because of the dataset size the analysis on raw data will be performed only on an example symbols (BTC_USDT as most likely the most data and EOS_USDT as probably the least data rich)

# %%
filters = [("currency_pair", "=", "BTC_USDT")]
file_name = ("s3://cryptokaizen-data-test/reorg/historical.airflow.pq/latest/bid_ask/futures/" 
             "universe_v3/crypto_chassis.downloaded_1sec/binance/"
            )
df_raw_btc = hparque.from_parquet(file_name, filters=filters, aws_profile="ck")
filters = [("currency_pair", "=", "EOS_USDT")]
df_raw_eos = hparque.from_parquet(file_name, filters=filters, aws_profile="ck")

# %%
file_name = ("s3://cryptokaizen-data-test/reorg/historical.airflow.pq/latest/bid_ask/futures/" 
             "universe_v3/crypto_chassis.downloaded_1sec/binance/"
            )
filters = [("year", "=", 2021)]
df_resampled = hparque.from_parquet(file_name, filters=filters, aws_profile="ck")

# %% [markdown]
# ## Preprocess data
#
# Drop duplicates and keep only bid/ask columns

# %%
df_raw_btc= df_raw_btc.drop_duplicates()
df_raw_eos = df_raw_eos.drop_duplicates()

# %%
df_raw_btc= df_raw_btc.drop(["timestamp", "knowledge_timestamp", "currency_pair", "year", "month"], axis=1)
df_raw_eos = df_raw_eos.drop(["timestamp", "knowledge_timestamp", "currency_pair", "year", "month"], axis=1)

# %% [markdown]
# ## Analysis (raw dataset)

# %% [markdown]
# Because of the dataset size the analysis on raw data will be performed only on an example symbols (BTC_USDT as most likely the most data and EOS_USDT as probably the least data rich)

# %%
df_raw_btc.head()

# %%
df_raw_eos.head()

# %%
df_raw_btc.index.min(), df_raw_btc.index.max()

# %%
df_raw_eos.index.min(), df_raw_eos.index.max()

# %% [markdown]
# ### Find gaps in index

# %%
hpandas.find_gaps_in_time_series()

# %% [markdown]
# ## Analysis (resampled dataset)

# %% [markdown]
# ### Find gaps in index per symbol

# %% [markdown]
# ### Number of incomplete rows per symbol (atleast one column is NaN)

# %% [markdown]
# ### Sum of NaNs per symbol per column

# %% [markdown]
# ## Conclusion
