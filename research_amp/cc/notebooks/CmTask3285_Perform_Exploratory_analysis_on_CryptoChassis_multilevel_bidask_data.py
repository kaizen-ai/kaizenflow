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
# We would like to conduct a small exploratory analysis and QA on these data (both raw data of 1-second granurality and resampled data with 1-min granularity) to understand if they are usable:
#
#  - How many missing bars per symbol are there?
#  - How many NaNs per symbol per level per bar?
#  - Does it hold for resampled bid(ask) prices that they are monotonically decreasing(increasing)
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
# ## Analysis (raw dataset)

# %% [markdown]
# ### Load and preprocess data
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
drop_cols = ["exchange_id", "knowledge_timestamp", "currency_pair", "year", "month"]
df_raw_btc = df_raw_btc.drop(drop_cols, axis=1)
df_raw_eos = df_raw_eos.drop(drop_cols, axis=1)

# %%
df_raw_btc = df_raw_btc.drop_duplicates()
df_raw_eos = df_raw_eos.drop_duplicates()

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
index_gaps_btc = hpandas.find_gaps_in_time_series(df_raw_btc.index.to_series(), 
                                              df_raw_btc.index.min(), 
                                              df_raw_btc.index.max(), "S")
index_gaps_btc

# %%
(len(index_gaps_btc) / len(df_raw_btc.index)) * 100

# %%
index_gaps_eos = hpandas.find_gaps_in_time_series(df_raw_eos.index.to_series(), 
                                              df_raw_eos.index.min(), 
                                              df_raw_eos.index.max(), "S")
index_gaps_eos

# %%
(len(index_gaps_eos) / len(df_raw_eos.index)) * 100

# %% [markdown]
# We can see that only ~0,01% of values are missing for both BTC_USDT and EOS_USDT

# %% [markdown]
# ### Number of incomplete rows per symbol (atleast one column is NaN)

# %%
df_raw_btc[df_raw_btc.isna().any(axis=1)]

# %%
df_raw_eos[df_raw_eos.isna().any(axis=1)]

# %% [markdown]
# There are no NaN values in the raw dataset

# %% [markdown]
# ### Sum of NaNs per column

# %% [markdown]
# There were no rows with NaN values found

# %% [markdown]
# ### Number of columns with zero values

# %%
df_raw_btc[(df_raw_btc == 0).any(axis=1)]

# %%
df_raw_eos[(df_raw_eos == 0).any(axis=1)]

# %% [markdown]
# ## Analysis (resampled dataset)

# %% [markdown]
# ### Load data and preprocess data

# %%
file_name = ("s3://cryptokaizen-data-test/reorg/historical.airflow.pq"
             "/latest/bid_ask/futures/universe_v3/crypto_chassis.resampled_1min/"
            )
filters = [("year", "=", 2021)]
df_resampled = hparque.from_parquet(file_name, filters=filters, aws_profile="ck")

# %%
drop_cols = ["exchange_id", "year", "month"]
df_resampled = df_resampled.drop(drop_cols, axis=1)

# %%
df_resampled.head()

# %%
df_resampled.info()

# %% [markdown]
# As we can see there are no NaN values in the resampled data

# %%
df_resampled.index.min(), df_resampled.index.max()

# %% [markdown]
# ### Find gaps in index per symbol

# %%
index_gaps_resampled = hpandas.find_gaps_in_time_series(df_resampled.index.to_series(), 
                                              df_resampled.index.min(), 
                                              df_resampled.index.max(), "T")
index_gaps_resampled

# %%
(len(index_gaps_resampled) / len(df_resampled.index)) * 100


# %% [markdown]
# ### Monotonity

# %%
def is_monotonic(row, increasing: bool):
    if increasing:
        return pd.Series(row.values).is_monotonic_increasing
    else:
        return pd.Series(row.values).is_monotonic_decreasing


# %%
bid_price_cols = [f"bid_price_l{l}" for l in range(1, 11)]
ask_price_cols = [f"ask_price_l{l}" for l in range(1, 11)]

# %%
df_resampled_ask_prices = df_resampled[ask_price_cols]
df_resampled_ask_prices[df_resampled_ask_prices.apply(lambda x: is_monotonic(x, False), axis=1)]

# %%
df_resampled_bid_prices = df_resampled[bid_price_cols]
df_resampled_bid_prices[df_resampled_bid_prices.apply(lambda x: is_monotonic(x, True), axis=1)]

# %% [markdown]
# There are no rows

# %% [markdown]
# ## Conclusion
#
# Bid/Ask futures multilevel data from CryptoChassis seem to be of high quality with very little issues
#
# - Less than 0,02% of the data are missing in the raw dataset 
#   - They seem to be isolated and do not break continuousness of the resampled data
# - There are no NaNs or zero values in the raw or resampled dataset
# - All bid(ask) prices in the resampled dataset are monotonically decreasing(increasing)

# %%
