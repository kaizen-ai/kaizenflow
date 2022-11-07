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
# This notebook shows raw data for CCXT OHLCV and Crypto Chassis bid-ask data.

# %% [markdown]
# # Imports

# %%
import logging

import pandas as pd

import core.statistics.descriptive as cstadesc
import helpers.hdbg as hdbg
import helpers.henv as henv
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
# # Functions

# %%
def get_ccxt_realtime_data(db_table: str, exchange_id: str) -> pd.DataFrame:
    # Get DB connection.
    env_file = imvimlita.get_db_env_path("dev")
    # Connect with the parameters from the env file.
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    connection = hsql.get_connection(*connection_params)
    # Read data from DB.
    query = f"SELECT * FROM {db_table} WHERE exchange_id='{exchange_id}'"
    rt_data = hsql.execute_query_to_df(connection, query)
    return rt_data


# %% [markdown]
# # Realtime (the DB data and the archives stored to S3)

# %% [markdown]
# ## OHLCV

# %% [markdown]
# ### CCXT futures

# %%
# Get the real time data.
ccxt_rt = get_ccxt_realtime_data("ccxt_ohlcv_futures", "binance")

# %%
print(f"{len(ccxt_rt)} rows overall")
print("Head:")
display(ccxt_rt.head(3))
print("Tail:")
display(ccxt_rt.tail(3))

# %% [markdown]
# #### Count NaNs

# %%
cstadesc.compute_frac_nan(ccxt_rt)

# %% [markdown]
# #### Rows with `volume` equal to 0

# %%
volume0 = ccxt_rt.loc[ccxt_rt["volume"] == 0]
volume0_proc = "{:.2f}".format(len(volume0) * 100 / len(ccxt_rt))
print(
    f"Percentage of data with `volume=0` in real time CCXT data: {volume0_proc}%"
)
print(f"{len(volume0)} overall")
print("First 5 rows:")
display(volume0.head())
print("Last 5 rows:")
display(volume0.tail())

# %%
volume0["currency_pair"].value_counts().plot(kind="bar")

# %% [markdown]
# # Historical (data updated daily)

# %% [markdown]
# ## OHLCV

# %% [markdown]
# ### CCXT futures

# %%
s3_path = "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/ohlcv-futures/ccxt/binance"
# Load daily data from s3 parquet.
ccxt_futures_daily = hparque.from_parquet(s3_path, aws_profile="ck")

# %%
print(f"{len(ccxt_futures_daily)} rows overall")
print("Head:")
display(ccxt_futures_daily.head())
print("Tail:")
display(ccxt_futures_daily.tail())

# %% [markdown]
# **Count NaNs**

# %%
cstadesc.compute_frac_nan(ccxt_futures_daily)


# %% [markdown]
# #### Rows with `volume` equal to 0

# %%
volume0 = ccxt_futures_daily.loc[ccxt_futures_daily["volume"] == 0]
volume0_proc = "{:.2f}".format(len(volume0) * 100 / len(ccxt_futures_daily))
print(
    f"Percentage of data with `volume=0` in historical CCXT data for the period: {volume0_proc}%"
)
print(f"{len(volume0)} overall")
print("First 5 rows:")
display(volume0.head())
print("Last 5 rows:")
display(volume0.tail())

# %%
volume0["currency_pair"].value_counts().plot(kind="bar")

# %% [markdown]
# ## BID-ASK

# %% [markdown]
# ### CC futures

# %%
# Get historical data.
s3_path = "s3://cryptokaizen-data.preprod/reorg/daily_staged.airflow.pq/bid_ask-futures/crypto_chassis/binance"
# Load daily data from s3 parquet.
cc_ba_futures_daily = hparque.from_parquet(s3_path, aws_profile="ck")

# %%
print(f"{len(cc_ba_futures_daily)} rows overall")
print("Head:")
display(cc_ba_futures_daily.head())
print("Tail:")
display(cc_ba_futures_daily.tail())

# %% [markdown]
# #### Count NaNs

# %%
cstadesc.compute_frac_nan(cc_ba_futures_daily)

# %% [markdown]
# #### Count zeros

# %%
cstadesc.compute_frac_zero(cc_ba_futures_daily)

# %% [markdown]
# ### CC spot

# %%
# Get historical data.
s3_path = "s3://cryptokaizen-data.preprod/reorg/daily_staged.airflow.pq/bid_ask/crypto_chassis/binance"
# Load daily data from s3 parquet.
cc_ba_daily = hparque.from_parquet(s3_path, aws_profile="ck")

# %%
print(f"{len(cc_ba_daily)} rows overall")
print("Head:")
display(cc_ba_daily.head())
print("Tail:")
display(cc_ba_daily.tail())

# %% [markdown]
# #### Count NaNs 

# %%
cstadesc.compute_frac_nan(cc_ba_daily)

# %% [markdown]
# #### Count zeros

# %%
cstadesc.compute_frac_zero(cc_ba_daily)

# %%
