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

# %%
import collections
import logging
import os

import helpers.hdbg as hdbg
import helpers.henv as henv
import helpers.hprint as hprint
import helpers.hsql as hsql
import im_v2.ccxt.data.client as icdcl
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
ccxt_rt = ccxt_rt_im_client.read_data(universe, None, None, None, "assert")

# %% [markdown]
# **Count NaNs**

# %%
print(
    f"Percentage of NaNs in real-time CCXT data for the period: {len(ccxt_rt[ccxt_rt.open.isna()])*100/len(ccxt_rt)}"
)


# %% [markdown]
# **Count rows with `volume` value equal to 0, typically rows with volume = 0 are duplicates**

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
volume0_stats = collections.Counter(volume0["full_symbol"])
volume0_stats

# %%
storj_usdt = "{:.2f}".format(
    volume0_stats["binance::STORJ_USDT"] / len(volume0) * 100
)
print(f"'binance::STORJ_USDT' coin takes {storj_usdt}% of all rows with volume=0")

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
    root_dir=os.path.join(
        "s3://cryptokaizen-data", "reorg", "daily_staged.airflow.pq"
    ),
    partition_mode="by_year_month",
    data_snapshot="",  # does it mean all the snapshots?
    aws_profile="ck",
    dataset="ohlcv",
    contract_type="futures",
)

# Get the historical data.
ccxt_futures_daily = ccxt_client.read_data(
    full_symbols=universe,
    start_ts=None,
    end_ts=None,
    columns=None,
    filter_data_mode="assert",
)


# %%
print(f"{len(ccxt_futures_daily)} rows overall")
print("Head:")
display(ccxt_futures_daily.head())
print("Tail:")
display(ccxt_futures_daily.tail())

# %% [markdown]
# **Count NaNs**

# %%
nans_proc = "{:.6f}".format(
    len(ccxt_futures_daily[ccxt_futures_daily.open.isna()])
    * 100
    / len(ccxt_futures_daily)
)
print(f"Percentage of NaNs in CCXT data for the period: {nans_proc}%")
display(ccxt_futures_daily.loc[ccxt_futures_daily.open.isna()])


# %% [markdown]
# **Count rows with `volume` value equal to 0, typically rows with volume = 0 are duplicates**
#

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
volume0_stats = collections.Counter(volume0["full_symbol"])
volume0_stats

# %%
doge_usdt_proc = "{:.2f}".format(
    volume0_stats["binance::DOGE_USDT"] / len(volume0) * 100
)
print(
    f"'binance::DOGE_USDT' coin takes {doge_usdt_proc}% of all rows with volume=0"
)

# %%
