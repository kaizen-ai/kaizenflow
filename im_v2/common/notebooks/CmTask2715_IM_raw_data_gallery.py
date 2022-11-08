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
from typing import List

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


# %%
def load_parquet_by_period(
    start_ts: str, end_ts: str, s3_path: str
) -> pd.DataFrame:
    start_ts = pd.Timestamp(start_ts, tz="UTC")
    end_ts = pd.Timestamp(end_ts, tz="UTC")
    # Create timestamp filters.
    timestamp_filters = hparque.get_parquet_filters_from_timestamp_interval(
        "by_year_month", start_ts, end_ts
    )
    # Load daily data from s3 parquet.
    cc_ba_futures_daily = hparque.from_parquet(
        s3_path, filters=timestamp_filters, aws_profile="ck"
    )
    cc_ba_futures_daily = cc_ba_futures_daily.sort_index()
    return cc_ba_futures_daily


# %%
def combine_stats(stats: List[pd.Series]) -> pd.Series:
    base_stat = stats[0]
    for stat in stats[1:]:
        base_stat = base_stat.add(stat)
    return base_stat


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
s3_path = "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/bid_ask-futures/crypto_chassis/binance"
overall_rows = 0

# %% [markdown]
# The amount of data is too big to process it all at once, so the data will be loaded separately for each month and all statistics will be aggregated.

# %% [markdown]
# Process June and July

# %%
start_ts = "20220627-000000"
end_ts = "20220730-000000"
cc_ba_futures_daily = load_parquet_by_period(start_ts, end_ts, s3_path)
display(cc_ba_futures_daily.head(2))
display(cc_ba_futures_daily.tail(2))
overall_rows += len(cc_ba_futures_daily)

# %%
# Count NaNs for June and July.
june_july_nans = cstadesc.compute_frac_nan(cc_ba_futures_daily)
# Count zeros for June and July.
june_july_zeros = cstadesc.compute_frac_zero(
    cc_ba_futures_daily[["bid_price", "bid_size", "ask_price", "ask_size"]]
)

# %% [markdown]
# Process August

# %%
start_ts = "20220801-000000"
end_ts = "20220831-000000"
cc_ba_futures_daily = load_parquet_by_period(start_ts, end_ts, s3_path)
display(cc_ba_futures_daily.head(2))
display(cc_ba_futures_daily.tail(2))
overall_rows += len(cc_ba_futures_daily)

# %%
# Count NaNs for August.
aug_nans = cstadesc.compute_frac_nan(cc_ba_futures_daily)
# Count zeros for August.
aug_zeros = cstadesc.compute_frac_zero(
    cc_ba_futures_daily[["bid_price", "bid_size", "ask_price", "ask_size"]]
)

# %% [markdown]
# Process September

# %%
# Load Sept.
start_ts = "20220901-000000"
end_ts = "20220930-000000"
cc_ba_futures_daily = load_parquet_by_period(start_ts, end_ts, s3_path)
display(cc_ba_futures_daily.head(2))
display(cc_ba_futures_daily.tail(2))
overall_rows += len(cc_ba_futures_daily)

# %%
# Count NaNs for September.
sept_nans = cstadesc.compute_frac_nan(cc_ba_futures_daily)
# Count zeros for September.
sept_zeros = cstadesc.compute_frac_zero(
    cc_ba_futures_daily[["bid_price", "bid_size", "ask_price", "ask_size"]]
)

# %% [markdown]
# Process October and November

# %%
# Load Oct and Nov.
start_ts = "20221001-000000"
end_ts = "20221101-000000"
cc_ba_futures_daily = load_parquet_by_period(start_ts, end_ts, s3_path)
display(cc_ba_futures_daily.head(2))
display(cc_ba_futures_daily.tail(2))
overall_rows += len(cc_ba_futures_daily)

# %%
# Count NaNs for October and November.
oct_nov_nans = cstadesc.compute_frac_nan(cc_ba_futures_daily)
# Count zeros for October and November.
oct_nov_zeros = cstadesc.compute_frac_zero(
    cc_ba_futures_daily[["bid_price", "bid_size", "ask_price", "ask_size"]]
)

# %%
print(f"{overall_rows} rows overall")

# %% [markdown]
# #### Count NaNs

# %% run_control={"marked": false}
combine_stats([june_july_nans, aug_nans, sept_nans, oct_nov_nans])

# %% [markdown]
# #### Count zeros

# %%
combine_stats([june_july_zeros, aug_zeros, sept_zeros, oct_nov_zeros])

# %%
