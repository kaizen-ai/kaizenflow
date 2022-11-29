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
from typing import List, Optional

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
def get_raw_ccxt_realtime_data(
    db_table: str,
    exchange_id: str,
    start_ts: Optional[str],
    end_ts: Optional[str],
) -> pd.DataFrame:
    """
    Read raw data for given exchange from the RT DB.

    Bypasses the IM Client to avoid any on-the-fly transformations.
    """
    # Get DB connection.
    connection = get_db_connection()
    # Read data from DB.
    query = f"SELECT * FROM {db_table} WHERE exchange_id='{exchange_id}'"
    if start_ts:
        start_ts = pd.Timestamp(start_ts)
        unix_start_timestamp = hdateti.convert_timestamp_to_unix_epoch(start_ts)
        query += f" AND timestamp >='{unix_start_timestamp}'"
    if end_ts:
        end_ts = pd.Timestamp(end_ts)
        unix_end_timestamp = hdateti.convert_timestamp_to_unix_epoch(end_ts)
        query += f" AND timestamp <='{unix_end_timestamp}'"
    rt_data = hsql.execute_query_to_df(connection, query)
    return rt_data


# %%
def combine_stats(stats: List[pd.Series]) -> pd.Series:
    """
    Summarize the statistics from pandas Series.

    Example of one statistic Series:

        bid_price    0.0
        bid_size     0.0
        ask_price    0.0
        ask_size     0.0
        dtype: float64
    """
    base_stat = stats[0]
    for stat in stats[1:]:
        base_stat = base_stat.add(stat)
    return base_stat


def find_gaps_in_time_series(
    time_series: pd.Series,
    start_timestamp: pd.Timestamp,
    end_timestamp: pd.Timestamp,
    step: str,
) -> pd.Series:
    """
    Find missing points on a time interval specified by [start_timestamp,
    end_timestamp], where point distribution is determined by <step>.

    :param time_series: time series to find gaps in
    :param start_timestamp: start of the time interval to check
    :param end_timestamp: end of the time interval to check
    :param step: distance between two data points on the interval,
      i.e. "S" -> second, "T" -> minute
      for a list of aliases.
    :return: pd.Series representing missing points in the source time series.
    """
    correct_time_series = pd.date_range(
        start=start_timestamp, end=end_timestamp, freq=step
    )
    return correct_time_series.difference(time_series)


# %%
def load_parquet_by_period(
    s3_path: str,
    start_ts: Optional[pd.Timestamp],
    end_ts: Optional[pd.Timestamp],
    columns: Optional[List[str]],
) -> pd.DataFrame:
    """
    Read raw historical data from the S3.

    Bypasses the IM Client to avoid any on-the-fly transformations.
    """
    # Create timestamp filters.
    timestamp_filters = hparque.get_parquet_filters_from_timestamp_interval(
        "by_year_month", start_ts, end_ts
    )
    # Load daily data from s3 parquet.
    cc_ba_futures_daily = hparque.from_parquet(
        s3_path, filters=timestamp_filters, columns=columns, aws_profile="ck"
    )
    cc_ba_futures_daily = cc_ba_futures_daily.sort_index()
    return cc_ba_futures_daily


# %%
def process_s3_data_in_chunks(
    start_ts: str, end_ts: str, s3_path: str
) -> List[pd.Series]:
    """
    Load S3 historical data by parts and count the statisticts.
    """
    overall_rows = 0
    gaps_count = 0
    nans_stats = []
    zeros_stats = []
    start_ts = pd.Timestamp(start_ts, tz="UTC")
    end_ts = pd.Timestamp(end_ts, tz="UTC")
    # Separate time period to months.
    # E.g. of `dates` sequence `['2022-10-31 00:00:00+00:00', '2022-11-30 00:00:00+00:00']`.
    dates = pd.date_range(start_ts, end_ts, freq="M")
    for i in range(0, len(dates), 2):
        # Iterate through the dates to select the loading period boundaries.
        # One chunk of data is loaded for two months.
        # E.g. the sequence of ['2022-09-31', '2022-10-30', '2022-11-31', '2022-12-31']
        # should be divided into two periods: ['2022-09-31', '2022-10-30']
        # and ['2022-11-31', '2022-12-31']
        start_end = dates[i : i + 2]
        if len(start_end) == 2:
            start, end = dates[i : i + 2]
            print(f"Loading data for the months {start.month} and {end.month}")
        else:
            start = dates[i]
            end = None
            print(f"Loading data for the month {start.month}")
        # Load the data of the time period.
        daily_data = load_parquet_by_period(s3_path, start, end, None)
        overall_rows += len(daily_data)
        print("Head:")
        display(daily_data.head(2))
        print("Tail:")
        display(daily_data.tail(2))
        # Count NaNs.
        nans = cstadesc.compute_frac_nan(daily_data)
        # Count zeros.
        zeros = cstadesc.compute_frac_zero(
            daily_data[["bid_price", "bid_size", "ask_price", "ask_size"]]
        )
        nans_stats.append(nans)
        zeros_stats.append(zeros)
        # Count gaps in time series.
        if end is None:
            end = daily_data.index[-1]
        gaps = find_gaps_in_time_series(daily_data.index, start, end, "T")
        gaps_count += len(gaps)
    print(f"{overall_rows} rows overall")
    print(f"Found {gaps_count} missing points on a time interval, step - minutes")
    return nans_stats, zeros_stats


# %% [markdown]
# # Realtime (the DB data)

# %% [markdown]
# ## OHLCV

# %% [markdown]
# ### CCXT futures

# %%
# Get the real time data from DB.
ccxt_rt = get_raw_ccxt_realtime_data(
    "ccxt_ohlcv_futures", "binance", start_ts=None, end_ts=None
)

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
ax = volume0["currency_pair"].value_counts().plot(kind="bar")
ax = ax.bar_label(ax.containers[-1], label_type="edge")

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
ax = volume0["currency_pair"].value_counts().plot(kind="bar")
ax = ax.bar_label(ax.containers[-1], label_type="edge")

# %% [markdown]
# ## BID-ASK

# %% [markdown]
# ### CC futures

# %%
s3_path = "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/bid_ask-futures/crypto_chassis/binance"

# %% [markdown]
# The amount of data is too big to process it all at once, so the data will be loaded separately for each month and all statistics will be aggregated.

# %%
start_ts = "20220627-000000"
end_ts = "20221130-000000"
nans_stats, zeros_stats = process_s3_data_in_chunks(start_ts, end_ts, s3_path)

# %% [markdown]
# #### Count NaNs

# %% run_control={"marked": false}
combine_stats(nans_stats)

# %% [markdown]
# #### Count zeros

# %%
combine_stats(zeros_stats)

# %% [markdown]
# ### CC spot

# %%
s3_path = "s3://cryptokaizen-data/reorg/daily_staged.airflow.pq/bid_ask/crypto_chassis/binance"

# %%
start_ts = "20220501-000000"
end_ts = "20221130-000000"
nans_stats, zeros_stats = process_s3_data_in_chunks(start_ts, end_ts, s3_path)

# %% [markdown]
# #### Count NaNs

# %%
combine_stats(nans_stats)

# %% [markdown]
# #### Count zeros

# %%
combine_stats(zeros_stats)

# %%
