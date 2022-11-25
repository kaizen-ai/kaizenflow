"""
Helpers functions for im_v2/common/notebooks/Master_raw_data_gallery.ipynb

import im_v2.commom.notebooks.master_raw_data_gallery as icnmgal
"""
import logging
from typing import List, Optional

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hsql as hsql
import im_v2.im_lib_tasks as imvimlita

_LOG = logging.getLogger(__name__)

def get_raw_data_from_db(
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
    env_file = imvimlita.get_db_env_path("dev")
    # Connect with the parameters from the env file.
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    connection = hsql.get_connection(*connection_params)
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


def load_parquet_by_period(
    start_ts: pd.Timestamp, end_ts: pd.Timestamp, s3_path: str
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
        s3_path, filters=timestamp_filters, aws_profile="ck"
    )
    
    cc_ba_futures_daily = cc_ba_futures_daily.sort_index()
    return cc_ba_futures_daily


def process_s3_data_in_chunks(
    start_ts: str, end_ts: str, s3_path: str
) -> List[pd.Series]:
    """
    Load S3 historical data by parts and count the statisticts.
    """
    overall_rows = 0
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
        # TODO(Danya): Pass a month timedelta as a parameter.
        start_end = dates[i : i + 2]
        if len(start_end) == 2:
            start, end = dates[i : i + 2]
            _LOG.info(f"Loading data for the months {start.month} and {end.month}")
        else:
            start = dates[i]
            end = None
            _LOG.info(f"Loading data for the month {start.month}")
        # Load the data of the time period.
        daily_data = load_parquet_by_period(start, end, s3_path)
        overall_rows += len(daily_data)
        _LOG.info("Head:")
        hpandas._display(daily_data.head(2)) 
        _LOG.info("Tail:")
        hpandas._display(daily_data.tail(2))
    print(f"{overall_rows} rows overall")
    return


