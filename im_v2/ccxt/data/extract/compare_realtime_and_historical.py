"""
Import as:

import im_v2.ccxt.data.extract.compare_realtime_and_historical as imvcdecrah
"""

# DF1 = reading from S3 bucket for 24 hours
# DF2 = SELECT FROM over 24 hours

# Remove "ended_downloaded_at" and "knowledge_time"
# Make timestamp an index

# Comparison 1: compare TIMESTAMP column, which ones are missing; print.

# Comparison 2: take timestamps present in both, compare row-by-row.

import argparse

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.im_lib_tasks as imvimlita


def find_gaps(rt_data, daily_data) -> pd.DataFrame:
    """
    
    """
    rt_data_reindex = rt_data.drop(
        ["ended_downloaded_at", "knowledge_time"], axis=1
    )
    rt_data_reindex = rt_data_reindex.set_index(["timestamp", "currency_pair"])
    daily_data_reindex = daily_data.drop(
        ["ended_downloaded_at", "knowledge_time"], axis=1
    )
    daily_data_reindex = daily_data_reindex.set_index(
        ["timestamp", "currency_pair"]
    )
    # Get data present in daily, but not present in rt.
    rt_missing_indices = daily_data_reindex.index.difference(
        rt_data_reindex.index
    )
    rt_missing_data = daily_data_reindex.loc[rt_missing_indices]
    # Get data present in rt, but not present in daily.
    daily_missing_indices = rt_data_reindex.index.difference(
        rt_data_reindex.index
    )
    daily_missing_data = rt_data_reindex.loc[daily_missing_indices]
    return rt_missing_data, daily_missing_data


def compare_rows(rt_data, daily_data) -> pd.DataFrame:
    """
    
    """
    #
    rt_data_reindex = rt_data.drop(
        ["ended_downloaded_at", "knowledge_time"], axis=1
    )
    rt_data_reindex = rt_data_reindex.set_index(["timestamp", "currency_pair"])
    daily_data_reindex = daily_data.drop(
        ["ended_downloaded_at", "knowledge_time"], axis=1
    )
    daily_data_reindex = daily_data_reindex.set_index(
        ["timestamp", "currency_pair"]
    )
    #
    idx_intersection = rt_data.index.intersection(daily_data.intersection)
    # Get difference between daily data and rt data.
    data_difference = daily_data_reindex.loc[idx_intersection].compare(
        rt_data_reindex.loc[idx_intersection]
    )
    return data_difference


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--db_table",
        action="store",
        required=False,
        default="ccxt_ohlcv",
        type=str,
        help="(Optional) DB table to use, default: 'ccxt_ohlcv'",
    )
    parser = hparser.add_verbosity_arg(parser)
    parser = hs3.add_s3_args(parser)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Get time range for last 24 hours.
    end_datetime = hdateti.get_current_time("UTC")
    start_datetime = end_datetime - pd.Timedelta(days=1)
    # Connect to database.
    env_file = imvimlita.get_db_env_path(args.db_stage)
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    connection = hsql.get_connection(*connection_params)
    # Read DB realtime data.
    query = f"SELECT * FROM ccxt_ohlcv WHERE created_at >='{start_datetime}' and created_at <= {end_datetime}"
    rt_data = hsql.execute_query_to_df(connection, query)
    # Connect to S3 filesystem, if provided.
    s3fs_ = hs3.get_s3fs(args.aws_profile)
    list_of_files = s3fs_.ls(args.s3_path)
    # Filter files by timestamps in names.
    end_datetime_str = end_datetime.strftime("%Y%m%d-%H%M%S")
    start_datetime_str = start_datetime.strftime("%Y%m%d-%H%M%S")
    daily_files = [
        f for f in list_of_files if f.rstrip(".csv") <= end_datetime_str
    ]
    daily_files = [
        f for f in list_of_files if f.rstrip(".csv") >= start_datetime_str
    ]
    # TODO(Danya): Reindex dataframes before comparison, outside of functions.
    daily_data = pd.DataFrame()
    # Get missing data.
    rt_missing_data, daily_missing_data = find_gaps(rt_data, daily_data)
    # Compare dataframe contents.
    compare_rows(rt_data, daily_data)
