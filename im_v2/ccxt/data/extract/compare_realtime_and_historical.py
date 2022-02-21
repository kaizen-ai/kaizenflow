#!/usr/bin/env python
"""
Compare daily data on DB and S3, raising when difference was found.

Use as:
# Compare daily S3 and realtime data for binance.
> im_v2/ccxt/data/extract/compare_realtime_and_historical.py \
   --db_stage 'dev' \
   --start_timestamp 20220216-000000 \
   --end_timestamp 20220217-000000 \
   --exchange_id 'binance' \
   --db_table 'ccxt_ohlcv' \
   --aws_profile 'ck' \
   --s3_path 's3://cryptokaizen-data/historical/'

Import as:

import im_v2.ccxt.data.extract.compare_realtime_and_historical as imvcdecrah
"""
import argparse
import os

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hparser as hparser
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.im_lib_tasks as imvimlita


def reindex_on_asset_and_ts(data: pd.DataFrame) -> pd.DataFrame:
    """
    Reindex data on currency pair and timestamp.

    Drops timestamps for downloading and saving.
    """
    # Select only index and OHLCV columns
    expected_col_names = [
        "timestamp",
        "currency_pair",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    hdbg.dassert_is_subset(expected_col_names, data.columns)
    data_reindex = data.loc[:, expected_col_names]
    # Reindex on ts and asset.
    data_reindex = data_reindex.set_index(["timestamp", "currency_pair"])
    return data_reindex


def find_gaps(rt_data: pd.DataFrame, daily_data: pd.DataFrame) -> pd.DataFrame:
    """
    Find data present in one dataframe and missing in other.

    :param rt_data: data downloaded in real time
    :param daily_data: data saved to S3, downloaded once daily
    :return: two dataframes with data missing in respective downloads
    """
    # Get data present in daily, but not present in rt.
    rt_missing_indices = daily_data.index.difference(rt_data.index)
    rt_missing_data = daily_data.loc[rt_missing_indices]
    # Get data present in rt, but not present in daily.
    daily_missing_indices = rt_data.index.difference(daily_data.index)
    daily_missing_data = rt_data.loc[daily_missing_indices]
    return rt_missing_data, daily_missing_data


def compare_rows(rt_data: pd.DataFrame, daily_data: pd.DataFrame) -> pd.DataFrame:
    """
    Compare contents of rows with same indices.

    :param rt_data: data downloaded to DB in real time
    :param daily_data: data downloaded to S3 once daily
    :return: dataframe with data with same indices and different contents
    """
    # Get rows on which on which the two dataframe indices match.
    idx_intersection = rt_data.index.intersection(daily_data.index)
    # Get difference between daily data and rt data.
    data_difference = daily_data.loc[idx_intersection].compare(
        # Remove columns not present in daily_data.
        rt_data.loc[idx_intersection]
    )
    return data_difference


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--start_timestamp",
        action="store",
        required=True,
        type=str,
        help="Beginning of the compared period",
    )
    parser.add_argument(
        "--end_timestamp",
        action="store",
        required=True,
        type=str,
        help="End of the compared period",
    )
    parser.add_argument(
        "--db_stage",
        action="store",
        required=True,
        type=str,
        help="DB stage to use",
    )
    parser.add_argument(
        "--exchange_id",
        action="store",
        required=True,
        type=str,
        help="Exchange for which the comparison should be done",
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
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Get time range for last 24 hours.
    start_timestamp = pd.Timestamp(args.start_timestamp)
    end_timestamp = pd.Timestamp(args.end_timestamp)
    # Connect to database.
    env_file = imvimlita.get_db_env_path(args.db_stage)
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    connection = hsql.get_connection(*connection_params)
    # Read DB realtime data.
    query = (
        f"SELECT * FROM ccxt_ohlcv WHERE knowledge_timestamp >='{start_timestamp}'"
        f" AND knowledge_timestamp <= '{end_timestamp}' AND exchange_id='{args.exchange_id}'"
    )
    rt_data = hsql.execute_query_to_df(connection, query)
    rt_data_reindex = reindex_on_asset_and_ts(rt_data)
    # Connect to S3 filesystem, if provided.
    s3fs_ = hs3.get_s3fs(args.aws_profile)
    # List files for given exchange.
    exchange_path = os.path.join(args.s3_path, args.exchange_id)
    s3_files = s3fs_.ls(exchange_path)
    # Filter files by timestamps in names.
    #  Example of downloaded file name: 'ADA_USDT_20210207-164012.csv'
    start_timestamp_str = start_timestamp.strftime("%Y%m%d-%H%M%S")
    end_timestamp_str = end_timestamp.strftime("%Y%m%d-%H%M%S")
    daily_files = [
        f
        for f in s3_files
        if f.split("_")[-1].rstrip(".csv") <= end_timestamp_str
    ]
    daily_files = [
        f
        for f in daily_files
        if f.split("_")[-1].rstrip(".csv") >= start_timestamp_str
    ]
    daily_data = []
    for file in daily_files:
        with s3fs_.open(file) as f:
            daily_data.append(pd.read_csv(f))
    daily_data = pd.concat(daily_data)
    daily_data_reindex = reindex_on_asset_and_ts(daily_data)
    # Get missing data.
    rt_missing_data, daily_missing_data = find_gaps(
        rt_data_reindex, daily_data_reindex
    )
    # Compare dataframe contents.
    data_difference = compare_rows(rt_data_reindex, daily_data_reindex)
    # Show difference and raise if one is found.
    error_message = []
    if not rt_missing_data.empty:
        error_message.append("Missing real time data:")
        error_message.append(
            hpandas.get_df_signature(
                rt_missing_data, num_rows=len(rt_missing_data)
            )
        )
    if not daily_missing_data.empty:
        error_message.append("Missing daily data:")
        error_message.append(
            hpandas.get_df_signature(
                daily_missing_data, num_rows=len(daily_missing_data)
            )
        )
    if not data_difference.empty:
        error_message.append("Differing table contents:")
        error_message.append(
            hpandas.get_df_signature(
                data_difference, num_rows=len(daily_missing_data)
            )
        )
    if error_message:
        hdbg.dfatal(message="\n".join(error_message))


if __name__ == "__main__":
    _main(_parse())
