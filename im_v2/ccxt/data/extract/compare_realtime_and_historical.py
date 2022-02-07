"""
Import as:

import im_v2.ccxt.data.extract.compare_realtime_and_historical as imvcdecrah
"""
import argparse

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.im_lib_tasks as imvimlita


def reindex_on_asset_and_ts(data: pd.DataFrame) -> pd.DataFrame:
    """
    Reindex data on currency pair and timestamp.

    Drops timestamps for downloading and saving.
    """
    # Drop download data timestamps.
    data_reindex = data.drop(
        ["ended_downloaded_at", "knowledge_time"], axis=1
    )
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
    rt_missing_indices = rt_data.index.difference(
        rt_data.index
    )
    rt_missing_data = rt_data.loc[rt_missing_indices]
    # Get data present in rt, but not present in daily.
    daily_missing_indices = daily_data.index.difference(
        rt_data.index
    )
    daily_missing_data = rt_data.loc[daily_missing_indices]
    return rt_missing_data, daily_missing_data


def compare_rows(rt_data: pd.DataFrame, daily_data: pd.DataFrame) -> pd.DataFrame:
    """
    Compare contents of rows with same indices.


    """
    #
    idx_intersection = rt_data.index.intersection(daily_data.intersection)
    # Get difference between daily data and rt data.
    data_difference = daily_data.loc[idx_intersection].compare(
        rt_data.loc[idx_intersection]
    )
    return data_difference


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--db_stage",
        action="store",
        required=True,
        type=str,
        help="DB stage to use",
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
        f
        for f in list_of_files
        if f.split("/")[-1].rstrip(".csv") <= end_datetime_str
    ]
    daily_files = [
        f
        for f in daily_files
        if f.split("/")[-1].rstrip(".csv") >= start_datetime_str
    ]
    daily_data = []
    for file in daily_files:
        with s3fs_.open(file) as f:
            daily_data.append(pd.read_csv(f))
    daily_data = pd.concat(daily_data)
    # Get missing data.
    rt_missing_data, daily_missing_data = find_gaps(rt_data, daily_data)
    # Compare dataframe contents.
    compare_rows(rt_data, daily_data)


if __name__ == "__main__":
    _main(_parse())