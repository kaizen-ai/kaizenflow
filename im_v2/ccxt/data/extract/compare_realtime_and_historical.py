# DF1 = reading from S3 bucket for 24 hours
# DF2 = SELECT FROM over 24 hours

# Remove "ended_downloaded_at" and "knowledge_time"
# Make timestamp an index

# Comparison 1: compare TIMESTAMP column, which ones are missing; print.

# Comparison 2: take timestamps present in both, compare row-by-row.

import pandas as pd
import logging
import helpers.hsql as hsql
import argparse
import helpers.hdatetime as hdateti
import im_v2.im_lib_tasks as imvimlita
import helpers.hdbg as hdbg


def find_gaps(rt_data, daily_data) -> pd.DataFrame:
    """

    """
    rt_data_reindex = rt_data.drop(["ended_downloaded_at", "knowledge_time"], axis=1)
    rt_data_reindex = rt_data_reindex.set_index(["timestamp", "currency_pair"])
    daily_data_reindex = daily_data.drop(["ended_downloaded_at", "knowledge_time"], axis=1)
    daily_data_reindex = daily_data_reindex.set_index(["timestamp", "currency_pair"])
    # Get data present in daily, but not present in rt.
    rt_missing_indices = daily_data_reindex.index.difference(rt_data_reindex.index)
    rt_missing_data = daily_data_reindex.loc[rt_missing_indices]
    # Get data present in rt, but not present in daily.
    daily_missing_indices = rt_data_reindex.index.difference(rt_data_reindex.index)
    daily_missing_data = rt_data_reindex.loc[daily_missing_indices]
    return rt_missing_data, daily_missing_data


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
    # Read for the latest 24 hours.
    # TODO(Danya): read S3 data from bucket.
    daily_data = pd.DataFrame()
    # Compare indices.
    rt_missing_data, daily_missing_data = find_gaps(rt_data, daily_data)

