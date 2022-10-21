#!/usr/bin/env python
"""
Archive data from DB table older than specified timestamp
into a S3 storage, based on `timestamp` column of the table

Use as:
> im_v2/ccxt/db/archive_db_data_to_s3.py \
   --db_stage 'dev' \
   --timestamp '2022-10-20 15:46:00+00:00' \
   --db_table 'ccxt_ohlcv_test' \
   --s3_path 's3://cryptokaizen-data-test/db_archive/' \
   --incremental  \
   --dry_run

"""
import argparse
from datetime import timedelta
import logging
import os

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hparser as hparser
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.common.data.transform.transform_utils as imvcdttrut
import im_v2.im_lib_tasks as imvimlita
import im_v2.common.db.db_utils as imvcddbut
import im_v2.common.data.extract.extract_utils as imvcdeexut

_LOG = logging.getLogger(__name__)

_AWS_PROFILE = "ck"

def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--timestamp",
        action="store",
        required=True,
        type=str,
        help="Specifies time threshold for archival. Data for which \
            `table_column` > `timestamp`, get archived and dropped"
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
        required=True,
        type=str,
        help="DB table to archive data from",
    )
    # TODO(Juraj): for now we assume that the only column used for archival
    #  will be `timestamp`.
    # parser.add_argument(
    #     "--table_column",
    #     action="store",
    #     required=True,
    #     type=str,
    #     help="Column to consider when applying the time threshold",
    # )
    # ####################
    # Only a base path needs to be provided, i.e.
    #  when archiving db table ccxt_ohlcv for dev DB
    #  you only need to provide s3://cryptokaizen-data/archive/
    #  The script automatically creates/maintains the subfolder
    #  structure for the specific stage and table.
    parser.add_argument(
        "--s3_path",
        action="store",
        required=True,
        type=str,
        help="S3 location to archive data into.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        required=False,
        help="Archival mode, if True the script fails if there is no archive yet \
            for the specified table at specified path, vice versa for False"
    )
    parser.add_argument(
        "--skip_time_continuity_assertion",
        action="store_true",
        required=False,
        help="If specified, the script only warns if the archival operation \
            creates a time gap in the archive data \
            but doesn't abort the execution."
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        required=False,
        help="If specified, simulates the execution but doesn't delete \
            DB data nor save any data to s3"
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]

def _assert_data_continuity(db_data: pd.DataFrame, last_archived_row: pd.DataFrame, table_column: str, warn_only: bool) -> None:
    """
    #TODO(Juraj): we might relax the constraint for bid/ask data to ~5 seconds.
    #TODO(Juraj): a perfect solution would go symbol by symbol to assert the continuity.
    Perform two types of assertions.
        1. Assert that the last archived row is not more than 1 second (1 minute) apart from the 
    first to-be-archived row for bid/ask (OHLCV) data.
        2. Assert that the last archived row is not later than the first to-be-archived row.
    If warn_only is True, the script is
    not aborted if a time gap is found, only a warning log message is issued.
    """
    pass

def _assert_db_args(connection: hsql.DbConnection, db_table: str, table_column: str) -> None:
    """
    Assert the DB table exists and contains the specified column.
    """
    tables = hsql.get_table_names(connection)
    hdbg.dassert_in(db_table, tables)
    table_columns = hsql.get_table_columns(connection, db_table)
    hdbg.dassert_in(table_column, table_columns)

def _assert_archival_mode(incremental: bool, s3_path: str, db_stage: str, db_table: str, table_column: str) -> None:
    """
    Assert that the path corresponding to th DB stage and DB table exists if incremental is True,
    assert the path doesn't exist.
    
    The folder structure used for archival:
    s3://<s3_base_path>/<db_stage>/<db_table>/<table_column>/..parquet/partition/columns../data.parquet
    Table column <table_column> in the path helps ensure that the same column is always reused in 
    a single archival parquet file.
    """
    if incremental:
        # The profile won't change for the foreseeable future so
        # so we can keep hardcoded.
        hs3.dassert_path_exists(s3_path, aws_profile=_AWS_PROFILE)
    else:
        hs3.dassert_path_not_exists(s3_path, aws_profile=_AWS_PROFILE)

def _get_db_connection(db_stage: str) -> hsql.DbConnection:
    """
    Get connection to the database.
    
    Assumes the use of env file.
    """
    env_file = imvimlita.get_db_env_path(db_stage)
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    return hsql.get_connection(*connection_params)

def _fetch_latest_row_from_s3(s3_path: str, timestamp: pd.Timestamp) -> pd.DataFrame:
    """
    Fetch the latest archived row.
    """
    # Assume that archival happens more often than once a month.
    end_ts = timestamp
    start_ts = end_ts - timedelta(months=1)
    timestamp_filters = hparque.get_parquet_filters_from_timestamp_interval(
        "by_year_month", self.start_ts, timestamp,
    )
    # Read data corresponding to given time range.
    archived_data = hparque.from_parquet(
        s3_path, filters=timestamp_filters, aws_profile=_AWS_PROFILE
    )
    # Data should be sorted but sort again as an insurance.
    archived_data = archived_data.sort_values("timestamp", ascending=False)
    return latest_archived_data.head(1)

def _assert_correct_archival(db_data: pd.DataFrame, s3_path: str) -> None:
    """
    Safety check that the data were archived successfully.
    """
    pass

def _archive_db_data_to_s3(args: argparse.Namespace) -> None:
    """
    Archive data from DB table older than specified timestamp
    into a S3 storage, based on `timestamp` column of the table
    """
    # Transorm and assign args for readability.
    is_incremental, s3_path, db_stage, db_table, dry_run = args.incremental, args.s3_path, args.db_stage, args.db_table, args.dry_run
    # TODO(Juraj): for now we assume that the only column used for archival
    #  will be `timestamp`.
    table_column = "timestamp"
    s3_path = os.path.join(s3_path, db_stage, db_table, table_column)
    skip_time_continuity_assertion = args.skip_time_continuity_assertion
    min_age_timestamp = pd.Timestamp(args.timestamp, tz="UTC")
    # Get database connection
    db_conn = _get_db_connection(db_stage)
    # Perform argument assertions.
    _assert_db_args(db_conn, db_table, table_column)
    _assert_archival_mode(is_incremental, s3_path, db_stage, db_table, table_column)
    # Fetch DB data.
    # TODO(Juraj): for now we assume that the only column used for archival 
    # age filter is `timestamp`.
    db_data = imvcddbut.fetch_data_by_age(db_conn, db_table, table_column, min_age_timestamp)
    if db_data.empty:
        _LOG.warning(f"There were no data older than '{min_age_timestamp}' in '{db_table}' table.")
    else:
        _LOG.info(f"Fetched {db_data.shape[0]} rows from '{db_table}'.")
    # Fetch latest S3 row upon incremental archival.
    if is_incremental:
        # TODO(Juraj): think about a HW resource friendly solution to this.
        # latest_row = _fetch_latest_row_from_s3(s3_path, timestamp)
        # Assert time continuity of both datasets.
        #_assert_data_continuity(latest_row, args.skip_time_continuity_assertion)
        pass
    if dry_run:
        _LOG.info("Dry run of data archival finished successfully.")
    else:
        # Archive the data
        # The `id` column is most likely not needed once the data is in S3.
        db_data = db_data.drop("id", axis=1)
        # Argument data_type is only used to specify duplicate removal mode in
        #  hparquet.list_and_merge_pq_files, 'None' is needed here.
        imvcdeexut.save_parquet(db_data, s3_path, unit="ms", aws_profile=_AWS_PROFILE, data_type=None,
        drop_db_metadata_column=False,
        list_and_merge=False)
        # Double check archival was successful
        # TODO(Juraj): this might a be pretty difficult problem.
        # _assert_correct_archival(db_data, s3_path)
        # Drop DB data.
        imvcddbut.drop_db_data_by_age(db_conn, db_table, "timestamp", min_age_timestamp)
        _LOG.info("Data archival finished successfully.")

    
def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _LOG.info(args)
    _archive_db_data_to_s3(args)


if __name__ == "__main__":
    _main(_parse())