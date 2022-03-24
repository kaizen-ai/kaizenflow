"""
Implement common exchange download operations.

Import as:

import im_v2.common.data.extract.extract_utils as imvcdeexut
"""


import argparse
import os
import time
from typing import Any

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparquet as hparque
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.common.data.transform.transform_utils as imvcdttrut
import im_v2.common.universe.universe as imvcounun
import im_v2.im_lib_tasks as imvimlita


def add_exchange_download_args(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    """
    Add the command line options exchange download.
    """
    parser.add_argument(
        "--start_timestamp",
        action="store",
        required=True,
        type=str,
        help="Beginning of the downloaded period",
    )
    parser.add_argument(
        "--end_timestamp",
        action="store",
        required=True,
        type=str,
        help="End of the downloaded period",
    )
    parser.add_argument(
        "--exchange_id",
        action="store",
        required=True,
        type=str,
        help="Name of exchange to download data from",
    )
    parser.add_argument(
        "--universe",
        action="store",
        required=True,
        type=str,
        help="Trade universe to download data for",
    )
    return parser


CCXT_EXCHANGE = "CcxtExchange"
TALOS_EXCHANGE = "TalosExchange"


def download_realtime_for_one_exchange(
    args: argparse.Namespace, exchange_class: Any
) -> None:
    """
    Helper function for encapsulating common logic for downloading exchange
    data.

    :param args: arguments passed on script run
    :param exchange_class: which exchange is used in script run
    """
    # Initialize exchange class and prepare additional args, if any.
    # Every exchange can potentially have a specific set of init args.
    additional_args = []
    if exchange_class.__name__ == CCXT_EXCHANGE:
        # Initialize CCXT with `exchange_id`.
        exchange = exchange_class(args.exchange_id)
        vendor = "CCXT"
    elif exchange_class.__name__ == TALOS_EXCHANGE:
        # Unlike CCXT, Talos is initialized with `api_stage`.
        exchange = exchange_class(args.api_stage)
        vendor = "Talos"
        additional_args.append(args.exchange_id)
    else:
        hdbg.dfatal(f"Unsupported `{exchange_class.__name__}` exchange!")
    # Load currency pairs.
    universe = imvcounun.get_vendor_universe(vendor, version=args.universe)
    currency_pairs = universe[args.exchange_id]
    # Connect to database.
    env_file = imvimlita.get_db_env_path(args.db_stage)
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    connection = hsql.get_connection(*connection_params)
    # Connect to S3 filesystem, if provided.
    if args.aws_profile:
        fs = hs3.get_s3fs(args.aws_profile)
    # Load DB table to work with
    db_table = args.db_table
    # Generate a query to remove duplicates.
    dup_query = hsql.get_remove_duplicates_query(
        table_name=db_table,
        id_col_name="id",
        column_names=["timestamp", "exchange_id", "currency_pair"],
    )
    # Convert timestamps.
    start_timestamp = pd.Timestamp(args.start_timestamp)
    end_timestamp = pd.Timestamp(args.end_timestamp)
    # Download data for specified time period.
    for currency_pair in currency_pairs:
        # Currency pair used for getting data from exchange should not be used
        # as column value as it can slightly differ.
        if exchange_class.__name__ == CCXT_EXCHANGE:
            currency_pair_for_download = currency_pair.replace("_", "/")
        elif exchange_class.__name__ == TALOS_EXCHANGE:
            currency_pair_for_download = currency_pair.replace("_", "-")
        data = exchange.download_ohlcv_data(
            currency_pair_for_download,
            *additional_args,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        # Assign pair and exchange columns.
        data["currency_pair"] = currency_pair
        data["exchange_id"] = args.exchange_id
        # Get timestamp of insertion in UTC.
        data["knowledge_timestamp"] = hdateti.get_current_time("UTC")
        # Insert data into the DB.
        hsql.execute_insert_query(
            connection=connection,
            obj=data,
            table_name=db_table,
        )
        # Save data to S3 bucket.
        if args.s3_path:
            # Get file name.
            file_name = (
                currency_pair
                + "_"
                + hdateti.get_current_timestamp_as_string("UTC")
                + ".csv"
            )
            path_to_file = os.path.join(args.s3_path, args.exchange_id, file_name)
            # Save data to S3 filesystem.
            with fs.open(path_to_file, "w") as f:
                data.to_csv(f, index=False)
        # Remove duplicated entries.
        connection.cursor().execute(dup_query)


def download_historical_data(
    args: argparse.Namespace, exchange_class: Any
) -> None:
    """
    Helper function for encapsulating common logic for downloading 
    historical exchange data.

    :param args: arguments passed on script run
    :param exchange_class: which exchange class is used in script run
     e.g. "CcxtExchange" or "TalosExchange"
    """
    # Initialize exchange class and prepare additional args, if any.
    # Every exchange can potentially have a specific set of init args.
    additional_args = []
    if exchange_class.__name__ == CCXT_EXCHANGE:
        # Initialize CCXT with `exchange_id`.
        exchange = exchange_class(args.exchange_id)
        vendor = "CCXT"
    elif exchange_class.__name__ == TALOS_EXCHANGE:
        # Unlike CCXT, Talos is initialized with `api_stage`.
        exchange = exchange_class(args.api_stage)
        vendor = "Talos"
        additional_args.append(args.exchange_id)
    else:
        hdbg.dfatal(f"Unsupported `{exchange_class.__name__}` exchange!")
    # Load currency pairs.
    universe = imvcounun.get_vendor_universe(vendor, version=args.universe)
    currency_pairs = universe[args.exchange_id]
    # Convert timestamps.
    end_timestamp = pd.Timestamp(args.end_timestamp)
    start_timestamp = pd.Timestamp(args.start_timestamp)
    path_to_exchange = os.path.join(args.s3_path, args.exchange_id)
    for currency_pair in currency_pairs:
        # Currency pair used for getting data from exchange should not be used
        # as column value as it can slightly differ.
        if exchange_class.__name__ == CCXT_EXCHANGE:
            currency_pair_for_download = currency_pair.replace("_", "/")
        elif exchange_class.__name__ == TALOS_EXCHANGE:
            currency_pair_for_download = currency_pair.replace("_", "-")
        # Download OHLCV data.
        data = exchange.download_ohlcv_data(
            currency_pair_for_download,
            *additional_args,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        # Assign pair and exchange columns.
        # TODO(Nikola): Exchange id was missing and it is added additionally to
        #  match signature of other scripts.
        data["currency_pair"] = currency_pair
        data["exchange_id"] = args.exchange_id
        # Change index to allow calling add_date_partition_cols function on the dataframe.
        data = imvcdttrut.reindex_on_datetime(data, "timestamp")
        data, partition_cols = hparque.add_date_partition_columns(
            data, "by_year_month"
        )
        # Get current time of push to s3 in UTC.
        knowledge_timestamp = hdateti.get_current_time("UTC")
        data["knowledge_timestamp"] = knowledge_timestamp
        # Save data to S3 filesystem.
        # Saves filename as `uuid`.
        hparque.to_partitioned_parquet(
            data,
            ["currency_pair"] + partition_cols,
            path_to_exchange,
            partition_filename=None,
            aws_profile=args.aws_profile,
        )
        # Sleep between iterations is needed for CCXT.
        if exchange_class == CCXT_EXCHANGE:
            time.sleep(args.sleep_time)
    # Merge all new parquet into a single `data.parquet`.
    hparque.list_and_merge_pq_files(
        path_to_exchange, aws_profile=args.aws_profile
    )
