"""
Implement common exchange download operations.

Import as:

import im_v2.common.data.extract.extract_utils as imvcdeexut
"""


import argparse
import os
import time
from typing import Any, Optional

import pandas as pd
import psycopg2

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparquet as hparque
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.common.data.transform.transform_utils as imvcdttrut
import im_v2.common.universe as ivcu
import im_v2.im_lib_tasks as imvimlita


def add_exchange_download_args(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    """
    Add the command line options exchange download.
    """
    parser.add_argument(
        "--start_timestamp",
        required=True,
        action="store",
        type=str,
        help="Beginning of the downloaded period",
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
    parser.add_argument(
        "--end_timestamp",
        action="store",
        required=False,
        type=str,
        help="End of the downloaded period",
    )
    parser.add_argument(
        "--file_format",
        action="store",
        required=False,
        default="parquet",
        type=str,
        help="File format to save files on disk",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        required=False,
        help="Append data instead of overwriting it",
    )
    return parser


CCXT_EXCHANGE = "CcxtExchange"
TALOS_EXCHANGE = "TalosExchange"
CRYPTO_CHASSIS_EXCHANGE = "CryptoChassisExchange"


def download_realtime_for_one_exchange(
    args: argparse.Namespace, exchange_class: Any
) -> None:
    """
    Encapsulate common logic for downloading exchange data.

    :param args: arguments passed on script run
    :param exchange_class: which exchange is used in script run
    """
    # Initialize exchange class and prepare additional args, if any.
    # Every exchange can potentially have a specific set of init args.
    additional_args = []
    # TODO(Nikola): Unify exchange initialization as separate function CMTask #1776.
    if exchange_class.__name__ == CCXT_EXCHANGE:
        # Initialize CCXT with `exchange_id`.
        exchange = exchange_class(args.exchange_id)
        vendor = "CCXT"
    elif exchange_class.__name__ == TALOS_EXCHANGE:
        # Unlike CCXT, Talos is initialized with `api_stage`.
        exchange = exchange_class(args.api_stage)
        vendor = "talos"
        additional_args.append(args.exchange_id)
    else:
        hdbg.dfatal(f"Unsupported `{exchange_class.__name__}` exchange!")
    # Load currency pairs.
    universe = ivcu.get_vendor_universe(vendor, version=args.universe)
    currency_pairs = universe[args.exchange_id]
    # Connect to database.
    env_file = imvimlita.get_db_env_path(args.db_stage)
    try:
        # Connect with the parameters from the env file.
        connection_params = hsql.get_connection_info_from_env_file(env_file)
        connection = hsql.get_connection(*connection_params)
    except psycopg2.OperationalError:
        # Connect with the dynamic parameters (usually during tests).
        actual_details = hsql.db_connection_to_tuple(args.connection)._asdict()
        connection_params = hsql.DbConnectionInfo(
            host=actual_details["host"],
            dbname=actual_details["dbname"],
            port=int(actual_details["port"]),
            user=actual_details["user"],
            password=actual_details["password"],
        )
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
        currency_pair_for_download = exchange_class.convert_currency_pair(
            currency_pair
        )
        # Download data.
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


def save_csv(
    data: pd.DataFrame,
    exchange_folder_path: str,
    currency_pair: str,
    incremental: bool,
    aws_profile: Optional[str],
) -> None:
    """
    Save extracted data to .csv.gz.

    :param data: newly extracted data to save as .csv.gz file
    :param exchange_folder_path: path where to save the data
    :param currency_pair: currency pair, e.g. "BTC_USDT"
    :param incremental: update existing file instead of overwriting
    """
    full_target_path = os.path.join(
        exchange_folder_path, f"{currency_pair}.csv.gz"
    )
    if incremental:
        hs3.dassert_path_exists(full_target_path, aws_profile)
        original_data = pd.read_csv(full_target_path)
        # Append new data and drop duplicates.
        hdbg.dassert_is_subset(data.columns, original_data.columns)
        data = data[original_data.columns.to_list()]
        data = pd.concat([original_data, data])
        # Drop duplicates on non-metadata columns.
        metadata_columns = ["end_download_timestamp", "knowledge_timestamp"]
        non_metadata_columns = data.drop(
            metadata_columns, axis=1, errors="ignore"
        ).columns.to_list()
        data = data.drop_duplicates(subset=non_metadata_columns)
    data.to_csv(full_target_path, index=False, compression="gzip")


def save_parquet(
    data: pd.DataFrame, path_to_exchange: str, aws_profile: Optional[str]
) -> None:
    """
    Save Parquet dataset.
    """
    # Update indexing and add partition columns.
    data = imvcdttrut.reindex_on_datetime(data, "timestamp")
    data, partition_cols = hparque.add_date_partition_columns(
        data, "by_year_month"
    )
    # Save filename as `uuid`, e.g.
    #  "16132792-79c2-4e96-a2a2-ac40a5fac9c7".
    hparque.to_partitioned_parquet(
        data,
        ["currency_pair"] + partition_cols,
        path_to_exchange,
        partition_filename=None,
        aws_profile=aws_profile,
    )
    # Merge all new parquet into a single `data.parquet`.
    hparque.list_and_merge_pq_files(path_to_exchange, aws_profile=aws_profile)


def download_historical_data(
    args: argparse.Namespace, exchange_class: Any
) -> None:
    """
    Encapsulate common logic for downloading historical exchange data.

    :param args: arguments passed on script run
    :param exchange_class: which exchange class is used in script run
     e.g. "CcxtExchange" or "TalosExchange"
    """
    # Convert Namespace object with processing arguments to dict format.
    args = vars(args)
    path_to_exchange = os.path.join(args["s3_path"], args["exchange_id"])
    # Verify that data exists for incremental mode to work.
    if args["incremental"]:
        hs3.dassert_path_exists(path_to_exchange, args["aws_profile"])
    elif not args["incremental"]:
        hs3.dassert_path_not_exists(path_to_exchange, args["aws_profile"])
    # Initialize exchange class.
    # Every exchange can potentially have a specific set of init args.
    if exchange_class.__name__ == CCXT_EXCHANGE:
        # Initialize CCXT with `exchange_id`.
        exchange = exchange_class(args["exchange_id"])
        vendor = "CCXT"
        data_type = "ohlcv"
    elif exchange_class.__name__ == TALOS_EXCHANGE:
        # Unlike CCXT, Talos is initialized with `api_stage`.
        exchange = exchange_class(args["api_stage"])
        vendor = "talos"
        data_type = "ohlcv"
    elif exchange_class.__name__ == CRYPTO_CHASSIS_EXCHANGE:
        exchange = exchange_class()
        vendor = "crypto_chassis"
        data_type = "market_depth"
    else:
        hdbg.dfatal(f"Unsupported `{exchange_class.__name__}` exchange!")
    # Load currency pairs.
    universe = ivcu.get_vendor_universe(vendor, version=args["universe"])
    currency_pairs = universe[args["exchange_id"]]
    # Convert timestamps.
    args["end_timestamp"] = pd.Timestamp(args["end_timestamp"])
    args["start_timestamp"] = pd.Timestamp(args["start_timestamp"])
    for currency_pair in currency_pairs:
        # Currency pair used for getting data from exchange should not be used
        # as column value as it can slightly differ.
        args["currency_pair"] = exchange.convert_currency_pair(currency_pair)
        # Download data.
        data = exchange.download_data(data_type, **args)
        if data.empty:
            continue
        # Assign pair and exchange columns.
        # TODO(Nikola): Exchange id was missing and it is added additionally to
        #  match signature of other scripts.
        data["currency_pair"] = currency_pair
        data["exchange_id"] = args["exchange_id"]
        # Change index to allow calling add_date_partition_cols function on the dataframe.
        # TODO(Danya): Move to parquet!
        # Get current time of push to s3 in UTC.
        knowledge_timestamp = hdateti.get_current_time("UTC")
        data["knowledge_timestamp"] = knowledge_timestamp
        # Save data to S3 filesystem.
        if args["file_format"] == "parquet":
            save_parquet(data, path_to_exchange, args["aws_profile"])
        elif args["file_format"] == "csv":
            save_csv(
                data,
                path_to_exchange,
                currency_pair,
                args["incremental"],
                args["aws_profile"],
            )
        else:
            hdbg.dfatal(f"Unsupported `{args['file_format']}` format!")
        # Sleep between iterations is needed for CCXT.
        if exchange_class == CCXT_EXCHANGE:
            time.sleep(args["sleep_time"])
