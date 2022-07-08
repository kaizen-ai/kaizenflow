"""
Implement common exchange download operations.

Import as:

import im_v2.common.data.extract.extract_utils as imvcdeexut
"""


import argparse
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import pandas as pd
import psycopg2

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparquet as hparque
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.common.data.extract.extractor as imvcdexex
import im_v2.common.data.transform.transform_utils as imvcdttrut
import im_v2.common.universe as ivcu
import im_v2.im_lib_tasks as imvimlita
from helpers.hthreading import timeout

_LOG = logging.getLogger(__name__)


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
        "--contract_type",
        action="store",
        required=False,
        default="spot",
        type=str,
        help="Type of contract, spot or futures"
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        required=False,
        help="Append data instead of overwriting it",
    )
    return parser


def add_periodical_download_args(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    """
    Add the command line options exchange download.
    """
    parser.add_argument(
        "--start_time",
        action="store",
        required=True,
        type=str,
        help="Timestamp when the download should start (e.g., '2022-05-03 00:40:00')",
    )
    parser.add_argument(
        "--stop_time",
        action="store",
        required=True,
        type=str,
        help="Timestamp when the script should stop (e.g., '2022-05-03 00:30:00')",
    )
    parser.add_argument(
        "--interval_min",
        type=int,
        help="Interval between download attempts, in minutes",
    )
    parser.add_argument(
        "--exchange_id",
        action="store",
        required=True,
        type=str,
        help="Name of exchange to download data from (e.g., 'binance')",
    )
    parser.add_argument(
        "--universe",
        action="store",
        required=True,
        type=str,
        help="Trading universe to download data for",
    )
    parser.add_argument(
        "--data_type",
        action="store",
        required=True,
        type=str,
        help="OHLCV, bid/ask or trades data.",
    )
    parser.add_argument(
        "--contract_type",
        action="store",
        required=False,
        default="spot",
        type=str,
        help="Type of contract, spot or futures"
    )
    return parser


# Time limit for each download execution.
TIMEOUT_SEC = 60

# Define the validation schema of the data.
SCHEMA = {
    'ask_price': 'float64',
    'ask_size': 'float64',
    'bid_price': 'float64',
    'bid_size': 'float64',
    'close': 'float64',
    'currency_pair': 'object',
    'exchange_id': 'object',
    'high': 'float64',
    'knowledge_timestamp': 'datetime64[ns, UTC]',
    'low': 'float64',
    'month': 'int32',
    'open': 'float64',
    'timestamp': 'int64',
    'volume': 'float64',
    'year': 'int32'
    }


def download_realtime_for_one_exchange(
    args: Dict[str, Any], exchange: imvcdexex.Extractor
) -> None:
    """
    Encapsulate common logic for downloading exchange data.

    :param args: arguments passed on script run
    :param exchange_class: which exchange is used in script run
    """
    # Load currency pairs.
    mode = "download"
    universe = ivcu.get_vendor_universe(exchange.vendor, mode, version=args["universe"])
    currency_pairs = universe[args["exchange_id"]]
    # Connect to database.
    env_file = imvimlita.get_db_env_path(args["db_stage"])
    try:
        # Connect with the parameters from the env file.
        connection_params = hsql.get_connection_info_from_env_file(env_file)
        connection = hsql.get_connection(*connection_params)
    except psycopg2.OperationalError:
        # Connect with the dynamic parameters (usually during tests).
        actual_details = hsql.db_connection_to_tuple(args["connection"])._asdict()
        connection_params = hsql.DbConnectionInfo(
            host=actual_details["host"],
            dbname=actual_details["dbname"],
            port=int(actual_details["port"]),
            user=actual_details["user"],
            password=actual_details["password"],
        )
        connection = hsql.get_connection(*connection_params)
    # Load DB table to work with
    db_table = args["db_table"]
    # Generate a query to remove duplicates.
    dup_query = hsql.get_remove_duplicates_query(
        table_name=db_table,
        id_col_name="id",
        column_names=["timestamp", "exchange_id", "currency_pair"],
    )
    # Convert timestamps.
    start_timestamp = pd.Timestamp(args["start_timestamp"])
    end_timestamp = pd.Timestamp(args["end_timestamp"])
    # Download data for specified time period.
    for currency_pair in currency_pairs:
        # Currency pair used for getting data from exchange should not be used
        # as column value as it can slightly differ.
        currency_pair_for_download = exchange.convert_currency_pair(currency_pair)
        # Download data.
        data = exchange.download_data(
            data_type=args["data_type"],
            currency_pair=currency_pair_for_download,
            exchange_id=args["exchange_id"],
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        # Assign pair and exchange columns.
        data["currency_pair"] = currency_pair
        data["exchange_id"] = args["exchange_id"]
        # Get timestamp of insertion in UTC.
        data["knowledge_timestamp"] = hdateti.get_current_time("UTC")
        # Insert data into the DB.
        hsql.execute_insert_query(
            connection=connection,
            obj=data,
            table_name=db_table,
        )
        # Remove duplicated entries.
        connection.cursor().execute(dup_query)
        # Save data to S3 bucket.
        if args["s3_path"]:
            # Connect to S3 filesystem.
            fs = hs3.get_s3fs(args["aws_profile"])
            # Get file name.
            file_name = (
                currency_pair
                + "_"
                + hdateti.get_current_timestamp_as_string("UTC")
                + ".csv"
            )
            path_to_file = os.path.join(
                args["s3_path"], args["exchange_id"], file_name
            )
            # Save data to S3 filesystem.
            with fs.open(path_to_file, "w") as f:
                data.to_csv(f, index=False)


@timeout(TIMEOUT_SEC)
def _download_realtime_for_one_exchange_with_timeout(
    args: Dict[str, Any],
    exchange_class: imvcdexex.Extractor,
    start_timestamp: datetime,
    end_timestamp: datetime,
) -> None:
    """
    Wrapper for download_realtime_for_one_exchange. Download data for given
    time range, raise Interrupt in case if timeout occured.

    :param args: arguments passed on script run
    :param start_timestamp: beginning of the downloaded period
    :param end_timestamp: end of the downloaded period
    """
    args["start_timestamp"], args["end_timestamp"] = (
        start_timestamp,
        end_timestamp,
    )
    _LOG.info(
        "Starting data download from: %s, till: %s",
        start_timestamp,
        end_timestamp,
    )
    download_realtime_for_one_exchange(args, exchange_class)


def download_realtime_for_one_exchange_periodically(
    args: Dict[str, Any], exchange: imvcdexex.Extractor
) -> None:
    """
    Encapsulate common logic for periodical exchange data download.

    :param args: arguments passed on script run
    :param exchange_class: which exchange is used in script run
    """
    # Time range for each download.
    time_window_min = 5
    # Check values.
    start_time = pd.Timestamp(args["start_time"])
    stop_time = pd.Timestamp(args["stop_time"])
    interval_min = args["interval_min"]
    hdbg.dassert_lte(
        1, interval_min, "interval_min: %s should be greater than 0", interval_min
    )
    hdbg.dassert_eq(start_time.tz, stop_time.tz)
    tz = start_time.tz
    hdbg.dassert_lt(datetime.now(tz), start_time, "start_time is in the past")
    hdbg.dassert_lt(start_time, stop_time, "stop_time is less than start_time")
    # Error will be raised if we miss full 5 minute window of data,
    # even if the next download succeeds, we don't recover all of the previous data.
    num_failures = 0
    max_num_failures = (
        time_window_min // interval_min + time_window_min % interval_min
    )
    # Delay start.
    iteration_start_time = start_time
    iteration_delay_sec = (
        iteration_start_time - datetime.now(tz)
    ).total_seconds()
    while (
        datetime.now(tz) + timedelta(seconds=iteration_delay_sec) < stop_time
        and num_failures < max_num_failures
    ):
        # Wait until next download.
        _LOG.info("Delay %s sec until next iteration", iteration_delay_sec)
        time.sleep(iteration_delay_sec)
        start_timestamp = iteration_start_time - timedelta(
            minutes=time_window_min
        )
        end_timestamp = datetime.now(tz)
        try:
            _download_realtime_for_one_exchange_with_timeout(
                args, exchange, start_timestamp, end_timestamp
            )
            # Reset failures counter.
            num_failures = 0
        except (KeyboardInterrupt, Exception) as e:
            num_failures += 1
            _LOG.error("Download failed %s", str(e))
            # Download failed.
            if num_failures >= max_num_failures:
                raise RuntimeError(
                    f"{max_num_failures} consecutive downloads were failed"
                ) from e
        # if the download took more than expected, we need to align on the grid.
        if datetime.now(tz) > iteration_start_time + timedelta(
            minutes=interval_min
        ):
            _LOG.error(
                "The download was not finished in %s minutes.", interval_min
            )
            iteration_delay_sec = 0
            # Download that will start after repeated one, should follow to the initial schedule.
            while datetime.now(tz) > iteration_start_time + timedelta(
                minutes=interval_min
            ):
                iteration_start_time = iteration_start_time + timedelta(
                    minutes=interval_min
                )
        # If download failed, but there is time before next download.
        elif num_failures > 0:
            _LOG.info("Start repeat download immediately.")
            iteration_delay_sec = 0
        else:
            download_duration_sec = (
                datetime.now(tz) - iteration_start_time
            ).total_seconds()
            # Calculate delay before next download.
            iteration_delay_sec = (
                iteration_start_time
                + timedelta(minutes=interval_min)
                - datetime.now(tz)
            ).total_seconds()
            # Add interval in order to get next download time.
            iteration_start_time = iteration_start_time + timedelta(
                minutes=interval_min
            )
            _LOG.info(
                "Successfully completed, iteration took %s sec",
                download_duration_sec,
            )


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
    data: pd.DataFrame,
    path_to_exchange: str,
    unit: str,
    aws_profile: Optional[str],
    data_type: str,
) -> None:
    """
    Save Parquet dataset.
    """
    # Update indexing and add partition columns.
    # TODO(Danya): Add `unit` as a parameter in the function.
    data = imvcdttrut.reindex_on_datetime(data, "timestamp", unit=unit)
    data, partition_cols = hparque.add_date_partition_columns(
        data, "by_year_month"
    )
    # Drop DB metadata columns.
    data = data.drop(
        ["end_download_timestamp"], axis=1, errors="ignore"
    )
    # Verify the schema of Dataframe.
    verify_schema(data)
    # Save filename as `uuid`, e.g.
    #  "16132792-79c2-4e96-a2a2-ac40a5fac9c7".
    hparque.to_partitioned_parquet(
        data,
        ["currency_pair"] + partition_cols,
        path_to_exchange,
        partition_filename=None,
        aws_profile=aws_profile,
    )
    mode = None
    if data_type == "ohlcv":
        mode = "ohlcv"
    # Merge all new parquet into a single `data.parquet`.
    hparque.list_and_merge_pq_files(
        path_to_exchange, aws_profile=aws_profile, drop_duplicates_mode=mode
    )


def download_historical_data(
    args: Dict[str, Any], exchange: imvcdexex.Extractor
) -> None:
    """
    Encapsulate common logic for downloading historical exchange data.

    :param args: arguments passed on script run
    :param exchange_class: which exchange class is used in script run
     e.g. "CcxtExtractor" or "TalosExtractor"
    """
    # Convert Namespace object with processing arguments to dict format.
    path_to_exchange = os.path.join(args["s3_path"], args["exchange_id"])
    # Verify that data exists for incremental mode to work.
    if args["incremental"]:
        hs3.dassert_path_exists(path_to_exchange, args["aws_profile"])
    elif not args["incremental"]:
        hs3.dassert_path_not_exists(path_to_exchange, args["aws_profile"])
    # Load currency pairs.
    mode = "download"
    universe = ivcu.get_vendor_universe(exchange.vendor, mode, version=args["universe"])
    currency_pairs = universe[args["exchange_id"]]
    # Convert timestamps.
    start_timestamp = pd.Timestamp(args["start_timestamp"])
    end_timestamp = pd.Timestamp(args["end_timestamp"])
    for currency_pair in currency_pairs:
        # Currency pair used for getting data from exchange should not be used
        # as column value as it can slightly differ.
        converted_currency_pair = exchange.convert_currency_pair(currency_pair)
        # Download data.
        data = exchange.download_data(
            args["data_type"],
            args["exchange_id"],
            converted_currency_pair,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        if data.empty:
            continue
        # Assign pair and exchange columns.
        data["currency_pair"] = currency_pair
        data["exchange_id"] = args["exchange_id"]
        # Get current time of download.
        knowledge_timestamp = hdateti.get_current_time("UTC")
        data["knowledge_timestamp"] = knowledge_timestamp
        # Save data to S3 filesystem.
        if args["file_format"] == "parquet":
            save_parquet(
                data,
                path_to_exchange,
                args["unit"],
                args["aws_profile"],
                args["data_type"],
            )
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


def verify_schema(data: pd.DataFrame) -> None:
    """
    Validate the columns types in the extracted data.

    :param data: the dataframe to verify
    """
    error_msg = []
    for column in data.columns:
        # Get the actual data type of the column.
        actual_type = str(data[column].dtype)
        # Extract the expected type of the column from the schema.
        expected_type = SCHEMA[column]
        # Compare types.
        if actual_type != expected_type:
            # Log the error.
             error_msg.append(f"Invalid dtype of `{column}` column: expected type `{expected_type}`, found `{actual_type}`")
    if error_msg:
        hdbg.dfatal(msg="\n".join())
    