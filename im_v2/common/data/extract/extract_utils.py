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
from typing import Any, Type, Union

import pandas as pd
import psycopg2

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparquet as hparque
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.ccxt.data.extract.exchange_class as imvcdeexcl
import im_v2.common.data.transform.transform_utils as imvcdttrut
import im_v2.common.universe as ivcu
import im_v2.im_lib_tasks as imvimlita
import im_v2.talos.data.extract.exchange_class as imvtdeexcl
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
    return parser


CCXT_EXCHANGE = "CcxtExchange"
TALOS_EXCHANGE = "TalosExchange"
CRYPTO_CHASSIS_EXCHANGE = "CryptoChassisExchange"
# Time limit for each download execution.
TIMEOUT_SEC = 60


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


@timeout(TIMEOUT_SEC)
def _download_realtime_for_one_exchange_with_timeout(
    args: argparse.Namespace,
    exchange_class: Type[
        Union[imvcdeexcl.CcxtExchange, imvtdeexcl.TalosExchange]
    ],
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
    args.start_timestamp, args.end_timestamp = start_timestamp, end_timestamp
    _LOG.info(
        "Starting data download from: %s, till: %s",
        start_timestamp,
        end_timestamp,
    )
    download_realtime_for_one_exchange(args, exchange_class)


def download_realtime_for_one_exchange_periodically(
    args: argparse.Namespace, exchange_class: Any
) -> None:
    """
    Encapsulate common logic for periodical exchange data download.

    :param args: arguments passed on script run
    :param exchange_class: which exchange is used in script run
    """
    # Time range for each download.
    time_window_min = 5
    # Check values.
    start_time = pd.Timestamp(args.start_time)
    stop_time = pd.Timestamp(args.stop_time)
    interval_min = args.interval_min
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
                args, exchange_class, start_timestamp, end_timestamp
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


def download_historical_data(
    args: argparse.Namespace, exchange_class: Any
) -> None:
    """
    Encapsulate common logic for downloading historical exchange data.

    :param args: arguments passed on script run
    :param exchange_class: which exchange class is used in script run
     e.g. "CcxtExchange" or "TalosExchange"
    """
    # Initialize exchange class.
    # Every exchange can potentially have a specific set of init args.
    if exchange_class.__name__ == CCXT_EXCHANGE:
        # Initialize CCXT with `exchange_id`.
        exchange = exchange_class(args.exchange_id)
        vendor = "CCXT"
        data_type = "ohlcv"
    elif exchange_class.__name__ == TALOS_EXCHANGE:
        # Unlike CCXT, Talos is initialized with `api_stage`.
        exchange = exchange_class(args.api_stage)
        vendor = "talos"
        data_type = "ohlcv"
    elif exchange_class.__name__ == CRYPTO_CHASSIS_EXCHANGE:
        exchange = exchange_class()
        vendor = "crypto_chassis"
        data_type = "market_depth"
    else:
        hdbg.dfatal(f"Unsupported `{exchange_class.__name__}` exchange!")
    # Load currency pairs.
    universe = ivcu.get_vendor_universe(vendor, version=args.universe)
    currency_pairs = universe[args.exchange_id]
    # Convert Namespace object with processing arguments to dict format.
    args = vars(args)
    # Convert timestamps.
    args["end_timestamp"] = pd.Timestamp(args["end_timestamp"])
    args["start_timestamp"] = pd.Timestamp(args["start_timestamp"])
    path_to_exchange = os.path.join(args["s3_path"], args["exchange_id"])
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
            aws_profile=args["aws_profile"],
        )
        # Sleep between iterations is needed for CCXT.
        if exchange_class == CCXT_EXCHANGE:
            time.sleep(args["sleep_time"])
    # Merge all new parquet into a single `data.parquet`.
    hparque.list_and_merge_pq_files(
        path_to_exchange, aws_profile=args["aws_profile"]
    )
