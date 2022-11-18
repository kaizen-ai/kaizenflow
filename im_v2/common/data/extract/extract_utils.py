"""
Implement common exchange download operations.

Import as:

import im_v2.common.data.extract.extract_utils as imvcdeexut
"""

import argparse
import asyncio
import logging
import os
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import psycopg2

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparquet as hparque
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.common.data.extract.extractor as ivcdexex
import im_v2.common.data.transform.transform_utils as imvcdttrut
import im_v2.common.db.db_utils as imvcddbut
import im_v2.common.universe as ivcu
import im_v2.im_lib_tasks as imvimlita
from helpers.hthreading import timeout

_LOG = logging.getLogger(__name__)

SUPPORTED_DOWNLOAD_METHODS = ["rest", "websocket"]
# Provides parameters for handling websocket download.
#  - sleep_between_iter_in_ms: time to sleep between iterations in miliseconds.
#  - max_buffer_size: specifies number of websocket
#    messages to cache before attempting DB insert.

WEBSOCKET_CONFIG = {
    "ohlcv": {
        # Buffer size is 0 for OHLCV because we want to insert after round of receival
        #  from websockets.
        "max_buffer_size": 0,
        "sleep_between_iter_in_ms": 60000,
    },
    "bid_ask": {"max_buffer_size": 250, "sleep_between_iter_in_ms": 200},
}


def _add_common_download_args(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    """
    Add command line arguments common to all downloaders.
    """
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
        choices=["ohlcv", "bid_ask", "trades"],
        help="OHLCV, bid/ask or trades data.",
    )
    parser.add_argument(
        "--contract_type",
        action="store",
        required=False,
        default="spot",
        type=str,
        help="Type of contract, spot or futures",
    )
    parser.add_argument(
        "--bid_ask_depth",
        action="store",
        required=False,
        type=int,
        help="Specifies depth of order book to \
            download (applies when data_type=bid_ask).",
    )
    return parser


def add_exchange_download_args(
    parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    """
    Add the command line options for exchange download.
    """
    parser = _add_common_download_args(parser)
    parser.add_argument(
        "--start_timestamp",
        required=False,
        action="store",
        type=str,
        help="Beginning of the downloaded period",
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
        choices=["csv", "parquet"],
        help="File format to save files on disk",
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
    parser = _add_common_download_args(parser)
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
        "--method",
        action="store",
        required=True,
        type=str,
        choices=SUPPORTED_DOWNLOAD_METHODS,
        help="Method used to download the data: rest (for HTTP REST based download), or websocket",
    )
    parser.add_argument(
        "--interval_min",
        type=int,
        help="Interval between download attempts, in minutes (applicable for --method=rest)",
    )
    return parser


# Time limit for each download execution.
TIMEOUT_SEC = 60

# Define the validation schema of the data.
DATASET_SCHEMA = {
    "ask_price": "float64",
    "ask_size": "float64",
    "bid_price": "float64",
    "bid_size": "float64",
    "close": "float64",
    "currency_pair": "object",
    "end_download_timestamp": "datetime64[ns, UTC]",
    "exchange_id": "object",
    "high": "float64",
    "knowledge_timestamp": "datetime64[ns, UTC]",
    "level": "int32",
    "low": "float64",
    "month": "int32",
    "number_of_trades": "int32",
    "open": "float64",
    "timestamp": "int64",
    "twap": "float64",
    "volume": "float64",
    "vwap": "float64",
    "year": "int32",
}


# TODO(Juraj): Refactor the method, divide into submethods
# by data type.
def download_realtime_for_one_exchange(
    args: Dict[str, Any], exchange: ivcdexex.Extractor
) -> None:
    """
    Encapsulate common logic for downloading exchange data.

    :param args: arguments passed on script run
    :param exchange_class: which exchange is used in script run
    """
    # Load currency pairs.
    mode = "download"
    universe = ivcu.get_vendor_universe(
        exchange.vendor, mode, version=args["universe"]
    )
    currency_pairs = universe[args["exchange_id"]]
    # Connect to database.
    env_file = imvimlita.get_db_env_path(args["db_stage"])
    try:
        # Connect with the parameters from the env file.
        connection_params = hsql.get_connection_info_from_env_file(env_file)
        db_connection = hsql.get_connection(*connection_params)
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
        db_connection = hsql.get_connection(*connection_params)
    # Load DB table to save data to.
    db_table = args["db_table"]
    data_type = args["data_type"]
    exchange_id = args["exchange_id"]
    bid_ask_depth = args.get("bid_ask_depth")
    if data_type == "ohlcv":
        # Convert timestamps.
        start_timestamp = pd.Timestamp(args["start_timestamp"])
        start_timestamp_as_unix = hdateti.convert_timestamp_to_unix_epoch(
            start_timestamp
        )
        end_timestamp = pd.Timestamp(args["end_timestamp"])
        end_timestamp_as_unix = hdateti.convert_timestamp_to_unix_epoch(
            end_timestamp
        )
    elif data_type == "bid_ask":
        # Make sure depth is set for bid/ask data.
        hdbg.dassert_lt(0, bid_ask_depth)
        # When downloading bid / ask data, CCXT returns the last data
        # ignoring the requested timestamp, so we set them to None.
        start_timestamp, end_timestamp = None, None
    else:
        raise ValueError(
            "Downloading for %s data_type is not implemented.", data_type
        )
    # Download data for specified time period.
    for currency_pair in currency_pairs:
        # Currency pair used for getting data from exchange should not be used
        # as column value as it can slightly differ.
        currency_pair_for_download = exchange.convert_currency_pair(currency_pair)
        # Download data.
        #  Note: timestamp arguments are ignored since historical data is absent
        #  from CCXT and only current state can be downloaded.
        data = exchange.download_data(
            data_type=data_type,
            currency_pair=currency_pair_for_download,
            exchange_id=exchange_id,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            depth=bid_ask_depth,
        )
        # Assign pair and exchange columns.
        data["currency_pair"] = currency_pair
        data["exchange_id"] = exchange_id
        # Add exchange specific filter.
        if exchange_id == "binance":
            data = imvcdttrut.remove_unfinished_ohlcv_bars(data)
        # Save data to the database.
        imvcddbut.save_data_to_db(
            data, data_type, db_connection, db_table, str(start_timestamp.tz)
        )
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
    exchange_class: ivcdexex.Extractor,
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


# TODO(Juraj): refactor names to get rid of "_for_one_exchange" part of the
#  functions' names since it spreads across the codebase. Docstring and the
# method signature should sufficiently explain what the function does.
async def _download_websocket_realtime_for_one_exchange_periodically(
    args: Dict[str, Any], exchange: ivcdexex.Extractor
) -> None:
    """
    Encapsulate common logic for periodical exchange data download using
    websocket based download.

    :param args: arguments passed on script run
    :param exchange: name of exchange used in script run
    """
    data_type = args["data_type"]
    # Time related arguments.
    start_time = pd.Timestamp(args["start_time"])
    stop_time = pd.Timestamp(args["stop_time"])
    tz = start_time.tz
    # Data related arguments
    universe = ivcu.get_vendor_universe(
        exchange.vendor, mode="download", version=args["universe"]
    )
    exchange_id = args["exchange_id"]
    currency_pairs = universe[exchange_id]
    # DB related arguments.
    # TODO(Juraj): create a common function to creation connection
    # and pass earlier in the call stack.
    env_file = imvimlita.get_db_env_path(args["db_stage"])
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    db_connection = hsql.get_connection(*connection_params)
    db_table = args["db_table"]
    for currency_pair in currency_pairs:
        await exchange.subscribe_to_websocket_data(
            data_type,
            exchange_id,
            currency_pair,
            # The following arguments are only applied for
            # the corresponding data type
            bid_ask_depth=args.get("bid_ask_depth"),
            since=hdateti.convert_timestamp_to_unix_epoch(pd.Timestamp.now(tz)),
        )
    _LOG.info("Subscribed to %s websocket data successfully", exchange_id)
    # In order not to bombard the database with many small insert operations
    # a buffer is created, its size is determined by the config specific to each
    # data type.
    data_buffer = []
    # Sync to the specified start_time.
    start_delay = max(0, ((start_time - datetime.now(tz)).total_seconds()))
    _LOG.info("Syncing with the start time, waiting for %s seconds", start_delay)
    # Exchange.sleep() method is needed instead of built in python time.sleep()
    #  to ensure websocket ping-pong messages are exchanged in a timely fashion.
    #  The method expects value in miliseconds.
    await exchange._async_exchange.sleep(start_delay * 1000)
    # Start data collection
    while pd.Timestamp.now(tz) < stop_time:
        iter_start_time = pd.Timestamp.now(tz)
        for curr_pair in currency_pairs:
            data_point = exchange.download_websocket_data(
                data_type, exchange_id, curr_pair
            )
            if data_point != None:
                data_buffer.append(data_point)
        # If the buffer is full or this is the last iteration, process and save buffered data.
        if (
            len(data_buffer) >= WEBSOCKET_CONFIG[data_type]["max_buffer_size"]
            or pd.Timestamp.now(tz) >= stop_time
        ):
            df = imvcdttrut.transform_raw_websocket_data(
                data_buffer, data_type, exchange_id
            )
            imvcddbut.save_data_to_db(
                df, data_type, db_connection, db_table, str(tz)
            )
            # Empty buffer after persisting the data.
            data_buffer = []
        # Determine actual sleep time needed based on the difference
        # between value set in config and actual time it took to complete
        # an iteration, this provides an "time align" mechanism.
        iter_length = (
            pd.Timestamp.now(tz) - iter_start_time
        ).total_seconds() * 1000
        actual_sleep_time = max(
            0,
            WEBSOCKET_CONFIG[data_type]["sleep_between_iter_in_ms"] - iter_length,
        )
        _LOG.info(
            "Iteration took %i ms, waiting between iterations for %i ms",
            iter_length,
            actual_sleep_time,
        )
        await exchange._async_exchange.sleep(actual_sleep_time)
    _LOG.info("Websocket download finished at %s", pd.Timestamp.now(tz))


def _download_rest_realtime_for_one_exchange_periodically(
    args: Dict[str, Any], exchange: ivcdexex.Extractor
) -> None:
    """
    Encapsulate common logic for periodical exchange data download using REST
    API based download.

    :param args: arguments passed on script run
    :param exchange: name of exchange used in script run
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
    tz = start_time.tz
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
        # The floor function does a cosmetic change to the parameters
        # so the logs are completely clear.
        start_timestamp = start_timestamp.floor("min")
        end_timestamp = pd.to_datetime(datetime.now(tz)).floor("min")
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
            _LOG.debug(
                "Initial start time before align `%s`.", iteration_start_time
            )
            iteration_delay_sec = 0
            # Download that will start after repeated one, should follow to the initial schedule.
            while datetime.now(tz) > iteration_start_time + timedelta(
                minutes=interval_min
            ):
                iteration_start_time = iteration_start_time + timedelta(
                    minutes=interval_min
                )
                _LOG.debug("Start time after align `%s`.", iteration_start_time)
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


def download_realtime_for_one_exchange_periodically(
    args: Dict[str, Any], exchange: ivcdexex.Extractor
) -> None:
    """
    Encapsulate common logic for periodical exchange data download via REST API
    or websocket.

    :param args: arguments passed on script run
    :param exchange: name of exchange used in script run
    """
    # Peform assertions common to all downloaders.
    start_time = pd.Timestamp(args["start_time"])
    stop_time = pd.Timestamp(args["stop_time"])
    hdbg.dassert_eq(start_time.tz, stop_time.tz)
    tz = start_time.tz
    hdbg.dassert_lt(datetime.now(tz), start_time, "start_time is in the past")
    hdbg.dassert_lt(start_time, stop_time, "stop_time is less than start_time")
    if args["method"] == "rest":
        _download_rest_realtime_for_one_exchange_periodically(args, exchange)
    elif args["method"] == "websocket":
        # Websockets work asynchronously, in order this outer function synchronous
        # the websocket download needs go be executed using asyncio.
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            _download_websocket_realtime_for_one_exchange_periodically(
                args, exchange
            )
        )
    else:
        raise ValueError(
            f"Method: {method} is not a valid method for periodical download, "
            + f"supported methods are: {SUPPORTED_DOWNLOAD_METHODS}"
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
    *,
    drop_columns: List[str] = ["end_download_timestamp"],
    mode: str = "list_and_merge",
) -> None:
    """
    Save Parquet dataset.
    """
    hdbg.dassert_in(mode, ["list_and_merge", "append"])
    # Update indexing and add partition columns.
    # TODO(Danya): Add `unit` as a parameter in the function.
    data = imvcdttrut.reindex_on_datetime(data, "timestamp", unit=unit)
    data, partition_cols = hparque.add_date_partition_columns(
        data, "by_year_month"
    )
    # Drop DB metadata columns.
    for column in drop_columns:
        data = data.drop(column, axis=1, errors="ignore")
    # Verify the schema of Dataframe.
    data = verify_schema(data)
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
    if mode == "list_and_merge":
        hparque.list_and_merge_pq_files(
            path_to_exchange,
            aws_profile=aws_profile,
            drop_duplicates_mode=data_type,
        )


def download_historical_data(
    args: Dict[str, Any], exchange: ivcdexex.Extractor
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
    universe = ivcu.get_vendor_universe(
        exchange.vendor, mode, version=args["universe"]
    )
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
            # If data_type = ohlcv, depth is ignored.
            depth=args.get("bid_ask_depth"),
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
                mode="append",
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


def verify_schema(data: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the columns types in the extracted data.

    :param data: the dataframe to verify
    """
    error_msg = []
    if data.isnull().values.any():
        _LOG.warning("Extracted Dataframe contains NaNs")
    for column in data.columns:
        # Extract the expected type of the column from the schema.
        #  Bid/ask columns have level suffix _l1, _l2 etc.
        #  for simplicity we store only base names in the schema
        #  table.
        column_re = re.sub("_l\d+$", "", column)
        expected_type = DATASET_SCHEMA[column_re]
        if (
            expected_type in ["float64", "int32", "int64"]
            and pd.to_numeric(data[column], errors="coerce").notnull().all()
        ):
            # Fix the type of numerical column.
            data[column] = data[column].astype(expected_type)
        # Get the actual data type of the column.
        actual_type = str(data[column].dtype)
        # Compare types.
        if actual_type != expected_type:
            # Log the error.
            error_msg.append(
                f"Invalid dtype of `{column}` column: expected type `{expected_type}`, found `{actual_type}`"
            )
    if error_msg:
        hdbg.dfatal(message="\n".join(error_msg))
    return data


def resample_rt_bid_ask_data_periodically(
    db_stage: str,
    src_table: str,
    dst_table: str,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> None:
    """
    Load raw bid/ask data from specified DB table every minute, resample to 1
    minute and insert back during a specified time interval <start_ts, end_ts>.

    :param db_stage: DB stage to use
    :param src_table: Source table to get raw data from
    :param dst_table: Destination table to insert resampled data into
    :param start_ts: start of the time interval
    :param end_ts: end of the time interval
    """
    # Peform timestamp assertions.
    hdbg.dassert_eq(start_ts.tz, end_ts.tz)
    tz = start_ts.tz
    hdbg.dassert_lt(datetime.now(tz), start_ts, "start_ts is in the past")
    hdbg.dassert_lt(start_ts, end_ts, "end_ts is less than start_time")
    env_file = imvimlita.get_db_env_path(db_stage)
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    db_connection = hsql.get_connection(*connection_params)
    tz = start_ts.tz
    start_delay = (start_ts - datetime.now(tz)).total_seconds()
    _LOG.info("Syncing with the start time, waiting for %s seconds", start_delay)
    time.sleep(start_delay)
    # Start resampling.
    while pd.Timestamp.now(tz) < end_ts:
        iter_start_time = pd.Timestamp.now(tz)
        df_raw = imvcddbut.fetch_last_minute_bid_ask_rt_db_data(
            db_connection, src_table, str(tz)
        )
        if df_raw.empty:
            _LOG.warning("Empty Dataframe, nothing to resample")
        else:
            df_resampled = imvcdttrut.transform_and_resample_bid_ask_rt_data(
                df_raw
            )
            imvcddbut.save_data_to_db(
                df_resampled,
                "bid_ask",
                db_connection,
                dst_table,
                str(start_ts.tz),
            )
        # Determine actual sleep time needed based on the difference
        # between value set in config and actual time it took to complete
        # an iteration, this provides an "time align" mechanism.
        iter_length = (pd.Timestamp.now(tz) - iter_start_time).total_seconds()
        actual_sleep_time = max(0, 60 - iter_length)
        _LOG.info(
            "Resampling iteration took %i s, waiting between iterations for %i s",
            iter_length,
            actual_sleep_time,
        )
        time.sleep(actual_sleep_time)
    _LOG.info("Resampling finished at %s", pd.Timestamp.now(tz))
