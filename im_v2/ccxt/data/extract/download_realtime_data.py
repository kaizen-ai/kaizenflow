#!/usr/bin/env python
"""
Script to download OHLCV data from CCXT in real-time.

Use as:

# Download OHLCV data for universe 'v03', saving dev_stage:
> im_v2/ccxt/data/extract/download_realtime_data.py \
    --to_datetime '20211110-101100' \
    --from_datetime '20211110-101200' \
    --dst_dir 'ccxt/ohlcv/' \
    --data_type 'ohlcv' \
    --universe 'v03' \
    --db_stage 'dev' \
    --v DEBUG

Import as:

import im_v2.ccxt.data.extract.download_realtime_data as imvcdedrda
"""

import argparse
import collections
import logging
import os
import time
from typing import Any, Dict, List, NamedTuple, Optional, Union

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hparser as hparser
import helpers.hsql as hsql
import im_v2.ccxt.data.extract.exchange_class as imvcdeexcl
import im_v2.ccxt.universe.universe as imvccunun
import im_v2.im_lib_tasks as imvimlita

_LOG = logging.getLogger(__name__)

# TODO(Danya): Move instantiation outside.
def instantiate_exchange(
    exchange_id: str,
    ccxt_universe: Dict[str, List[str]],
) -> NamedTuple:
    """
    Create a tuple with exchange id, its class instance and currency pairs.

    :param exchange_id: CCXT exchange id
    :param ccxt_universe: CCXT trade universe
    :return: named tuple with exchange id and currencies
    """
    exchange_to_currency = collections.namedtuple(
        "ExchangeToCurrency",
        ["id", "instance", "currency_pairs"],
    )
    exchange_to_currency.id = exchange_id
    exchange_to_currency.instance = imvcdeexcl.CcxtExchange(exchange_id)
    exchange_to_currency.currency_pairs = ccxt_universe[exchange_id]
    return exchange_to_currency


def _download_data(
    start_timestamp: pd.Timestamp,
    end_timestamp: pd.Timestamp,
    data_type: str,
    exchange: NamedTuple,
    currency_pair: str,
) -> Union[pd.DataFrame, Dict[str, Any]]:
    """
    Download order book or OHLCV data.

    :param start_timestamp: start of time period, e.g. `pd.Timestamp("2021-01-01")`
    :param end_timestamp: end of time period, e.g. `pd.Timestamp("2021-01-01")`
    :param data_type: 'ohlcv' or 'orderbook'
    :param exchange: exchange instance
    :param currency_pair: currency pair, e.g. 'BTC_USDT'
    :return: downloaded data
    """
    # Download 5 latest OHLCV candles.
    if data_type == "ohlcv":
        data = exchange.instance.download_ohlcv_data(
            currency_pair.replace("_", "/"),
            start_datetime=start_timestamp,
            end_datetime=end_timestamp,
        )
        # Assign pair and exchange columns.
        data["currency_pair"] = currency_pair
        data["exchange_id"] = exchange.id
    elif data_type == "orderbook":
        # Download current state of the orderbook.
        data = exchange.instance.download_order_book(currency_pair)
    else:
        hdbg.dfatal(
            "'%s' data type is not supported. Supported data types: 'ohlcv', 'orderbook'",
            data_type,
        )
    return data


def _save_data_on_disk(
    data_type: str,
    dst_dir: str,
    data: Union[pd.DataFrame, Dict[str, Any]],
    exchange: NamedTuple,
    currency_pair: str,
) -> None:
    """
    Save downloaded data to disk.

    :param data_type: 'ohlcv' or 'orderbook'
    :param dst_dir: directory to save to
    :param data: downloaded data
    :param exchange: exchange instance
    :param currency_pair: currency pair, e.g. 'BTC_USDT'
    """
    current_datetime = hdateti.get_current_time("ET")
    if data_type == "ohlcv":
        file_name = f"{exchange.id}_{currency_pair}_{current_datetime}.csv.gz"
        full_path = os.path.join(dst_dir, file_name)
        data.to_csv(full_path, index=False, compression="gzip")
    elif data_type == "orderbook":
        file_name = (
            f"orderbook_{exchange.id}_"
            f"{currency_pair}_"
            f"{hdateti.get_current_timestamp_as_string('ET')}.json"
        )
        full_path = os.path.join(dst_dir, file_name)
        hio.to_json(full_path, data)
    else:
        hdbg.dfatal(
            "'%s' data type is not supported. Supported data types: 'ohlcv', 'orderbook'",
            data_type,
        )


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--to_datetime",
        action="store",
        required=True,
        type=str,
        help="End of the downloaded period",
    )
    parser.add_argument(
        "--from_datetime",
        action="store",
        required=True,
        type=str,
        help="Beginning of the downloaded period",
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        required=True,
        type=str,
        help="Folder to save copies of data to",
    )
    parser.add_argument(
        "--data_type",
        action="store",
        required=True,
        type=str,
        help="Type of data to load, 'ohlcv' or 'orderbook'",
    )
    parser.add_argument(
        "--universe",
        action="store",
        required=True,
        type=str,
        help="Trade universe to download data for",
    )
    parser.add_argument(
        "--db_stage",
        action="store",
        required=True,
        type=str,
        help="DB stage to use",
    )
    parser.add_argument("--incremental", action="store_true")
    parser = hparser.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Create the directory.
    hio.create_dir(args.dst_dir, incremental=args.incremental)
    # Connect to database.
    db_stage = args.db_stage
    env_file = imvimlita.get_db_env_path(db_stage)
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    connection = hsql.get_connection(*connection_params)
    # Load universe.
    universe = imvccunun.get_trade_universe(args.universe)
    exchange_ids = universe["CCXT"].keys()
    # Build mappings from exchange ids to classes and currencies.
    exchanges = []
    for exchange_id in exchange_ids:
        exchanges.append(instantiate_exchange(exchange_id, universe["CCXT"]))
    # Construct table name.
    table_name = f"ccxt_{args.data_type}"
    # Generate a query to remove duplicates.
    dup_query = hsql.get_remove_duplicates_query(
        table_name=table_name,
        id_col_name="id",
        column_names=["timestamp", "exchange_id", "currency_pair"],
    )
    # Convert timestamps.
    end = pd.Timestamp(args.to_datetime)
    start = pd.Timestamp(args.from_datetime)
    # Download data for specified time period.
    for exchange in exchanges:
        for currency_pair in exchange.currency_pairs:
            pair_data = _download_data(
                start, end, args.data_type, exchange, currency_pair
            )
            # Save to disk.
            _save_data_on_disk(
                args.data_type, args.dst_dir, pair_data, exchange, currency_pair
            )
            hsql.execute_insert_query(
                connection=connection,
                obj=pair_data,
                table_name=table_name,
            )
            # Drop duplicates inside the table.
            connection.cursor().execute(dup_query)
            # Sleep to prevent interruption by exchanges' API.
            time.sleep(2)


if __name__ == "__main__":
    _main(_parse())
