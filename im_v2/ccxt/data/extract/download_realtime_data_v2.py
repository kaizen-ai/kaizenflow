#!/usr/bin/env python
"""
Script to download OHLCV data for a single exchange from CCXT.

Use as:

# Download OHLCV data for binance 'v03', saving dev_stage:
> im_v2/ccxt/data/extract/download_realtime_data.py \
    --to_datetime '20211110-101100' \
    --from_datetime '20211110-101200' \
    --exchange_id 'binance' \
    --universe 'v03' \
    --db_stage 'dev' \
    --v DEBUG

Import as:

import im_v2.ccxt.data.extract.download_realtime_data_v2 as imvcdedrdv
"""

import argparse
import logging

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hsql as hsql
import im_v2.ccxt.data.extract.exchange_class as imvcdeexcl
import im_v2.ccxt.universe.universe as imvccunun
import im_v2.im_lib_tasks as imvimlita

_LOG = logging.getLogger(__name__)


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
    # Connect to database.
    env_file = imvimlita.get_db_env_path(args.db_stage)
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    connection = hsql.get_connection(*connection_params)
    # Initialize exchange class.
    exchange = imvcdeexcl.CcxtExchange(args.exchange_id)
    # Load currency pairs.
    universe = imvccunun.get_trade_universe(args.universe)
    currency_pairs = universe[args.exchange_id]
    # Generate a query to remove duplicates.
    dup_query = hsql.get_remove_duplicates_query(
        table_name="ccxt_ohlcv",
        id_col_name="id",
        column_names=["timestamp", "exchange_id", "currency_pair"],
    )
    # Convert timestamps.
    end = pd.Timestamp(args.to_datetime)
    start = pd.Timestamp(args.from_datetime)
    # Download data for specified time period.
    for currency_pair in currency_pairs:
        data = exchange.download_ohlcv_data(
            currency_pair.replace("_", "/"),
            start_datetime=start,
            end_datetime=end,
        )
        # Assign pair and exchange columns.
        data["currency_pair"] = currency_pair
        data["exchange_id"] = args.exchange_id
        # Remove duplicated entries.
        connection.cursor().execute(dup_query)


if __name__ == "__main__":
    _main(_parse())
