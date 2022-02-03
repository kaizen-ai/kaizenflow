#!/usr/bin/env python
"""
Script to download OHLCV data for a single exchange from CCXT.

Use as:

# Download OHLCV data for binance 'v03', saving dev_stage:
> im_v2/ccxt/data/extract/download_realtime_for_one_exchange.py \
    --to_datetime '20211110-101100' \
    --from_datetime '20211110-101200' \
    --exchange_id 'binance' \
    --universe 'v03' \
    --db_stage 'dev' \

Import as:

import im_v2.ccxt.data.extract.download_realtime_for_one_exchange as imvcdedrfoe
"""

import argparse
import logging
import os

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hs3 as hs3
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
    parser.add_argument(
        "--db_table",
        action="store",
        required=False,
        default="ccxt_ohlcv",
        type=str,
        help="(Optional) DB table to use, default: 'ccxt_ohlcv'",
    )
    parser.add_argument(
        "s3_bucket",
        action="store",
        required=False,
        default=None,
        type=str,
        help="Name of S3 bucket to save copy of the data to",
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
    # Connect to bucket, if provided.
    if args.s3_bucket:
        hs3.get_s3fs("ck")
        s3_path = f"s3://{args.s3_bucket}/{args.exchange_id}/"
    # Initialize exchange class.
    exchange = imvcdeexcl.CcxtExchange(args.exchange_id)
    # Load currency pairs.
    universe = imvccunun.get_trade_universe(args.universe)
    currency_pairs = universe["CCXT"][args.exchange_id]
    # Load DB table to work with
    db_table = args.db_table
    # Generate a query to remove duplicates.
    dup_query = hsql.get_remove_duplicates_query(
        table_name=db_table,
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
        # Insert data into the DB.
        hsql.execute_insert_query(
            connection=connection,
            obj=data,
            table_name=db_table,
        )
        # Save data to S3 bucket.
        if args.s3_bucket:
            # bytes_to_write = data.to_csv(None).encode()
            file_name = hdateti.get_current_time("UTC") + ".csv.gz"
            path_to_file = os.path.join(s3_path, file_name)
            data.to_csv(path_to_file, index=False, compression="gzip")
        # Remove duplicated entries.
        connection.cursor().execute(dup_query)


if __name__ == "__main__":
    _main(_parse())
