#!/usr/bin/env python
"""
Get CCXT trades from the exchange and save to JSON.

Example use:
# Get trades from binance from 2022-09-22 to 2022-09-23.
> oms/ccxt/scripts/get_ccxt_trades.py \
    --start_timestamp '2022-09-22' \
    --end_timestamp '2022-09-23' \
    --dst_dir '/shared_data/filled_orders/' \
    --exchange_id 'binance' \
    --contract_type 'futures' \
    --stage 'preprod' \
    --account_type 'trading' \
    --secrets_id '1' \
    --universe 'v7.1' \
    --incremental
"""

import argparse
import logging
import os

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hparser as hparser
import helpers.hsql as hsql
import im_v2.ccxt.data.client as icdcl
import im_v2.im_lib_tasks as imvimlita
import oms.ccxt.ccxt_broker_instances as occcbrin
import oms.ccxt.ccxt_broker_v1 as occcbrv1
import oms.ccxt.ccxt_filled_orders_reader as occforre
import oms.hsecrets as omssec

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--start_timestamp",
        action="store",
        required=True,
        type=str,
        help="Beginning of the time period, e.g. '2022-09-22'",
    )
    parser.add_argument(
        "--end_timestamp",
        action="store",
        required=True,
        type=str,
        help="Beginning of the time period, e.g. '2022-09-23'",
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        required=True,
        type=str,
        help="Location of the output JSON, e.g. '/shared_data/filled_orders/'",
    )
    parser.add_argument(
        "--exchange_id",
        action="store",
        required=True,
        type=str,
        help="Name of the exchange, e.g. 'binance'.",
    )
    parser.add_argument(
        "--contract_type",
        action="store",
        required=True,
        type=str,
        help="'futures' or 'spot'. Note: only futures contracts are supported.",
    )
    parser.add_argument(
        "--stage",
        type=str,
        required=True,
        help="Stage: 'local' for testnet environment, 'prod' for trading",
        action="store",
    )
    parser.add_argument(
        "--account_type",
        type=str,
        required=True,
        help="Stage: 'sandbox' for testnet account, 'trading' for trading account.",
        action="store",
    )
    parser.add_argument(
        "--universe",
        type=str,
        required=True,
        help="Version of the universe, e.g. 'v7.1'",
    )
    parser.add_argument(
        "--secrets_id",
        type=int,
        default=1,
        help="Integer ID of the secret key, 1 by default.",
    )
    parser.add_argument(
        "--incremental",
        help="Whether to overwrite the existing data in dst_dir",
        action="store_true",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Create dst dir incrementally.
    hio.create_dir(args.dst_dir, incremental=args.incremental)
    # Get argument for the broker initialization.
    exchange_id = args.exchange_id
    universe_version = args.universe
    stage = args.stage
    account_type = args.account_type
    portfolio_id = "get_fills_portfolio"
    contract_type = args.contract_type
    secrets_id = args.secrets_id
    secret_identifier = omssec.SecretIdentifier(
        exchange_id, stage, account_type, secrets_id
    )
    # Initialize market data for the Broker.
    #  Note: It is a mock required to initialize CcxtBroker class.
    env_file = imvimlita.get_db_env_path("dev")
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    connection = hsql.get_connection(*connection_params)
    table_name = "ccxt_ohlcv_futures"
    im_client = icdcl.CcxtSqlRealTimeImClient(
        universe_version, connection, table_name, resample_1min=False
    )
    market_data = occcbrin.get_RealTimeImClientMarketData_example2(im_client)
    # Initialize broker.
    broker = occcbrv1.CcxtBroker_v1(
        exchange_id,
        universe_version,
        stage,
        account_type,
        portfolio_id,
        contract_type,
        secret_identifier,
        strategy_id="SAU1",
        market_data=market_data,
    )
    #
    start_timestamp = pd.Timestamp(args.start_timestamp)
    end_timestamp = pd.Timestamp(args.end_timestamp)
    # Get all trades.
    trades = broker._get_ccxt_trades_for_time_period(
        start_timestamp, end_timestamp
    )
    # Save file.
    start_timestamp_str = start_timestamp.strftime("%Y%m%d-%H%M%S")
    end_timestamp_str = end_timestamp.strftime("%Y%m%d-%H%M%S")
    # Save data as JSON.
    json_dir = os.path.join(args.dst_dir, "json")
    hio.create_dir(json_dir, incremental=args.incremental)
    json_file_name = os.path.join(
        json_dir,
        f"trades_{start_timestamp_str}_{end_timestamp_str}_{secret_identifier}.json",
    )
    _LOG.debug("json_file_name=%s", json_file_name)
    hio.to_json(json_file_name, trades)
    # Save data as a CSV file.
    logs_reader = occforre.CcxtLogsReader(args.dst_dir)
    trades_dataframe = logs_reader._convert_ccxt_trades_json_to_dataframe(trades)
    csv_dir = os.path.join(args.dst_dir, "csv")
    hio.create_dir(csv_dir, incremental=args.incremental)
    csv_file_name = os.path.join(
        csv_dir,
        f"trades_{start_timestamp_str}_{end_timestamp_str}_{secret_identifier}.csv.gz",
    )
    _LOG.debug("csv_file_name=%s", csv_file_name)
    trades_dataframe.to_csv(csv_file_name)


if __name__ == "__main__":
    _main(_parse())
