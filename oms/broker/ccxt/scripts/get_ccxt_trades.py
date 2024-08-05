#!/usr/bin/env python
"""
Get CCXT trades from the exchange and save to JSON.

Example use:
# Get trades from binance from 2022-09-22 to 2022-09-23.
> oms/broker/ccxt/scripts/get_ccxt_trades.py \
    --start_timestamp '2023-09-22' \
    --end_timestamp '2023-09-23' \
    --log_dir '/shared_data/filled_orders/' \
    --secret_id '4' \
    --universe 'v7.4' \
    --exchange 'binance' \
    --contract_type 'futures' \
    --stage 'preprod'
"""

import argparse

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import oms.broker.ccxt.ccxt_broker_instances as obccbrin
import oms.broker.ccxt.ccxt_utils as obccccut
import oms.hsecrets.secret_identifier as ohsseide


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
    parser = obccccut.add_CcxtBroker_cmd_line_args(parser)
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    secret_identifier = ohsseide.SecretIdentifier(
        args.exchange, args.stage, "trading", args.secret_id
    )
    broker = obccbrin.get_CcxtBroker_exchange_only_instance1(
        args.universe, secret_identifier, args.log_dir, args.contract_type
    )
    #
    start_timestamp = pd.Timestamp(args.start_timestamp)
    end_timestamp = pd.Timestamp(args.end_timestamp)
    # Get all trades.
    trades = broker._get_ccxt_trades_for_time_period(
        start_timestamp, end_timestamp
    )
    wall_clock_time = broker._get_wall_clock_time
    broker._logger.log_ccxt_trades(wall_clock_time, trades)


if __name__ == "__main__":
    _main(_parse())
