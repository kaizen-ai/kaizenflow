#!/usr/bin/env python
"""
Import as:

import oms.submit_twap_orders as osutword
"""

import argparse
import logging

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import im_v2.crypto_chassis.data.client as iccdc
import oms.oms_ccxt_utils as oomccuti

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--currency_pair",
        action="store",
        required=True,
        type=str,
        help="Name of the currency pair in Binance format, e.g. 'BTC/USDT'.",
    )
    parser.add_argument(
        "--volume",
        action="store",
        required=True,
        type=str,
        help="Amount of asset to include in the TWAP order.",
    )
    parser.add_argument(
        "--side",
        action="store",
        required=True,
        type=str,
        help="'buy' or 'sell'.",
    )
    parser.add_argument(
        "--execution_start",
        action="store",
        required=True,
        type=str,
        help="When to start executing the order.",
    )
    parser.add_argument(
        "--execution_end",
        action="store",
        required=True,
        type=str,
        help="When to stop executing the order.",
    )
    parser.add_argument(
        "--execution_freq",
        action="store",
        required=True,
        type=str,
        help="Frequency of order placement as a string, e.g. '1T'",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Get IM client for bid/ask data.
    # TODO(Danya): get latest universe, provide contract type as argument.
    # TODO(Danya): pass full symbols as arguments.
    universe_version = "v3"
    resample_1min = False
    contract_type = "futures"
    tag = "resampled_1min"
    im_client = iccdc.get_CryptoChassisHistoricalPqByTileClient_example2(
        universe_version, resample_1min, contract_type, tag
    )
    # Initialize Market Data.
    market_data = oomccuti.get_RealTimeImClientMarketData_example2(im_client)
    # Initialize Broker.
    exchange_id = "binance"
    stage = "preprod"
    secret_id = 2
    broker = oomccuti.get_CcxtBroker_example1(
        market_data, exchange_id, contract_type, stage, secret_id
    )
    #
    currency_pair = args.currency_pair
    volume = args.volume
    side = args.side
    execution_start = pd.Timestamp(args.execution_start)
    execution_end = pd.Timestamp(args.execution_end)
    execution_freq = args.execution_freq
    _ = broker.create_twap_orders(
        currency_pair,
        volume,
        side,
        execution_start,
        execution_end,
        execution_freq,
    )
    # TODO(Danya): Add logging.


if __name__ == "__main__":
    _main(_parse())