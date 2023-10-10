#!/usr/bin/env python
"""
Use example:

# Create a buy TWAP order for 10000 DOGE/USDT.
> oms/broker/ccxt/scripts/submit_twap_orders.py \
    --currency_pair 'DOGE/USDT' \
    --volume 10000 \
    --execution_start '2023-01-18 12:00:00' \
    --execution_end '2023-01-18 12:10:00' \
    --execution_freq '1T'
"""

import argparse
import asyncio
import logging

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import im_v2.crypto_chassis.data.client as iccdc
import oms.broker.ccxt.ccxt_broker_instances as obccbrin
import oms.order.order as oordorde

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
        "--diff_num_shares",
        action="store",
        required=True,
        type=int,
        help="Amount of asset to include in the TWAP order.",
    )
    parser.add_argument(
        "--execution_start",
        action="store",
        required=True,
        type=str,
        help="When to start executing the order. By default = wall_clock + 1min.",
    )
    parser.add_argument(
        "--execution_end",
        action="store",
        required=True,
        type=str,
        help="When to stop executing the order. By default = wall_clock + 11min.",
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
    market_data = obccbrin.get_RealTimeImClientMarketData_example2(im_client)
    # Initialize Broker.
    exchange_id = "binance"
    stage = "preprod"
    secret_id = 3
    broker = obccbrin.get_CcxtBroker_example1(
        market_data, exchange_id, contract_type, stage, secret_id
    )
    # Generate TWAP order.
    currency_pair = args.currency_pair
    diff_num_shares = args.diff_num_shares
    execution_start = pd.Timestamp(args.execution_start)
    execution_end = pd.Timestamp(args.execution_end)
    execution_freq = args.execution_freq
    order = oordorde.Order(
        creation_timestamp=pd.Timestamp.now(),
        asset_id=broker.ccxt_symbol_to_asset_id_mapping[currency_pair],
        type_="price@twap",
        start_timestamp=execution_start,
        end_timestamp=execution_end,
        curr_num_shares=0,
        diff_num_shares=diff_num_shares,
        order_id=1,
    )
    # Submit the orders.
    _LOG.debug("Executing from %s to %s", execution_start, execution_end)
    _ = asyncio.run(
        broker.submit_twap_orders([order], execution_freq=execution_freq)
    )


if __name__ == "__main__":
    exit_code = _main(_parse())
