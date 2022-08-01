#!/usr/bin/env python
"""
Flatten all open positions in test account using CCXT.

Example use:

# Flatten futures account in Binance testnet.
> oms/flatten_ccxt_account.py \
    --exchange_id 'binance' \
    --contract_type 'futures'
"""
import argparse
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import oms.ccxt_broker as occxbrok
import oms.oms_utils as oomsutil

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
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
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _LOG.debug("Initializing broker.")
    # Initialize broker to connect to exchange.
    exchange_id = args.exchange_id
    contract_type = args.contract_type
    universe = "v7"
    mode = "test"
    portfolio_id = "ck_portfolio_id"
    broker = occxbrok.CcxtBroker(
        exchange_id, universe, mode, portfolio_id, contract_type
    )
    _LOG.info(
        "Flattening the %s account for %s exchange.", contract_type, exchange_id
    )
    # Close all open positions.
    oomsutil.flatten_ccxt_account(broker)


if __name__ == "__main__":
    _main(_parse())
