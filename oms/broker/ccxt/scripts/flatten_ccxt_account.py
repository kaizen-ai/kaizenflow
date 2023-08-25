#!/usr/bin/env python
"""
Flatten all open positions in the account using CCXT.

Example use:

# Flatten futures live account and assert if some positions are not completely
# closed (i.e. holdings not brought to zero).
> oms/broker/ccxt/scripts/flatten_ccxt_account.py \
    --log_dir ./flatten_log \
    --exchange 'binance' \
    --contract_type 'futures' \
    --stage 'preprod' \
    --secret_id 4 \
    --assert_on_non_zero
"""
import argparse
import asyncio
import logging

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import oms.broker.ccxt.ccxt_broker_utils as obccbrut

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--exchange",
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
        action="store",
        required=True,
        type=str,
        help="Stage to run at: local, preprod, prod.",
    )
    parser.add_argument(
        "--secret_id",
        action="store",
        required=True,
        type=int,
        help="ID of the API Keys to use as they are stored in AWS SecretsManager.",
    )
    parser.add_argument(
        "--log_dir",
        action="store",
        type=str,
        required=True,
        help="Log dir to save open positions info.",
    )
    parser.add_argument(
        "--assert_on_non_zero_positions",
        action="store_true",
        required=False,
        help="Assert if the open positions failed to close completely.",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    args = vars(args)
    hdbg.init_logger(verbosity=args["log_level"], use_exec_path=True)
    exchange = args["exchange"]
    contract_type = args["contract_type"]
    stage = args["stage"]
    log_dir = args["log_dir"]
    secret_id = args["secret_id"]
    assert_on_non_zero_positions = args["assert_on_non_zero_positions"]
    broker = obccbrut.get_broker(exchange, contract_type, stage, secret_id, db_stage=args["stage"])
    obccbrut.describe_and_flatten_account(
        broker,
        exchange,
        contract_type,
        log_dir,
        assert_on_non_zero_positions=assert_on_non_zero_positions,
    )


if __name__ == "__main__":
    _main(_parse())
