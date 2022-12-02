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
import helpers.hsql as hsql
import im_v2.common.data.client as icdc
import im_v2.im_lib_tasks as imvimlita
import oms.oms_ccxt_utils as oomccuti

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
    parser.add_argument(
        "--stage",
        action="store",
        required=True,
        type=str,
        help="Stage to run at: local, preprod.",
    )
    parser.add_argument(
        "--secret_id",
        action="store",
        required=True,
        type=str,
        help="ID of the API Keys to use as they are stored in AWS SecretsManager.",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    _LOG.debug("Initializing broker.")
    # Get environment variables with login info.
    env_file = imvimlita.get_db_env_path("dev")
    # Get login info.
    connection_params = hsql.get_connection_info_from_env_file(env_file)
    # Login.
    connection = hsql.get_connection(*connection_params)
    # Remove table if it already exists.
    hsql.remove_table(connection, "example2_marketdata")
    # Initialize real-time market data.
    im_client = icdc.get_mock_realtime_client(connection)
    market_data = oomccuti.get_RealTimeImClientMarketData_example2(im_client)
    # Initialize CcxtBroker connected to testnet.
    exchange_id = args.exchange_id
    contract_type = args.contract_type
    stage = args.stage
    secret_id = args.secret_id
    broker = oomccuti.get_CcxtBroker_example1(
        market_data, exchange_id, contract_type, stage, secret_id
    )
    # Close all open positions.
    oomccuti.flatten_ccxt_account(broker, dry_run=False)


if __name__ == "__main__":
    _main(_parse())
