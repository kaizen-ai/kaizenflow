#!/usr/bin/env python
"""
Get current balance of the account in USDT.

Example use:

# Get total balance and save them to `shared_data` directory.
> oms/ccxt/scripts/get_ccxt_total_balance.py \
    --exchange 'binance' \
    --contract_type 'futures' \
    --stage 'preprod' \
    --secret_id 4 \
    --log_dir /shared_data/system_log_dir/
"""
import argparse
import logging
import os

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hparser as hparser
import helpers.hprint as hprint
import im_v2.common.data.client as icdc
import im_v2.common.db.db_utils as imvcddbut
import oms.ccxt.ccxt_broker_instances as occcbrin

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
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Initialize IM client.
    #  Data is not needed in this case, so connecting to `dev` DB.
    connection = imvcddbut.DbConnectionManager.get_connection("dev")
    im_client = icdc.get_mock_realtime_client("v1", connection)
    market_data = occcbrin.get_RealTimeImClientMarketData_example2(im_client)
    exchange = args.exchange
    contract_type = args.contract_type
    stage = args.stage
    secret_id = args.secret_id
    # Initialize broker.
    broker = occcbrin.get_CcxtBroker_example1(
        market_data, exchange, contract_type, stage, secret_id
    )
    # Get open positions.
    total_balance = broker.get_total_balance()
    _LOG.debug(hprint.to_str("total_balance"))
    # Save to provided log_dir.
    log_dir = args.log_dir
    hdbg.dassert_path_exists(log_dir)
    # Get enclosed balance directory.
    enclosing_dir = os.path.join(log_dir, "total_balance")
    hio.create_dir(enclosing_dir, incremental=True)
    # Get file name for balance, e.g.
    #  '/shared_data/system_log_dir/total_balance/binance_futures_balance_20230419_172022.json'
    timestamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%d_%H%M%S")
    file_name = f"{exchange}_{contract_type}_balance_{timestamp}.json"
    file_path = os.path.join(enclosing_dir, file_name)
    # Save.
    hio.to_json(file_path, total_balance)
    _LOG.debug("Total balance saved to %s", file_path)


if __name__ == "__main__":
    _main(_parse())
