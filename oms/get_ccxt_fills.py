"""
Get filled orders from the exchange and save to JSON.

Example use:
# Get filled orders from binance from 2022-09-22 to 2022-09-23.
> oms/flatten_ccxt_account.py \
    --start_timestamp '2022-09-22' \
    --dst_dir '/shared_data/filled_orders/' \
    --exchange_id 'binance' \
    --contract_type 'futures' \
    --stage 'preprod' \
    --account_type 'trading' \
    --secrets_id '1' \
    --universe 'v7.1'
    --incremental

Import as:

import oms.get_ccxt_fills as ogeccfil
"""

import argparse
import logging
import os

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hio as hio
import helpers.hparser as hparser
import helpers.hsql as hsql
import im_v2.common.data.client as icdc
import im_v2.im_lib_tasks as imvimlita
import oms.ccxt_broker as occxbrok
import oms.oms_ccxt_utils as oomccuti
import oms.secrets as omssec

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
    im_client = icdc.get_mock_realtime_client(connection)
    market_data = oomccuti.get_RealTimeImClientMarketData_example2(im_client)
    # Initialize broker.
    broker = occxbrok.CcxtBroker(
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
    # Get timestamp of execution.
    current_timestamp = hdateti.get_current_timestamp_as_string("ET")
    # Get all trades.
    fills = broker.get_fills_since_timestamp(start_timestamp)
    # Save file.
    start_timestamp_str = start_timestamp.strftime("%Y%m%d-%H%M%S")
    file_name = os.path.join(args.dst_dir, f"fills_{start_timestamp_str}_{current_timestamp}.json")
    hio.to_json(file_name, fills)
