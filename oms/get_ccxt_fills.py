"""
Get filled orders from the exchange and save to JSON.

Example use:
# Get filled orders from binance from 2022-09-22 to 2022-09-23.
> oms/flatten_ccxt_account.py \
    --start_timestamp '2022-09-22' \
    --end_timestamp '2022-09-23' \
    --dst_dir '/shared_data/filled_orders/' \
    --exchange_id 'binance' \
    --contract_type 'futures' \
    --stage 'preprod' \
    --account_type 'trading' \
    --secret_identifer '1'
"""

import logging
import helpers.hio as hio
import helpers.hdbg as hdbg
import helpers.hparser as hparser
import argparse
import os
import oms.secrets as omssec
import helpers.hdatetime as hdateti
import im_v2.common.data.client as icdc
import oms.ccxt_broker as occxbrok
import helpers.hsecrets as s
import ccxt
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
        help="End of the time period, e.g. '2022-09-23'",
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
    # Initialize exchange.
    secrets_id = str(args.secret_identifier)
    exchange_params = hsecret.get_secret(secrets_id)
    if args.contract_type == "futures":
        exchange_params["options"] = {"defaultType": "future"}
    ccxt_exchange = getattr(ccxt, args.exchange_id)
    exchange = ccxt_exchange(exchange_params)
    # Get all trades.
    # File name as fills_{timestamp-timestamp}.json
    # Save.
    ...
