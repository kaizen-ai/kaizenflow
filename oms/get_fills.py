"""
Get filled orders from the exchange and save to JSON.

Example use:
# Get filled orders from binance from 2022-09-22 to 2022-09-23.
> oms/flatten_ccxt_account.py \
    --exchange_id 'binance' \
    --contract_type 'futures' \
    --stage 'preprod' \
    --account_type 'trading' \
    --secret_identifer '1'
"""

import logging
import oms.broker as ombroker
import helpers.hio as hio
import helpers.hdbg as hdbg
import argparse

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
        type=str,
        help="Stage: 'local' for testnet, or 'prod' for trading account",
        action="store",
    )
    parser.add_argument(
        "--account_type",
        type=str,
        help="Stage: 'sandbox' for testnet account, 'trading' for",
        action="store",
    )