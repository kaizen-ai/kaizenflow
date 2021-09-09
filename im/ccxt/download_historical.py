"""
Script to download historical data from ccxt.
"""

import im.ccxt.exchange_class as icec
import logging
import time
from typing import Any, Dict, List, Optional, Union

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.parser as prsr
import argparse

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        required=True,
        type=str,
        help="The path to the folder to store the output",
    )

    parser.add_argument(
        "--file_name",
        action="store",
        required=True,
        type=str,
        help="The path to the folder to store the output"
    )

    parser.add_argument(
        "--exchange",
        action="store",
        required=True,
        type=str,
        help="CCXT name of the exchange to download data from",
    )
    parser.add_argument(
        "--currency_pair",
        action="store",
        required=True,
        type=str,
        help="Name of the currency pair to download data from",
    )
    parser.add_argument(
        "--start_date",
        action="store",
        required=True,
        type=str,
        help="Start date of download in iso8601 format"
    )
    parser.add_argument(
        "--end_date",
        action="store",
        type=str,
        default=None,
        help="End date of download in iso8601 format (optional, defaults to datetime.now())"
    )
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--dry_run", action="store_true")
    parser = prsr.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Create the dst dir.
    # Initialize the exchange class.
    #  Note: won't work with default keys path inside bash docker

    # Download ohlcv.

    # Transform to dataframe.

    # Save as single .csv.gz file.
    return None


if __name__ == "__main__":
    _main(_parse())