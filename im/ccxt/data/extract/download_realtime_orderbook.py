#!/usr/bin/env python
"""
Script to download order book data from CCXT in real-time.

Use as:

# Download all currency pairs for Binance, Kucoin,
  FTX exchanges:
> python im/ccxt/data/extract/download_realtime_orderbook.py \
    --dst_dir 'ccxt_test' \
    --universe '01'

Import as:

import im.ccxt.data.extract.download_realtime_orderbook as imcdaexdoreaord
"""
# TODO(Danya): Merge with `download_realtime_orderbook.py`
import argparse
import collections
import logging
import os
import time
from typing import Dict, List, NamedTuple, Optional

import helpers.datetime_ as hdatetim
import helpers.dbg as hdbg
import helpers.io_ as hio
import helpers.parser as hparser
import im.ccxt.data.extract.exchange_class as imcdaexexccla
import im.data.universe as imdauni

_LOG = logging.getLogger(__name__)


# TODO(Danya): Create a type and move outside.
def _instantiate_exchange(
    exchange_id: str,
    ccxt_universe: Dict[str, List[str]],
    api_keys: Optional[str] = None,
) -> NamedTuple:
    """
    Create a tuple with exchange id, its class instance and currency pairs.

    :param exchange_id: CCXT exchange id
    :param ccxt_universe: CCXT trade universe
    :return: named tuple with exchange id and currencies
    """
    exchange_to_currency = collections.namedtuple(
        "ExchangeToCurrency", ["id", "instance", "pairs"]
    )
    exchange_to_currency.id = exchange_id
    exchange_to_currency.instance = imcdaexexccla.CcxtExchange(
        exchange_id, api_keys
    )
    exchange_to_currency.pairs = ccxt_universe[exchange_id]
    return exchange_to_currency


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
        help="Folder to download files to",
    )
    parser.add_argument(
        "--api_keys",
        action="store",
        type=str,
        default=imcdaexexccla.API_KEYS_PATH,
        help="Path to JSON file that contains API keys for exchange access",
    )
    parser.add_argument(
        "--universe",
        action="store",
        required=True,
        type=str,
        help="Trade universe to download data for",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    hio.create_dir(args.dst_dir, incremental=False)
    # Load universe.
    universe = imdauni.get_trade_universe(args.universe)
    exchange_ids = universe["CCXT"].keys()
    # Build mappings from exchange ids to classes and currencies.
    exchanges = []
    for exchange_id in exchange_ids:
        exchanges.append(
            _instantiate_exchange(exchange_id, universe["CCXT"], args.api_keys)
        )
    # Launch an infinite loop.
    while True:
        for exchange in exchanges:
            for pair in exchange.pairs:
                # Download latest 5 minutes for the currency pair and exchange.
                order_book = exchange.instance.fetch_order_book(pair)
                file_name = (
                    f"orderbook_{exchange.id}_"
                    f"{pair.replace('/', '_')}_"
                    f"{hdatetim.get_timestamp('ET')}.json"
                )
                full_path = os.path.join(args.dst_dir, file_name)
                # Save file.
                hio.to_json(full_path, order_book)
                _LOG.info("Saved %s", file_name)
        time.sleep(60)


if __name__ == "__main__":
    _main(_parse())
