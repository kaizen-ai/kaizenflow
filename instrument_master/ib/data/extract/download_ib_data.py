#!/usr/bin/env python
r"""
Extract date from IB Gateway to S3.

Note, that IB Gateway app should be up.

Usage examples:
- Download NQ data to S3:
  > download_ib_data.py \
      --symbol NQ \
      --frequency D \
      --contract_type continuous \
      --asset_class Futures \
      --exchange GLOBEX \
      --currency USD

- Download data by parts:
  > download_ib_data.py \
      --symbol NQ \
      --frequency D \
      --contract_type continuous \
      --asset_class Futures \
      --exchange GLOBEX \
      --currency USD \
      --action download \
      --dst_dir ./tmp

- Push downloaded parts to S3:
  > download_ib_data.py \
      --symbol NQ \
      --frequency D \
      --contract_type continuous \
      --asset_class Futures \
      --exchange GLOBEX \
      --currency USD \
      --action push \

- Download data for all GLOBEX symbols with USD currency:
  > download_ib_data.py \
      --frequency D \
      --exchange GLOBEX \
      --currency USD
"""

import argparse
import logging
from typing import List

import pandas as pd

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.parser as hparse
import instrument_master.common.data.types as icdtyp
import instrument_master.common.metadata.symbols as icmsym
import instrument_master.ib.data.extract.ib_data_extractor as iideib
import instrument_master.ib.metadata.ib_symbols as iimibs

# from tqdm.notebook import tqdm

_LOG = logging.getLogger(__name__)
_DOWNLOAD_ACTION = "download"
_PUSH_TO_S3_ACTION = "push"

VALID_ACTIONS = [_DOWNLOAD_ACTION, _PUSH_TO_S3_ACTION]
DEFAULT_ACTIONS = [_DOWNLOAD_ACTION, _PUSH_TO_S3_ACTION]


def _get_symbols_from_args(args: argparse.Namespace) -> List[icmsym.Symbol]:
    """
    Get list of symbols to extract.
    """
    # If all args are specified to extract only one symbol, return this symbol.
    if args.symbol and args.exchange and args.asset_class and args.currency:
        return [
            icmsym.Symbol(
                ticker=args_symbol,
                exchange=args.exchange,
                asset_class=args.asset_class,
                contract_type=args.contract_type,
                currency=args.currency,
            )
            for args_symbol in args.symbol
        ]
    # Find all matched symbols otherwise.
    symbol_universe = iimibs.IbSymbolUniverse()
    if args.symbol is None:
        args_symbols = [args.symbol]
    else:
        args_symbols = args.symbol
    symbols: List[icmsym.Symbol] = []
    for symbol in args_symbols:
        symbols.extend(
            symbol_universe.get(
                ticker=symbol,
                exchange=args.exchange,
                asset_class=args.asset_class,
                contract_type=args.contract_type,
                currency=args.currency,
            )
        )
    return symbols


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    dbg.shutup_chatty_modules()
    actions = hparse.select_actions(
        args, valid_actions=VALID_ACTIONS, default_actions=DEFAULT_ACTIONS
    )
    # Get symbols to retrieve.
    symbols = _get_symbols_from_args(args)
    # Extract the data.
    extractor = iideib.IbDataExtractor()
    _LOG.info("Found %s symbols", len(symbols))
    for symbol in symbols:
        _LOG.info("Processing symbol '%s'", symbol)
        dst_dir = args.dst_dir
        if dst_dir is None:
            dst_dir = extractor.get_default_part_files_dir(
                symbol=symbol.ticker,
                frequency=args.frequency,
                asset_class=symbol.asset_class,
                contract_type=symbol.contract_type,
            )
        _LOG.info("dst_dir='%s'", dst_dir)
        # If dst dir is on the Local machine, make sure that path exists.
        if not dst_dir.startswith("s3://"):
            hio.create_dir(dst_dir, incremental=True)
        if _DOWNLOAD_ACTION in actions:
            _LOG.info("Downloading")
            extractor.extract_data_parts_with_retry(
                exchange=symbol.exchange,
                symbol=symbol.ticker,
                asset_class=symbol.asset_class,
                frequency=args.frequency,
                currency=symbol.currency,
                contract_type=symbol.contract_type,
                start_ts=args.start_ts,
                end_ts=args.end_ts,
                incremental=args.incremental,
                part_files_dir=dst_dir,
            )
        if _PUSH_TO_S3_ACTION in actions:
            _LOG.info("Pushing to S3")
            extractor.update_archive(
                part_files_dir=dst_dir,
                symbol=symbol.ticker,
                asset_class=symbol.asset_class,
                frequency=args.frequency,
                contract_type=symbol.contract_type,
            )


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--symbol",
        type=str,
        help="Symbols to process",
        action="append",
        required=False,
    )
    parser.add_argument(
        "--exchange",
        type=str,
        help="Selected Exchange",
        required=False,
    )
    parser.add_argument(
        "--asset_class",
        type=icdtyp.AssetClass,
        help="Asset class (e.g. Futures)",
        required=False,
    )
    parser.add_argument(
        "--contract_type",
        type=icdtyp.ContractType,
        help="Contract type (e.g. Expiry)",
        required=False,
    )
    parser.add_argument(
        "--currency",
        type=str,
        help="Symbol currency (e.g. USD)",
        required=False,
    )
    parser.add_argument(
        "--frequency",
        type=icdtyp.Frequency,
        help="Frequency of data (e.g. Minutely)",
        required=True,
    )
    parser.add_argument("--start_ts", type=pd.Timestamp)
    parser.add_argument("--end_ts", type=pd.Timestamp)
    parser.add_argument("--incremental", action="store_true", default=False)
    parser.add_argument(
        "--dst_dir", type=str, help="Path to extracted files with partial data"
    )
    hparse.add_action_arg(
        parser, valid_actions=VALID_ACTIONS, default_actions=DEFAULT_ACTIONS
    )
    hparse.add_verbosity_arg(parser)
    return parser


if __name__ == "__main__":
    _main(_parse())
