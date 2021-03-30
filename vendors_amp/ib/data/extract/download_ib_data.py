#!/usr/bin/env python
r"""
Extract date from IB Gateway to S3.

Usage examples:
- Convert daily data from S3 to SQL for AAPL, and provider "kibot":
  > download_ib_data.py \
      --symbol NQ \
      --frequency D \
      --contract_type continuous \
      --asset_class Futures \
      --exchange GLOBEX
"""

import argparse
import logging

import pandas as pd

import helpers.dbg as dbg
import helpers.parser as hparse
import vendors_amp.common.data.types as vcdtyp
import vendors_amp.ib.data.extract.data_extractor as videda

# from tqdm.notebook import tqdm

_LOG = logging.getLogger(__name__)


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    dbg.shutup_chatty_modules()
    # Extract the data.
    extractor = videda.IbDataExtractor()
    for symbol in args.symbol:
        extractor.extract_data(
            exchange=args.exchange,
            symbol=symbol,
            asset_class=args.asset_class,
            frequency=args.frequency,
            contract_type=args.contract_type,
            start_ts=args.start_ts,
            end_ts=args.end_ts,
            incremental=args.incremental,
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
        required=True,
    )
    parser.add_argument(
        "--exchange",
        type=str,
        help="Selected Exchange",
        required=True,
    )
    parser.add_argument(
        "--asset_class",
        type=vcdtyp.AssetClass,
        help="Asset class (e.g. Futures)",
        required=True,
    )
    parser.add_argument(
        "--frequency",
        type=vcdtyp.Frequency,
        help="Frequency of data (e.g. Minutely)",
        required=True,
    )
    parser.add_argument(
        "--contract_type",
        type=vcdtyp.ContractType,
        help="Contract type (e.g. Expiry)",
        required=True,
    )
    parser.add_argument("--start_ts", type=pd.Timestamp)
    parser.add_argument("--end_ts", type=pd.Timestamp)
    parser.add_argument("--incremental", action="store_true", default=False)
    hparse.add_verbosity_arg(parser)
    return parser


if __name__ == "__main__":
    _main(_parse())
