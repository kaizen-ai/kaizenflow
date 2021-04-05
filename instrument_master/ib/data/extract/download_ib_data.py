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
      --exchange GLOBEX

- Download data by parts:
  > download_ib_data.py \
      --symbol NQ \
      --frequency D \
      --contract_type continuous \
      --asset_class Futures \
      --exchange GLOBEX \
      --action download \
      --dst_dir ./tmp

- Push downloaded parts to S3:
  > download_ib_data.py \
      --symbol NQ \
      --frequency D \
      --contract_type continuous \
      --asset_class Futures \
      --exchange GLOBEX \
      --action push \
"""
import argparse
import logging

import pandas as pd

import helpers.dbg as dbg
import helpers.io_ as hio
import helpers.parser as hparse
import instrument_master.common.data.types as vcdtyp
import instrument_master.ib.data.extract.ib_data_extractor as videda

# from tqdm.notebook import tqdm

_LOG = logging.getLogger(__name__)
_DOWNLOAD_ACTION = "download"
_PUSH_TO_S3_ACTION = "push"

VALID_ACTIONS = [_DOWNLOAD_ACTION, _PUSH_TO_S3_ACTION]
DEFAULT_ACTIONS = [_DOWNLOAD_ACTION, _PUSH_TO_S3_ACTION]


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    dbg.shutup_chatty_modules()
    actions = hparse.select_actions(
        args, valid_actions=VALID_ACTIONS, default_actions=DEFAULT_ACTIONS
    )
    # Extract the data.
    extractor = videda.IbDataExtractor()
    for symbol in args.symbol:
        part_files_dir = args.dst_dir
        if part_files_dir is None:
            part_files_dir = extractor.get_default_part_files_dir(
                symbol=symbol,
                frequency=args.frequency,
                asset_class=args.asset_class,
                contract_type=args.contract_type,
            )
        if not part_files_dir.startswith("s3://"):
            hio.create_dir(part_files_dir, incremental=True)
        if _DOWNLOAD_ACTION in actions:
            extractor.extract_data_parts_with_retry(
                exchange=args.exchange,
                symbol=symbol,
                asset_class=args.asset_class,
                frequency=args.frequency,
                contract_type=args.contract_type,
                start_ts=args.start_ts,
                end_ts=args.end_ts,
                incremental=args.incremental,
                part_files_dir=part_files_dir,
            )
        if _PUSH_TO_S3_ACTION in actions:
            extractor.update_archive(
                part_files_dir=part_files_dir,
                symbol=symbol,
                asset_class=args.asset_class,
                frequency=args.frequency,
                contract_type=args.contract_type,
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
