#!/usr/bin/env python

"""
Convert data from Parquet to CSV.

Usage sample:
> im_v2/common/data/transform/convert_...

Note that the input file can be S3 or local file system, but the destination dir needs to be local.

The code is similar to the conversion from CSV to Parquet performed by
`im_v2/common/data/transform/convert_csv_to_pq.py`
"""

import argparse
import logging
import os

import pandas as pd

import helpers.hdbg as hdbg
import helpers.hparser as hparser
import helpers.hs3 as hs3
import helpers.hparquet as hparquet

from pathlib import Path

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--pq_path",
        action="store",
        type=str,
        required=True,
        help="Input Parquet file to convert to CSV format",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        required=True
        help="Skip files that have already been converted",
    )
    parser.add_argument(
        "--aws_profile",
        action="store",
        required=True,
        type=str,
        help="The AWS profile to use for `.aws/credentials` or for env vars",
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        type=str,
        default=None
        help="Destination dir where to save converted CSV file",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    hs3.dassert_path_exists(args.dst_dir, aws_profile = args.aws_profile)
    hs3.dassert_path_exists(args.pq_path)
    _LOG.info("Files found at %s", args.pq_path)
    _LOG.debug("Converting %s...", args.pq_path)
    df = hparquet.from_parquet(args.pq_path)
    df.to_csv(args.dst_dir)

if __name__ == "__main__":
    _main(_parse())
