#!/usr/bin/env python
"""
Convert data from csv to daily PQ files.

# Example:
> im_v2/common/data/transform/csv_to_pq.py \
    --src-dir test/ccxt_test \
    --dst-dir test_pq

Import as:

import im_v2.common.data.transform.csv_to_pq as imvcdtctpq
"""

import argparse
import logging
import os

import helpers.csv_helpers as hcsv
import helpers.dbg as hdbg
import helpers.io_ as hio
import helpers.parser as hparser

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--src_dir",
        action="store",
        type=str,
        required=True,
        help="Location of input CSV to convert to .pq format",
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        type=str,
        required=True,
        help="Destination dir where to save converted PQ files",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    hio.create_dir(args.dst_dir, False)
    ext = (".csv", ".csv.gz")
    csv_files = [fn for fn in os.listdir(args.src_dir) if fn.endswith(ext)]
    hdbg.dassert_ne(len(csv_files), 0,  "No .csv files inside '%s'", args.src_dir)
    for f in csv_files:
        csv_full_path = os.path.join(args.src_dir, f)
        filename = f[:-4] if f.endswith(".csv") else f[:-7]  # for .csv.gz
        pq_full_path = os.path.join(args.dst_dir, f"{filename}.pq")
        hcsv.convert_csv_to_pq(csv_full_path, pq_full_path)


if __name__ == "__main__":
    _main(_parse())
