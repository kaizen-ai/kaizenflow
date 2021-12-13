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
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Skip files that have already being converted",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    hio.create_dir(args.dst_dir, args.incremental)
    ext = (".csv", ".csv.gz")
    # Find all the CSV files to convert.
    csv_files = [fn for fn in os.listdir(args.src_dir) if fn.endswith(ext)]
    hdbg.dassert_ne(len(csv_files), 0, "No .csv files inside '%s'", args.src_dir)
    if args.incremental:
        # Find pq files that already converted.
        pq_filenames = []
        for f in os.listdir(args.dst_dir):
            if f.endswith(".pq"):
                pq_filenames.append(f[:-3])
            elif f.endswith(".parquet"):
                pq_filenames.append(f[:-8])
            else:
                raise ValueError("Invalid file '{f}'")
        # Remove csv files that do not need to be converted.
        for f in csv_files:
            filename = f[:-4] if f.endswith(".csv") else f[:-7]
            if filename in pq_filenames:
                csv_files.remove(f)
                _LOG.warning(
                    "Skipping the conversion of CSV file '%s' since '%s' already exists",
                    f,
                    f,
                )
    # Transform csv files.
    for f in csv_files:
        filename = f[:-4] if f.endswith(".csv") else f[:-7]  # for .csv.gz
        csv_full_path = os.path.join(args.src_dir, f)
        pq_full_path = os.path.join(args.dst_dir, f"{filename}.parquet")
        hcsv.convert_csv_to_pq(csv_full_path, pq_full_path)


if __name__ == "__main__":
    _main(_parse())
