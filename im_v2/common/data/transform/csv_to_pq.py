#!/usr/bin/env python
"""
Convert data from csv to daily PQ files.

# Example:
> python im_v2/common/data/transform/csv_to_pq.py \
    --csv-dir test/ccxt_test \
    --pq-dir test_pq

Import as:

import im_v2.common.data.transform.csv_to_pq as imvcdtctpq
"""

import argparse
import logging
import os
import re

import helpers.csv_helpers as hcsv
import helpers.dbg as hdbg
import helpers.git as hgit
import helpers.io_ as hio
import helpers.parser as hparser

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--csv-dir",
        action="store",
        type=str,
        required=True,
        help="Location of csv files to convert to PQ",
    )
    parser.add_argument(
        "--pq-dir",
        action="store",
        type=str,
        required=True,
        help="Location of dir to convert to PQ files",
    )
    parser.add_argument(
        "--header",
        action="store",
        type=str,
        help="Header specification of CSV",
    )
    parser.add_argument(
        "--normalizer",
        action="store",
        type=str,
        help="function to apply to df before writing ot PQ",
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    hdbg.dassert_dir_exists(args.csv_dir)
    hdbg.dassert_dir_exists(args.pq_dir)
    csv_dir = os.path.join(hgit.get_amp_abs_path(), args.csv_dir)
    pq_dir = os.path.join(hgit.get_amp_abs_path(), args.pq_dir)
    # TODO(Dan3) What to do with normalizer function
    if args.normalizer:
        norm = eval(args.normalizer)
    else:
        norm = args.normalizer
    ext = (".csv", ".csv.gz")
    csv_files = [fn for fn in os.listdir(csv_dir) if fn.endswith(ext)]
    if csv_files:
        for f in csv_files:
            csv_full_path = os.path.join(csv_dir, f)
            date = re.search(r"\d{4}-\d{2}-\d{2}", f).group(0).replace("-", "")
            date_dir = f"date={date}"
            hio.create_dir(os.path.join(pq_dir, date_dir), True)
            filename = f[:-4] if f.endswith(".csv") else f[:-7]  # for .csv.gz
            pq_file = f"{filename}.parquet"
            pq_full_path = os.path.join(pq_dir, date_dir, pq_file)
            hcsv.convert_csv_to_pq(
                csv_full_path, pq_full_path, header=args.header, normalizer=norm
            )
    else:
        hdbg.dassert("No .csv files inside '%s'", csv_dir)


if __name__ == "__main__":
    _main(_parse())
