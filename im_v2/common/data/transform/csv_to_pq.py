#!/usr/bin/env python
"""
Extract RT data from csv to daily PQ files.

# Example:
> python im_v2/common/data/transform/extract_data_from_db.py \
    --daily_pq_path im_v2/common/data/transform/test_dir

Import as:

import im_v2.common.data.transform.csv_to_pq as imvcdtctpq
"""

import argparse
import logging
import os

import helpers.csv_helpers as hcsv
import helpers.dbg as hdbg
import helpers.git as hgit
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
        help="Location of dir with PQ files",
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
        help="function to apply to apply to df before writing ot PQ",
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
    filenames = []
    for fn in os.listdir(csv_dir):
        if fn.endswith(".csv"):
            filenames.append((fn[:-4], ".csv"))
        if fn.endswith(".csv.gz"):
            filenames.append((fn[:-7], ".csv.gz"))
    if not filenames:
        hdbg.dassert("No .csv files inside '%s'", csv_dir)
    for fil in filenames:
        csv_full_path = os.path.join(csv_dir, "".join(fil))
        pq_full_path = os.path.join(pq_dir, "".join((fil[0], ".parquet")))
        hcsv.convert_csv_to_pq(
            csv_full_path, pq_full_path, header=args.header, normalizer=norm
        )


if __name__ == "__main__":
    _main(_parse())
