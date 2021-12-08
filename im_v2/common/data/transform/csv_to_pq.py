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
import re

import pandas as pd

import helpers.csv_helpers as hcsv
import helpers.dbg as hdbg
import helpers.git as hgit
import helpers.io_ as hio
import helpers.parser as hparser
import helpers.hparquet as hparque

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
    ext = (".csv", ".csv.gz")
    csv_files = [fn for fn in os.listdir(args.src_dir) if fn.endswith(ext)]
    hdbg.dassert(csv_files != [], "No .csv files inside '%s'", args.src_dir)
    for f in csv_files:
        csv_full_path = os.path.join(args.src_dir, f)

#        df = pd.read_csv(csv_full_path)
#        hparque.save_daily_df_as_pq(df, args.dst_dir)

#        date = re.search(r"\d{4}-\d{2}-\d{2}", f).group(0).replace("-", "")
#        date_dir = f"date={date}"
#        hio.create_dir(os.path.join(args.dst_dir, date_dir), True)
#        filename = f[:-4] if f.endswith(".csv") else f[:-7]  # for .csv.gz
#        pq_file = f"{filename}.parquet"
#        pq_full_path = os.path.join(args.dst_dir,"by_date", date_dir, pq_file)
#        hcsv.convert_csv_to_pq(csv_full_path, pq_full_path)

        filename = f[:-4] if f.endswith(".csv") else f[:-7]  # for .csv.gz
        pq_file = f"{filename}.parquet"
        pq_full_path = os.path.join(args.dst_dir, filename)
        hcsv.convert_csv_to_pq(csv_full_path, pq_full_path)

    df = pd.read_parquet("pqzzz/date=20211204/data.parquet")
    print(df)
if __name__ == "__main__":
    _main(_parse())
