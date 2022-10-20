#!/usr/bin/env python
"""
Archive data from DB table into a S3 storage

Use as:
> im_v2/ccxt/db/archive_db_data_to_s3.py \
   --db_stage 'dev' \
   --timestamp 20220217-000000 \
   --db_table 'ccxt_ohlcv_test' \
   --table_column 'timestamp' \
   --s3_path 's3://cryptokaizen-data-test/daily_staged/' \
   --incremental \
   --assert_continuity \

"""
import argparse
import logging
import os

import pandas as pd

import helpers.hdatetime as hdateti
import helpers.hdbg as hdbg
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.hparser as hparser
import helpers.hs3 as hs3
import helpers.hsql as hsql
import im_v2.common.data.transform.transform_utils as imvcdttrut
import im_v2.im_lib_tasks as imvimlita

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--timestamp",
        action="store",
        required=True,
        type=str,
        help="Specifies threshold for archival. Data older than \
            <timestamp> threshold get archived"
    )
    parser.add_argument(
        "--db_stage",
        action="store",
        required=True,
        type=str,
        help="DB stage to use",
    )
    parser.add_argument(
        "--db_table",
        action="store",
        required=True,
        type=str,
        help="DB table to archive data from",
    )
    parser.add_argument(
        "--table_column",
        action="store",
        required=True,
        type=str,
        help="Column to consider when applying the time threshold",
    )
    # Only a base path needs to be provided, i.e.
    #  when archiving db table ccxt_ohlcv for dev DB
    #  you only need to provide s3://cryptokaizen-data/archive/
    #  The script automatically creates/maintains the subfolder
    #  structure for the specific stage and table.
    parser.add_argument(
        "--s3_path",
        action="store",
        required=True,
        type=str,
        help="S3 location to archive data into.",
    )
    parser.add_argument(
        "--incremental",
        action="store",
        required=True,
        type=bool,
        help="Archival mode, if True the script fails if there is no archive yet \
            for the specified table, vice versa for False"
    )
    parser.add_argument(
        "--assert_continuity",
        action="store",
        required=True,
        type=bool,
        help="If True, the script fails if the archival operation creates a time gap \
            in the archive data or if a different column is used as a threshold than the \
            last entry"
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser  # type: ignore[no-any-return]

def _run(args: argparse.Namespace) -> None:
    pass

if __name__ == "__main__":
    _main(_parse())