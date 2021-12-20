"""

"""

import argparse
import logging
import os
from typing import Any, Dict, List

import numpy as np

import helpers.datetime_ as hdateti
import helpers.dbg as hdbg
import helpers.hpandas as hpandas
import helpers.hparquet as hparque
import helpers.io_ as hio
import helpers.joblib_helpers as hjoblib
import helpers.parser as hparser
import helpers.printing as hprint

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
        help="Source directory with unpartitioned .parquet files",
    )
    parser.add_argument(
        "--dst_dir",
        action="store",
        type=str,
        required=True,
        help="Location to place partitioned parquet dataset"
    )
    parser.add_argument(
        "--by",
        action="store",
        type=str,
        required=True,
        help="Partition dataset by date or by asset",
    )
    parser.add_argument(
        "--col_name",
        action="store",
        type=str,
        required=True,
        help="Name of column to parition dataset by"
    )


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level, use_exec_path=True)


if __name__ == "__main__":
    _main(_parse())
