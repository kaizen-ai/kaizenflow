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
import pandas as pd


_LOG = logging.getLogger(__name__)


def convert_timestamp_column(datetime_col: pd.Series) -> pd.Series:
    """

    :param datetime_col:
    :return:
    """
    if pd.api.types.is_integer_dtype(datetime_col):
        converted_datetime_col = datetime_col.apply(hdateti.convert_unix_epoch_to_timestamp)
    elif pd.api.types.is_string_dtype(datetime_col):
        converted_datetime_col = hdateti.to_generalized_datetime(datetime_col)
    else:
        raise ValueError("Incorrect data format. Datetime column should be of integer or string type.")
    return converted_datetime_col

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
