#!/usr/bin/env python

r"""
Convert Kibot data from csv.gz to Parquet.

The data is located in `kibot` directory on S3 and is separated into
several subdirectories.
The files in the following subdirectories:
- `All_Futures_Contracts_1min`
- `All_Futures_Continuous_Contracts_1min`
- `All_Futures_Continuous_Contracts_daily`
are converted to Parquet and saved to 'kibot/pq` in corresponding
subdirectories.

Usage example:
> python vendors/kibot/convert_kibot_to_pq.py \
  -v DEBUG
"""

import argparse
import logging

import helpers.dbg as dbg
import vendors.kibot.utils as kut

_LOG = logging.getLogger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    args = parser.parse_args()
    dbg.init_logger(args.log_level)
    #
    kut.convert_kibot_csv_gz_to_pq()
