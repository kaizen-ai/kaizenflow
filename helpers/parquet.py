#!/usr/bin/env python

"""
Add a description of what the script does and examples of command lines.

Check dev_scripts/linter.py to see an example of a script using this
template.
"""

import argparse
import logging

import helpers.dbg as dbg
import helpers.parser as prsr

# import helpers.system_interaction as si

_LOG = logging.getLogger(__name__)

# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--start_date", nargs="store", required=True, help="Start date, e.g., 2010-01-01")
    parser.add_argument("--end_date", nargs="store", required=True, help="End date, e.g., 2010-01-01")
    parser.add_argument("--incremental", nargs="store_true", help="End date, e.g., 2010-01-01")
    parser.add_argument("--dst_dir", action="store", help="Destination dir")
    prsr.add_verbosity_arg(parser)
    return parser


def _save_data_as_pq(df, dst_dir):
    # Append year and month.
    # Save.
    pass


def _save_data_as_pq_without_extra_cols(df, dst_dir):
    # Append year and month.
    # Save.
    pass


def _date_exists(date, dst_dir) -> bool:
    """
    Check if the partition corresponding to `date` under `dst_dir` exists.
    """
    pass


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Insert your code here.
    # - Use _LOG.info(), _LOG.debug() instead of printing.
    # - Use dbg.dassert_*() for assertion.
    # - Use si.system() and si.system_to_string() to issue commands.
    #
    hio.create_dir(dst_dir, incremental=True)
    # Get all the dates with s3.list
    dates = []
    dates = sorted(dates)
    # Scan the dates.
    for date in dates:
        if incremental and _date_exists(date, dst_dir):
            _LOG.info("Skipping processing of date '%s since incremental mode'", date)
        # Read data.



if __name__ == "__main__":
    _main(_parse())
