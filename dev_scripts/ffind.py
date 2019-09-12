#!/usr/bin/env python

"""
# Find all files / dirs whose name contains Task243, i.e., the regex "*Task243*"
> ffind.py Task243

# Look for files / dirs with name containing "stocktwits" in "this_dir"
> ffind.py stocktwits this_dir

# Look only for files.
> ffind.py stocktwits --only_files
"""

import argparse
import logging
import os
import sys

import helpers.dbg as dbg

_log = logging.getLogger(__name__)


def _print_help(parser):
    print(parser.format_help())
    sys.exit(-1)


def _main(parser):
    args = parser.parse_args()
    dbg.init_logger(verb=args.log_level)
    positional = args.positional
    # Error check.
    if len(positional) < 1:
        print("Error: not enough parameters")
        _print_help(parser)
    if len(positional) > 2:
        print("Error: too many parameters")
        _print_help(parser)
    # Parse.
    if len(positional) == 2:
        dir_name = positional[1]
    else:
        dir_name = "."
    dbg.dassert_exists(dir_name)
    name = "*" + positional[0].rstrip("").lstrip("") + "*"
    #
    cmd = "find %s" % dir_name
    if args.only_files:
        cmd += " -type f"
    cmd += ' -iname "*%s*"' % name
    cmd += " | sort"
    cmd += " | grep -v .ipynb_checkpoints"
    print(cmd)
    print()
    os.system(cmd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "positional",
        nargs="*",
        help="First param is regex, optional second param is dirname",
    )
    parser.add_argument("--only_files", action="store_true", help="Only files")
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    _main(parser)
