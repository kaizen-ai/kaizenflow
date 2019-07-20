#!/usr/bin/env python

# TODO(gp): Profile to understand why it's slow.

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
    cmd = 'find %s -type f -iname "%s"' % (dir_name, name)
    _log.debug("> %s", cmd)
    os.system(cmd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('positional', nargs='*')
    parser.add_argument(
        "-v",
        dest="log_level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level")
    _main(parser)