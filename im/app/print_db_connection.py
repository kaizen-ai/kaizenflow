#!/usr/bin/env python

"""
Print the IM DB connection.
"""

import argparse
import logging

import helpers.dbg as dbg
import helpers.parser as prsr
import im.common.db.init as init

_LOG = logging.getLogger(__name__)

# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    prsr.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    conn = init.get_db_connection()
    print(conn)


if __name__ == "__main__":
    _main(_parse())
