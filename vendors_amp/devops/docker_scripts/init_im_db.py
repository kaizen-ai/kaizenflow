#!/usr/bin/env python

"""
Apply schema to PostgreSQL database inside the container.

Usage:
- Apply schema to `im_db_local` database:
    > init_in_db.py --db im_db_local
"""
import argparse
import logging

import helpers.dbg as dbg
import helpers.parser as hparse
import vendors_amp.common.db.init as vcdini

_LOG = logging.getLogger(__name__)

# #############################################################################


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--db", action="store", help="Database to update", required=True
    )
    hparse.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level, use_exec_path=True)
    # Insert your code here.
    _LOG.info("Updating schema for %s...", args.db)
    vcdini.initialize_database(
        args.db, init_sql_files=vcdini.get_init_sql_files()
    )
    _LOG.info("Database %s is ready to use.", args.db)


if __name__ == "__main__":
    _main(_parse())
