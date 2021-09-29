#!/usr/bin/env python

"""
Apply schema to PostgreSQL database inside a Docker container.

Usage:
- Apply schema to database with name `im_db_local`:
> init_in_db.py --db_name im_db_local
"""
import argparse
import logging

import helpers.dbg as dbg
import helpers.parser as hparse
import im.common.db.create_schema as icdcrsch

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--db_name",
        action="store",
        help="Name of a database to update",
        required=True,
    )
    hparse.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level)
    _LOG.info("Updating schema to DB %s...", args.db)
    sql_schemas = icdcrsch.get_init_sql_files()
    icdcrsch.initialize_database(
        args.db, init_sql_files=sql_schemas
    )
    _LOG.info("Database %s is ready to use", args.db)


if __name__ == "__main__":
    _main(_parse())
