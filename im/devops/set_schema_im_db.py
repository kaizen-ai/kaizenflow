#!/usr/bin/env python

"""
Set SQL schema for IM database inside a Docker container.

Note: IM database is creates using environment variables.

Usage:
- Set SQL schema for the IM database:
    > init_im_db.py
"""
import argparse
import logging
import os

import helpers.dbg as dbg
import helpers.parser as hparse
import im.common.db.create_schema as icdcrsch

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    hparse.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    dbg.init_logger(verbosity=args.log_level)
    db_name = os.environ["POSTGRES_DB"]
    _LOG.info("Applying schema to DB `%s`...", db_name)
    icdcrsch.create_schema()
    _LOG.info("Database `%s` is ready to use.", db_name)


if __name__ == "__main__":
    _main(_parse())
