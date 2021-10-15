#!/usr/bin/env python

"""
Set SQL schema for IM database inside a Docker container.

Note: IM database is created using environment variables.

Usage:
- Set SQL schema for the IM database:
    > set_schema_im_db.py

Import as:

import im.devops.docker_scripts.set_schema_im_db as imddoscsescimdb
"""
import argparse
import logging
import os

import helpers.dbg as hdbg
import helpers.parser as hparser
import helpers.sql as hsql
import im.common.db.create_schema as imcodbcrsch
import im.common.db.utils as imcodbuti

_LOG = logging.getLogger(__name__)


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level)
    db_name = os.environ["POSTGRES_DB"]
    host = os.environ["POSTGRES_HOST"]
    port = int(os.environ["POSTGRES_PORT"])
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    # Verify that the database is available.
    imcodbuti.check_db_connection(
        db_name=db_name, host=host, port=port, user=user, password=password
    )
    conn, cursor = hsql.get_connection(
        dbname=db_name,
        host=host,
        port=port,
        user=user,
        password=password,
    )
    # Set schema for the database.
    _LOG.info("Setting schema for DB `%s`...", os.environ["POSTGRES_DB"])
    # TODO(Danya): remove cursor and pass connection (#169).
    imcodbcrsch.create_all_tables(cursor)
    imcodbcrsch.test_tables(conn, cursor)
    _LOG.info("Database `%s` is ready to use.", os.environ["POSTGRES_DB"])


if __name__ == "__main__":
    _main(_parse())
