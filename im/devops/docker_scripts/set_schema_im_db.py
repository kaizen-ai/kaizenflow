#!/usr/bin/env python

"""
Set SQL schema for IM database inside a Docker container.

Note: IM database is created using environment variables.

Usage:
- Set SQL schema for the IM database:
    > set_schema_im_db.py

Import as:

import im.devops.docker_scripts.set_schema_im_db as imddsssimdb
"""
import argparse
import logging
import os

import helpers.dbg as hdbg
import helpers.parser as hparser
import helpers.sql as hsql
import im.common.db.create_db as imcodbcrdb
import im.common.db.utils as imcodbuti

_LOG = logging.getLogger(__name__)


# TODO(gp): CmampTask413: Pass db credentials through command line like for
#  im/app/transform/convert_s3_to_sql.py to override env vars.
#  Probably we want to factor out the parser part like we do in helpers/parser.py
#  so that all scripts can use the same interface.
def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    hdbg.init_logger(verbosity=args.log_level)
    # Verify that the database is available.
    hsql.check_db_connection(
        os.environ["POSTRES_DB"],
        os.environ["POSTRES_HOST"],
        os.environ["POSTRES_PORT"],
    )
    connection, _ = hsql.get_connection_from_env_vars()
    # Set schema for the database.
    _LOG.info("Setting schema for DB `%s`...", os.environ["POSTGRES_DB"])
    imcodbcrdb.create_all_tables(connection)
    imcodbcrdb.test_tables(connection)
    _LOG.info("Database `%s` is ready to use.", os.environ["POSTGRES_DB"])


if __name__ == "__main__":
    _main(_parse())
