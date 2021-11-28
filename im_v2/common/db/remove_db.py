#!/usr/bin/env python
"""
Script to remove database using connection.

Use as:

# Remove a db named 'test_db' using environment variables:
> im/common/db/remove_db.py --db-name 'test_db'

Import as:

import im.common.db.remove_db as imcdbredb
"""

import argparse
import os.path

import helpers.io_ as hio
import helpers.parser as hparser
import helpers.sql as hsql
import im_v2.common.db.utils as imcodbuti


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--credentials",
        action="store",
        default="from_env",
        type=str,
        help="Connection string or path to json file with credentials to DB",
    )
    parser.add_argument(
        "--db-name",
        action="store",
        required=True,
        type=str,
        help="DB to drop",
    )
    parser = hparser.add_verbosity_arg(parser)
    return parser


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    json_exists = os.path.exists(os.path.abspath(args.credentials))
    if args.credentials == "from_env":
        connection = hsql.get_connection_from_env_vars()
    elif json_exists:
        connection = hsql.get_connection(**hio.from_json(args.credentials))
    else:
        connection = hsql.get_connection_from_string(args.credentials)
    hsql.remove_database(connection=connection, dbname=args.db_name)


if __name__ == "__main__":
    _main(_parse())
